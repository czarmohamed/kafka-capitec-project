package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Traces each fraud score: producer acknowledgement, then consumer arrival, then scoring. Mirrors
 * scripts/trace_fraud.py.
 */
public final class TraceFraud {
    static final int JOURNEYS = 300;
    static final long SEED = 66;
    static final double OUTLIER_MS = 100;

    private TraceFraud() {
    }

    public static void main(String[] args) throws Exception {
        Path runDir = Path.of(Config.RUN_ROOT, "trace-fraud-java-" + (System.currentTimeMillis() / 1000));
        Files.createDirectories(runDir);
        Map<String, Map<Integer, Long>> snapshot = new HashMap<>();
        snapshot.put(Config.LIFECYCLE_TOPIC, Reader.endOffsets(Config.LIFECYCLE_TOPIC));
        snapshot.put(Config.FRAUD_SCORES_TOPIC, Reader.endOffsets(Config.FRAUD_SCORES_TOPIC));

        List<Map<String, Object>> profile = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread worker = new Thread(() -> ProfileFraud.consume(runDir, profile));
        worker.start();
        MeasureLatency.waitForStableGroups(List.of(Config.FRAUD_SCORING_GROUP));

        Producer producer = new Producer(KafkaSinks.producer("trace-fraud-java"), runDir.resolve("producer"));
        Map<String, Long> ackedAt = new HashMap<>();
        for (ObjectNode event : Journeys.flatten(Journeys.generate(JOURNEYS, SEED, Instant.now(),
                UUID.randomUUID().toString()))) {
            if (!producer.sendSync(event)) {
                throw new IllegalStateException("event not acknowledged");
            }
            ackedAt.put(event.get("eventId").asText(), System.currentTimeMillis());
        }
        producer.close();
        worker.join();

        List<Reader.Record> lifecycle = Reader.read(Config.LIFECYCLE_TOPIC, Events.LIFECYCLE,
                snapshot.get(Config.LIFECYCLE_TOPIC));
        Map<String, Reader.Record> scores = new HashMap<>();
        for (Reader.Record r : Reader.read(Config.FRAUD_SCORES_TOPIC, Events.FRAUD_SCORE,
                snapshot.get(Config.FRAUD_SCORES_TOPIC))) {
            scores.put(r.event().get("sourceEventId").asText(), r);
        }
        Map<String, Map<String, Object>> handled = new HashMap<>();
        for (Map<String, Object> entry : profile) {
            handled.put(entry.get("partition") + ":" + entry.get("offset"), entry);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Reader.Record record : lifecycle) {
            if (!record.event().get("eventType").asText().equals("PAYMENT_INITIATED")) {
                continue;
            }
            String id = record.event().get("eventId").asText();
            Reader.Record score = scores.get(id);
            Map<String, Object> message = handled.get(record.partition() + ":" + record.offset());
            Long acked = ackedAt.get(id);
            if (score == null || message == null || acked == null) {
                continue;
            }
            long created = record.timestampMs();
            double arrived = created + ((Number) message.get("queue_ms")).doubleValue();
            Map<String, Object> row = new HashMap<>();
            row.put("eventId", id);
            row.put("total_ms", (double) (score.timestampMs() - created));
            row.put("produce_to_ack_ms", (double) (acked - created));
            row.put("ack_to_arrival_ms", arrived - acked);
            row.put("arrival_to_score_ms", score.timestampMs() - arrived);
            rows.add(row);
        }
        List<Double> totals = rows.stream().map(r -> (Double) r.get("total_ms")).toList();
        List<Map<String, Object>> worst = rows.stream()
                .filter(r -> (Double) r.get("total_ms") > OUTLIER_MS)
                .sorted((a, b) -> Double.compare((Double) b.get("total_ms"), (Double) a.get("total_ms")))
                .limit(5).toList();
        ObjectNode report = Events.object();
        report.put("scored", rows.size());
        report.put("outlier_threshold_ms", OUTLIER_MS);
        report.set("total", Events.MAPPER.valueToTree(Stats.summarise(totals, Double.MAX_VALUE)));
        report.set("worst", Events.MAPPER.valueToTree(worst));
        Path out = Path.of("evidence/trace_fraud_java.json");
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }
}
