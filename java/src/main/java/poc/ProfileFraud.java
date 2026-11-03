package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Breaks each initiated payment's fraud latency into queue time and handle time, using the live
 * consumer. Mirrors scripts/profile_fraud.py. The per-batch flush and commit timings are not recorded
 * in this port, so only per-message timings are reported.
 */
public final class ProfileFraud {
    static final int JOURNEYS = 300;
    static final long SEED = 55;

    private ProfileFraud() {
    }

    public static void main(String[] args) throws Exception {
        Path runDir = Path.of(Config.RUN_ROOT, "profile-fraud-java-" + (System.currentTimeMillis() / 1000));
        Files.createDirectories(runDir);
        Map<String, Map<Integer, Long>> snapshot = new HashMap<>();
        snapshot.put(Config.LIFECYCLE_TOPIC, Reader.endOffsets(Config.LIFECYCLE_TOPIC));
        snapshot.put(Config.FRAUD_SCORES_TOPIC, Reader.endOffsets(Config.FRAUD_SCORES_TOPIC));

        List<Map<String, Object>> profile = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread worker = new Thread(() -> consume(runDir, profile));
        worker.start();
        MeasureLatency.waitForStableGroups(List.of(Config.FRAUD_SCORING_GROUP));
        Process producer = MeasureLatency.javaProcess(runDir.resolve("producer.err"), "poc.ProducerMain",
                "--journeys", Integer.toString(JOURNEYS), "--seed", Long.toString(SEED),
                "--output-dir", runDir.resolve("producer").toString());
        if (producer.waitFor() != 0) {
            throw new IllegalStateException("producer failed");
        }
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

        List<Double> latency = new ArrayList<>();
        List<Double> queue = new ArrayList<>();
        List<Double> handle = new ArrayList<>();
        for (Reader.Record record : lifecycle) {
            if (!record.event().get("eventType").asText().equals("PAYMENT_INITIATED")) {
                continue;
            }
            Reader.Record score = scores.get(record.event().get("eventId").asText());
            Map<String, Object> message = handled.get(record.partition() + ":" + record.offset());
            if (score == null || message == null) {
                continue;
            }
            latency.add((double) (score.timestampMs() - record.timestampMs()));
            queue.add(((Number) message.get("queue_ms")).doubleValue());
            handle.add(((Number) message.get("handle_ms")).doubleValue());
        }
        if (latency.isEmpty()) {
            throw new IllegalStateException("no initiated payments were scored");
        }
        ObjectNode report = Events.object();
        report.put("journeys", JOURNEYS);
        report.put("seed", SEED);
        report.put("scored_payments", latency.size());
        report.set("latency_ms", Events.MAPPER.valueToTree(Stats.summarise(latency, 50)));
        report.set("queue_ms", Events.MAPPER.valueToTree(Stats.summarise(queue, Double.MAX_VALUE)));
        report.set("handle_ms", Events.MAPPER.valueToTree(Stats.summarise(handle, Double.MAX_VALUE)));
        Path out = Path.of("evidence/profile_fraud_java.json");
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }

    static void consume(Path runDir, List<Map<String, Object>> profile) {
        Path state = runDir.resolve("state");
        ProcessedStore store = new ProcessedStore(state.resolve("state.log"));
        Publisher publisher = new Publisher(KafkaSinks.producer("profile-fraud-java"));
        try {
            Handler handler = new Handlers.Fraud(publisher, Config.FRAUD_SCORES_TOPIC, state);
            MessageSource source = KafkaSinks.consumer(Config.FRAUD_SCORING_GROUP, "profile-fraud-java",
                    Config.LIFECYCLE_TOPIC);
            Loop loop = new Loop(source, handler, store, runDir.resolve("dead_letters.jsonl"), profile);
            loop.run(ConsumerMain.IDLE_EXIT_MILLIS_DEFAULT);
            publisher.close();
        } finally {
            store.close();
        }
    }
}
