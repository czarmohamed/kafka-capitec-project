package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Producer acknowledgement latency on the ordered path. Mirrors scripts/measure_ack_latency.py. */
public final class MeasureAckLatency {
    static final int JOURNEYS = 300;
    static final long SEED = 88;
    static final int WARM_UP_EVENTS = 50;
    static final double SLA_MS = 150;

    private MeasureAckLatency() {
    }

    public static void main(String[] args) throws Exception {
        Path runDir = Path.of(Config.RUN_ROOT, "ack-latency-java-" + (System.currentTimeMillis() / 1000));
        Producer producer = new Producer(KafkaSinks.producer("ack-latency-java"), runDir);
        List<ObjectNode> events = Journeys.flatten(Journeys.generate(JOURNEYS, SEED, Instant.now(),
                UUID.randomUUID().toString()));
        List<Double> durations = new ArrayList<>();
        int notAcknowledged = 0;
        for (int i = 0; i < events.size(); i++) {
            long started = System.nanoTime();
            boolean acknowledged = producer.sendSync(events.get(i));
            double elapsedMs = (System.nanoTime() - started) / 1e6;
            if (!acknowledged) {
                notAcknowledged++;
            } else if (i >= WARM_UP_EVENTS) {
                durations.add(elapsedMs);
            }
        }
        producer.close();

        ObjectNode report = Events.object();
        report.put("journeys", JOURNEYS);
        report.put("seed", SEED);
        report.put("events_sent", events.size());
        report.put("warm_up_events_discarded", WARM_UP_EVENTS);
        report.put("measured_events", durations.size());
        report.put("not_acknowledged", notAcknowledged);
        report.set("ack_latency", Events.MAPPER.valueToTree(Stats.summarise(durations, SLA_MS)));
        Path out = Path.of("evidence/ack_latency_report_java.json");
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }
}
