package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Command-line producer. Mirrors src/payment_producer/main.py.
 *
 *   --journeys N   complete payment journeys, sent in order
 *   --burst N      events sent as fast as possible
 *   --seed S       (default 42)
 *   --output-dir D (default: a new timestamped folder under output/)
 */
public final class ProducerMain {
    private ProducerMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = Args.parse(args);
        long seed = Long.parseLong(opts.getOrDefault("seed", "42"));
        String runId = UUID.randomUUID().toString().replace("-", "");
        long stamp = System.currentTimeMillis() / 1000;
        Path output;
        Map<String, Object> summary;
        if (opts.containsKey("journeys")) {
            output = Path.of(opts.getOrDefault("output-dir", Config.RUN_ROOT + "/journeys-" + stamp));
            summary = runJourneys(Integer.parseInt(opts.get("journeys")), seed, output, runId);
        } else if (opts.containsKey("burst")) {
            output = Path.of(opts.getOrDefault("output-dir", Config.RUN_ROOT + "/burst-" + stamp));
            summary = runBurst(Integer.parseInt(opts.get("burst")), seed, output, runId);
        } else {
            throw new IllegalArgumentException("give --journeys N or --burst N");
        }
        Files.createDirectories(output);
        Files.writeString(output.resolve("summary.json"), Events.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(summary));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
    }

    static Map<String, Object> runJourneys(int count, long seed, Path output, String runId) {
        Producer producer = new Producer(KafkaSinks.producer("payment-producer"), output);
        producer.warmUp();
        List<List<ObjectNode>> journeys = Journeys.generate(count, seed, Instant.now(), runId);
        for (List<ObjectNode> journey : journeys) {
            for (int index = 0; index < journey.size(); index++) {
                ObjectNode event = journey.get(index);
                if (!producer.sendSync(event)) {
                    for (ObjectNode remaining : journey.subList(index + 1, journey.size())) {
                        producer.skip(remaining, "skipped: an earlier event in this payment failed");
                    }
                    break;
                }
            }
        }
        producer.close();
        return summary("journeys", seed, runId, count, producer, null);
    }

    static Map<String, Object> runBurst(int target, long seed, Path output, String runId) {
        Producer producer = new Producer(KafkaSinks.producer("payment-burst"), output);
        producer.warmUp();
        long started = System.nanoTime();
        int events = 0;
        for (ObjectNode event : Journeys.flatten(Journeys.generate(target, seed, Instant.now(), runId))) {
            producer.sendAsync(event);
            events++;
            if (events >= target) {
                break;
            }
        }
        producer.close();
        double elapsed = (System.nanoTime() - started) / 1e9;
        return summary("burst", seed, runId, null, producer, new double[]{events, elapsed});
    }

    private static Map<String, Object> summary(String mode, long seed, String runId, Integer journeys,
                                               Producer producer, double[] burst) {
        Map<String, Object> s = new java.util.TreeMap<>();
        s.put("mode", mode);
        s.put("seed", seed);
        s.put("run_id", runId);
        s.put("journeys", journeys);
        s.put("delivered", producer.stats().getOrDefault("delivered", 0L));
        s.put("failed", producer.stats().getOrDefault("failed", 0L));
        s.put("retried_attempts", producer.stats().getOrDefault("retried_attempts", 0L));
        s.put("unsent_after_flush", 0);
        Map<String, Long> byType = new java.util.TreeMap<>();
        producer.stats().forEach((k, v) -> {
            if (k.startsWith("delivered:")) {
                byType.put(k.substring("delivered:".length()), v);
            }
        });
        s.put("delivered_by_event_type", byType);
        s.put("delivered_by_partition", producer.partitions());
        if (burst != null) {
            s.put("events_queued", (long) burst[0]);
            s.put("elapsed_seconds", Math.round(burst[1] * 1000) / 1000.0);
            s.put("events_per_second", burst[1] > 0 ? Math.round(burst[0] / burst[1]) : null);
        }
        return s;
    }
}
