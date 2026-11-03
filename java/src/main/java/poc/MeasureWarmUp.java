package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * First-event latency of a fresh producer, with and without warm-up. Each run is a new JVM, because
 * the delay happens once per process. Mirrors scripts/measure_warm_up.py.
 */
public final class MeasureWarmUp {
    static final int RUNS_PER_MODE = 20;
    static final long SEED = 123;

    private MeasureWarmUp() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--single")) {
            System.out.println(single(args[1]));
            return;
        }
        Map<String, List<Double>> results = new java.util.TreeMap<>();
        for (String mode : List.of("cold", "built")) {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < RUNS_PER_MODE; i++) {
                values.add(runFreshJvm(mode));
            }
            results.put(mode, values);
        }
        ObjectNode report = Events.object();
        report.put("method", "Fresh JVM per run. Measures the first payment event sent after start-up.");
        report.put("seed", SEED);
        report.set("cold_first_event", Events.MAPPER.valueToTree(Stats.summarise(results.get("cold"), 100)));
        report.set("built_warm_up_first_event", Events.MAPPER.valueToTree(Stats.summarise(results.get("built"), 100)));
        report.put("generated_at", Instant.now().toString());
        Path out = Path.of("evidence/warm_up_java.json");
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }

    static double single(String mode) {
        Path dir = Path.of(Config.RUN_ROOT, "warmup-java", mode + "-" + UUID.randomUUID().toString().substring(0, 8));
        Producer producer = new Producer(KafkaSinks.producer("warmup-" + mode), dir);
        if (mode.equals("built")) {
            producer.warmUp();
        }
        List<List<ObjectNode>> journeys = Journeys.generate(2, SEED, Instant.now(), UUID.randomUUID().toString());
        double measured = -1;
        for (int j = 0; j < journeys.size(); j++) {
            for (int p = 0; p < journeys.get(j).size(); p++) {
                long started = System.nanoTime();
                if (!producer.sendSync(journeys.get(j).get(p))) {
                    throw new IllegalStateException("event not acknowledged");
                }
                double elapsedMs = (System.nanoTime() - started) / 1e6;
                if (j == 0 && p == 0) {
                    measured = elapsedMs;
                }
            }
        }
        producer.close();
        return Math.round(measured * 100) / 100.0;
    }

    static double runFreshJvm(String mode) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java, "-Djava.net.preferIPv4Stack=true",
                "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
                "poc.MeasureWarmUp", "--single", mode);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes()).trim();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("warm-up run failed in mode " + mode);
        }
        String[] lines = out.split("\n");
        return Double.parseDouble(lines[lines.length - 1].trim());
    }
}
