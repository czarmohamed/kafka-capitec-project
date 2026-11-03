package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Starts N instances of one group, each a separate JVM running Worker, and reports the rate at which
 * they read the consumer-test backlog. Mirrors scripts/consumer_throughput.py.
 *
 *   --group fraud|notification|audit   --instances N   --backlog EVENTS
 */
public final class ConsumerThroughput {
    static final double AVERAGE_PAYLOAD_BYTES = 456;

    private ConsumerThroughput() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = Args.parse(args);
        String group = opts.get("group");
        int instances = Integer.parseInt(opts.getOrDefault("instances", "1"));
        long backlog = Long.parseLong(opts.getOrDefault("backlog", "300000"));
        String runId = UUID.randomUUID().toString().replace("-", "");
        Path results = Files.createTempDirectory("tp-results-");

        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        List<Process> processes = new ArrayList<>();
        for (int i = 0; i < instances; i++) {
            Path result = results.resolve("instance-" + i + ".json");
            ProcessBuilder pb = new ProcessBuilder(java, "-Djava.net.preferIPv4Stack=true", "-Xmx512m",
                    "-XX:+UseSerialGC", "-cp", classpath, "poc.Worker", "--group", group, "--run-id", runId,
                    "--instance", Integer.toString(i), "--result", result.toString());
            pb.redirectError(results.resolve("instance-" + i + ".err").toFile());
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            processes.add(pb.start());
        }
        List<Integer> codes = new ArrayList<>();
        for (Process p : processes) {
            codes.add(p.waitFor());
        }

        long total = 0;
        double first = Double.MAX_VALUE;
        double last = 0;
        List<Long> perInstance = new ArrayList<>();
        long deadLetters = 0;
        for (int i = 0; i < instances; i++) {
            ObjectNode r = (ObjectNode) Events.MAPPER.readTree(new File(results.resolve("instance-" + i + ".json").toString()));
            long count = r.get("count").asLong();
            total += count;
            perInstance.add(count);
            if (r.get("first").isNumber()) {
                first = Math.min(first, r.get("first").asDouble());
            }
            if (r.get("last").isNumber()) {
                last = Math.max(last, r.get("last").asDouble());
            }
            deadLetters += r.path("stats").path("dead_lettered").asLong(0);
        }
        double window = last - first;
        ObjectNode report = Events.object();
        report.put("group", group);
        report.put("implementation", "java");
        report.put("instances", instances);
        report.put("backlog_events", backlog);
        report.put("events_read", total);
        report.put("complete", total == backlog);
        report.put("exit_codes", codes.toString());
        report.put("window_seconds", Math.round(window * 100) / 100.0);
        report.put("events_per_second", Math.round(total / window));
        report.put("payload_mbit_per_second", Math.round(total / window * AVERAGE_PAYLOAD_BYTES * 8 / 1e6));
        report.put("per_instance_events", perInstance.toString());
        report.put("dead_letters", deadLetters);
        Path out = Path.of("evidence/consumer_throughput");
        Files.createDirectories(out);
        Files.writeString(out.resolve(group + "-x" + instances + "-java.json"),
                Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writeValueAsString(report));
    }
}
