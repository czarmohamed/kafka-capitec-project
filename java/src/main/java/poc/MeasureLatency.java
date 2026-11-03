package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.ConsumerGroupState;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * End-to-end latency of the fraud and notification consumers, measured while they run live.
 * Mirrors scripts/measure_latency.py. Latency is the output timestamp minus the input timestamp.
 */
public final class MeasureLatency {
    static final int JOURNEYS = 300;
    static final long SEED = 77;
    static final int IDLE_EXIT_SECONDS = 45;

    private MeasureLatency() {
    }

    public static void main(String[] args) throws Exception {
        Path runDir = Path.of(Config.RUN_ROOT, "latency-java-" + (System.currentTimeMillis() / 1000));
        Files.createDirectories(runDir);
        Map<String, Map<Integer, Long>> snapshot = new HashMap<>();
        snapshot.put(Config.LIFECYCLE_TOPIC, Reader.endOffsets(Config.LIFECYCLE_TOPIC));
        snapshot.put(Config.FRAUD_SCORES_TOPIC, Reader.endOffsets(Config.FRAUD_SCORES_TOPIC));
        snapshot.put(Config.NOTIFICATION_EVENTS_TOPIC, Reader.endOffsets(Config.NOTIFICATION_EVENTS_TOPIC));

        List<Process> consumers = new ArrayList<>();
        for (String group : List.of("fraud", "notification")) {
            consumers.add(javaProcess(runDir.resolve(group + "-consumer.err"), "poc.ConsumerMain",
                    "--group", group, "--idle-exit", Integer.toString(IDLE_EXIT_SECONDS),
                    "--output-dir", runDir.resolve(group).toString()));
        }
        double warmUp = waitForStableGroups(List.of(Config.FRAUD_SCORING_GROUP, Config.NOTIFICATION_SENDER_GROUP));

        Process producer = javaProcess(runDir.resolve("producer.err"), "poc.ProducerMain",
                "--journeys", Integer.toString(JOURNEYS), "--seed", Long.toString(SEED),
                "--output-dir", runDir.resolve("producer").toString());
        if (producer.waitFor() != 0) {
            throw new IllegalStateException("producer failed; see " + runDir.resolve("producer.err"));
        }
        List<Integer> exitCodes = new ArrayList<>();
        for (Process c : consumers) {
            exitCodes.add(c.waitFor());
        }

        List<Reader.Record> lifecycle = Reader.read(Config.LIFECYCLE_TOPIC, Events.LIFECYCLE,
                snapshot.get(Config.LIFECYCLE_TOPIC));
        Map<String, Reader.Record> input = new HashMap<>();
        lifecycle.forEach(r -> input.put(r.event().get("eventId").asText(), r));
        List<Double> fraud = new ArrayList<>();
        for (Reader.Record out : Reader.read(Config.FRAUD_SCORES_TOPIC, Events.FRAUD_SCORE,
                snapshot.get(Config.FRAUD_SCORES_TOPIC))) {
            Reader.Record source = input.get(out.event().get("sourceEventId").asText());
            if (source != null && source.event().get("eventType").asText().equals("PAYMENT_INITIATED")) {
                fraud.add((double) (out.timestampMs() - source.timestampMs()));
            }
        }
        List<Double> notify = new ArrayList<>();
        for (Reader.Record out : Reader.read(Config.NOTIFICATION_EVENTS_TOPIC, Events.NOTIFICATION,
                snapshot.get(Config.NOTIFICATION_EVENTS_TOPIC))) {
            Reader.Record source = input.get(out.event().get("sourceEventId").asText());
            if (source != null && ReconcileNotify.contains(source.event().get("eventType").asText())) {
                notify.add((double) (out.timestampMs() - source.timestampMs()));
            }
        }

        ObjectNode report = Events.object();
        report.put("journeys_produced", JOURNEYS);
        report.put("seed", SEED);
        report.put("consumer_warm_up_seconds", warmUp);
        report.put("producer_output", runDir.resolve("producer").toString());
        report.put("consumer_exit_codes", exitCodes.toString());
        report.put("new_lifecycle_records", lifecycle.size());
        report.set("fraud", Events.MAPPER.valueToTree(Stats.summarise(fraud, 50)));
        report.set("notification", Events.MAPPER.valueToTree(Stats.summarise(notify, 2000)));
        Path out = Path.of("evidence/latency_java.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }

    /** Notifications are sent for these event types only. */
    static final class ReconcileNotify {
        static boolean contains(String type) {
            return Reconcile.ELIGIBLE.contains(type);
        }
    }

    static Process javaProcess(Path errorFile, String mainClass, String... args) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>(List.of(java, "-Djava.net.preferIPv4Stack=true",
                "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"), mainClass));
        command.addAll(List.of(args));
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectError(errorFile.toFile());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return pb.start();
    }

    /** Waits until every group is STABLE with a member. Returns the seconds waited. */
    static double waitForStableGroups(List<String> groups) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", Config.BOOTSTRAP);
        long started = System.currentTimeMillis();
        try (AdminClient admin = AdminClient.create(p)) {
            while (System.currentTimeMillis() - started < 90_000) {
                boolean ready = true;
                for (String group : groups) {
                    ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(group))
                            .describedGroups().get(group).get();
                    if (d.state() != ConsumerGroupState.STABLE || d.members().isEmpty()) {
                        ready = false;
                    }
                }
                if (ready) {
                    return (System.currentTimeMillis() - started) / 1000.0;
                }
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("groups not stable after 90 s: " + groups);
    }
}
