package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Runs one production consumer group. Mirrors src/fraud_consumer/main.py,
 * src/notification_consumer/main.py and src/audit_consumer/main.py.
 *
 *   --group fraud|notification|audit
 *   --idle-exit SECONDS  (default 30)
 *   --output-dir D       (default output/<group name>)
 */
public final class ConsumerMain {
    /** Default idle exit, used by in-process tools. */
    static final long IDLE_EXIT_MILLIS_DEFAULT = 30_000;

    private ConsumerMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = Args.parse(args);
        String group = opts.get("group");
        String groupName = groupName(group);
        Path output = Path.of(opts.getOrDefault("output-dir", "output/" + groupName));
        long idleExitMillis = (long) (Double.parseDouble(opts.getOrDefault("idle-exit", "30")) * 1000);
        Files.createDirectories(output);

        ProcessedStore store = new ProcessedStore(output.resolve("state.log"));
        Publisher publisher = needsPublisher(group)
                ? new Publisher(KafkaSinks.producer(groupName + "-producer")) : null;
        Handler handler = makeHandler(group, output, publisher);
        MessageSource source = KafkaSinks.consumer(groupName, groupName + "-consumer", Config.LIFECYCLE_TOPIC);
        Loop loop = new Loop(source, handler, store, output.resolve("dead_letters.jsonl"), null);
        try {
            loop.run(idleExitMillis);
            if (publisher != null) {
                publisher.close();
            }
        } finally {
            store.close();
        }

        ObjectNode summary = Events.object();
        summary.put("group", groupName);
        summary.put("topic", Config.LIFECYCLE_TOPIC);
        ObjectNode stats = summary.putObject("stats");
        loop.stats().forEach(stats::put);
        Files.writeString(output.resolve("summary.json"),
                Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
    }

    static String groupName(String group) {
        return switch (group) {
            case "fraud" -> Config.FRAUD_SCORING_GROUP;
            case "notification" -> Config.NOTIFICATION_SENDER_GROUP;
            case "audit" -> Config.AUDIT_ARCHIVER_GROUP;
            default -> throw new IllegalArgumentException("unknown group " + group);
        };
    }

    static boolean needsPublisher(String group) {
        return !group.equals("audit");
    }

    static Handler makeHandler(String group, Path output, Publisher publisher) {
        return switch (group) {
            case "fraud" -> new Handlers.Fraud(publisher, Config.FRAUD_SCORES_TOPIC, output);
            case "notification" -> new Handlers.Notification(publisher, Config.NOTIFICATION_EVENTS_TOPIC);
            case "audit" -> new Handlers.Audit(output);
            default -> throw new IllegalArgumentException("unknown group " + group);
        };
    }
}
