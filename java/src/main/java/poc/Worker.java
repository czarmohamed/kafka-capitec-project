package poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * One consumer-group instance over the consumer-test backlog, for ConsumerThroughput.
 * Mirrors the worker mode of scripts/consumer_throughput.py.
 */
public final class Worker {
    static final String OUTPUT_TOPIC = "digitalpayments.consumertest-out.v1";
    static final long IDLE_EXIT_MILLIS = 10_000;

    /** Records when the first and last events are accepted. */
    static final class Timed implements Handler {
        private final Handler inner;
        long first = -1;
        long last = -1;
        long count;

        Timed(Handler inner) {
            this.inner = inner;
        }

        @Override
        public boolean accepts(JsonNode event) {
            long now = System.currentTimeMillis();
            if (first < 0) {
                first = now;
            }
            last = now;
            count++;
            return inner.accepts(event);
        }

        @Override
        public void handle(JsonNode event) {
            inner.handle(event);
        }

        @Override
        public void flush() {
            inner.flush();
        }
    }

    private Worker() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = Args.parse(args);
        String group = opts.get("group");
        String runId = opts.get("run-id");
        String instance = opts.get("instance");
        String backlog = Config.env("CONSUMER_BACKLOG_TOPIC", "digitalpayments.consumertest.v1");
        String groupName = ConsumerMain.groupName(group);
        Path stateDir = Files.createTempDirectory("tp-" + group + "-" + instance + "-");

        Publisher publisher = ConsumerMain.needsPublisher(group)
                ? new Publisher(KafkaSinks.producer("tp-" + group + "-" + runId.substring(0, 8) + "-" + instance))
                : null;
        Handler business = switch (group) {
            case "fraud" -> new Handlers.Fraud(publisher, OUTPUT_TOPIC, stateDir);
            case "notification" -> new Handlers.Notification(publisher, OUTPUT_TOPIC);
            case "audit" -> new Handlers.Audit(stateDir);
            default -> throw new IllegalArgumentException("unknown group " + group);
        };
        Timed timed = new Timed(business);
        MessageSource source = KafkaSinks.consumer("tp-" + groupName + "-" + runId, "tp-" + group + "-" + instance, backlog);
        ProcessedStore store = new ProcessedStore(stateDir.resolve("state.log"));
        Loop loop = new Loop(source, timed, store, stateDir.resolve("dead_letters.jsonl"), null);
        try {
            loop.run(IDLE_EXIT_MILLIS);
            if (publisher != null) {
                publisher.close();
            }
        } finally {
            store.close();
        }

        ObjectNode result = Events.object();
        result.put("count", timed.count);
        result.put("first", timed.first < 0 ? null : timed.first / 1000.0);
        result.put("last", timed.last < 0 ? null : timed.last / 1000.0);
        ObjectNode stats = result.putObject("stats");
        loop.stats().forEach(stats::put);
        Events.MAPPER.writeValue(new File(opts.get("result")), result);
        System.out.println("instance " + instance + " read " + timed.count + " events");
    }
}
