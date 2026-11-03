package poc;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Publishes derived events, keyed by customerId. flush() waits for every delivery and throws if
 * any failed, so the batch is not committed. Mirrors src/common/publisher.py.
 */
public final class Publisher {
    private final RecordSink sink;
    private final List<String> failures = new CopyOnWriteArrayList<>();

    public Publisher(RecordSink sink) {
        this.sink = sink;
    }

    public void publish(String topic, String customerId, JsonNode event, String schema) {
        byte[] value = Events.encode(event, schema);
        byte[] key = customerId.getBytes(StandardCharsets.UTF_8);
        sink.send(topic, key, value, null, (error, partition, offset) -> {
            if (error != null) {
                failures.add(topic + ": " + error);
            }
        });
    }

    public void flush() {
        sink.flush();
        if (!failures.isEmpty()) {
            String first = failures.get(0);
            failures.clear();
            throw new IllegalStateException("output delivery failed: " + first);
        }
    }

    public void close() {
        flush();
        sink.close();
    }
}
