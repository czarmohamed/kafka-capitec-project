package poc;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Reads topics for verification. Uses a throwaway group that never commits. Mirrors src/common/reader.py. */
public final class Reader {
    public record Record(String topic, int partition, long offset, String key, long timestampMs, JsonNode event) {
    }

    private Reader() {
    }

    private static KafkaConsumer<byte[], byte[]> consumer() {
        Properties p = Config.consumerProps("verify-" + UUID.randomUUID().toString().substring(0, 12), "verify");
        p.put("enable.auto.commit", "false");
        return new KafkaConsumer<>(p);
    }

    /** Current end offset of every partition. */
    public static Map<Integer, Long> endOffsets(String topic) {
        try (KafkaConsumer<byte[], byte[]> c = consumer()) {
            Map<Integer, Long> ends = new HashMap<>();
            List<TopicPartition> parts = c.partitionsFor(topic, Duration.ofSeconds(15)).stream()
                    .map(info -> new TopicPartition(topic, info.partition())).toList();
            c.assign(parts);
            Map<TopicPartition, Long> raw = c.endOffsets(parts, Duration.ofSeconds(15));
            raw.forEach((tp, off) -> ends.put(tp.partition(), off));
            return ends;
        }
    }

    /** Every record from the given start offsets (default: the beginning) up to the current end. */
    public static List<Record> read(String topic, String schema, Map<Integer, Long> start) {
        Map<Integer, Long> ends = endOffsets(topic);
        List<Record> records = new ArrayList<>();
        Map<Integer, Long> pending = new HashMap<>();
        ends.forEach((p, end) -> {
            long from = start == null ? 0 : start.getOrDefault(p, 0L);
            if (from < end) {
                pending.put(p, end);
            }
        });
        if (pending.isEmpty()) {
            return records;
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer()) {
            List<TopicPartition> parts = ends.keySet().stream().map(p -> new TopicPartition(topic, p)).toList();
            c.assign(parts);
            for (TopicPartition tp : parts) {
                c.seek(tp, start == null ? 0 : start.getOrDefault(tp.partition(), 0L));
            }
            long deadline = System.currentTimeMillis() + 300_000;
            while (!pending.isEmpty()) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException(topic + ": still waiting on partitions " + pending.keySet());
                }
                c.poll(Duration.ofSeconds(1)).forEach(r -> {
                    if (!pending.containsKey(r.partition())) {
                        return;
                    }
                    records.add(new Record(topic, r.partition(), r.offset(),
                            r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8),
                            r.timestamp(), decodeOrRaw(r.value(), schema)));
                    if (r.offset() + 1 >= pending.get(r.partition())) {
                        pending.remove(r.partition());
                    }
                });
            }
        }
        return records;
    }

    private static JsonNode decodeOrRaw(byte[] value, String schema) {
        return Events.decode(value, schema);
    }
}
