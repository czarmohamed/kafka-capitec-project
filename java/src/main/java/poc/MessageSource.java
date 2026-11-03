package poc;

import java.util.List;
import java.util.Map;

/** Where consumed records come from. The real one is a Kafka consumer; tests use a fake. */
public interface MessageSource {
    record Message(String topic, int partition, long offset, byte[] key, byte[] value, long timestampMs) {
    }

    record PartitionRef(String topic, int partition) {
    }

    /** Up to maxRecords records, or an empty list after timeoutMs with nothing new. */
    List<Message> poll(int maxRecords, long timeoutMs);

    /** Commits the next offset to read for each partition. */
    void commit(Map<PartitionRef, Long> nextOffsets);

    void close();
}
