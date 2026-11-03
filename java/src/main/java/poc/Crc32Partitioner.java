package poc;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.zip.CRC32;

/**
 * Chooses a partition as CRC-32 of the key, modulo the partition count. This is the same rule as
 * librdkafka's default partitioner, which the Python producer used, so Java and Python write each
 * customer to the same partition. Kafka's own default (murmur2) would split a customer's history.
 */
public final class Crc32Partitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitions = cluster.partitionCountForTopic(topic);
        if (keyBytes == null) {
            throw new IllegalArgumentException("keys are required: the partition follows the customer id");
        }
        CRC32 crc = new CRC32();
        crc.update(keyBytes);
        return (int) (crc.getValue() % partitions);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
