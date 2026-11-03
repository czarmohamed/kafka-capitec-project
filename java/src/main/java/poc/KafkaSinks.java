package poc;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/** Kafka-backed implementations of RecordSink and MessageSource. */
public final class KafkaSinks {
    private KafkaSinks() {
    }

    public static RecordSink producer(String clientId) {
        return producer(Config.producerProps(clientId));
    }

    public static RecordSink producer(Properties properties) {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
        return new RecordSink() {
            @Override
            public void send(String topic, byte[] key, byte[] value, Integer partition, DeliveryCallback callback) {
                producer.send(new ProducerRecord<>(topic, partition, key, value), (metadata, error) -> {
                    if (metadata != null) {
                        callback.onDelivery(null, metadata.partition(), metadata.offset());
                    } else {
                        callback.onDelivery(error, -1, -1);
                    }
                });
            }

            @Override
            public void flush() {
                producer.flush();
            }

            @Override
            public void close() {
                producer.close();
            }
        };
    }

    public static MessageSource consumer(String groupId, String clientId, String topic) {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Config.consumerProps(groupId, clientId));
        consumer.subscribe(List.of(topic));
        return new MessageSource() {
            @Override
            public List<Message> poll(int maxRecords, long timeoutMs) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeoutMs));
                List<Message> out = new ArrayList<>(records.count());
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    out.add(new Message(r.topic(), r.partition(), r.offset(), r.key(), r.value(), r.timestamp()));
                    if (out.size() >= maxRecords) {
                        break;
                    }
                }
                return out;
            }

            @Override
            public void commit(Map<PartitionRef, Long> nextOffsets) {
                if (nextOffsets.isEmpty()) {
                    return;
                }
                Map<TopicPartition, OffsetAndMetadata> offsets = nextOffsets.entrySet().stream().collect(
                        Collectors.toMap(
                                e -> new TopicPartition(e.getKey().topic(), e.getKey().partition()),
                                e -> new OffsetAndMetadata(e.getValue())));
                try {
                    consumer.commitSync(offsets);
                } catch (RebalanceInProgressException | CommitFailedException e) {
                    // A rebalance started. The records are replayed after it, and the processed store
                    // skips the duplicates, so skipping this commit is safe (at-least-once).
                    System.err.println("offset commit skipped during rebalance: " + e.getMessage());
                }
            }

            @Override
            public void close() {
                consumer.close();
            }
        };
    }
}
