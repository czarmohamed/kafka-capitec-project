package poc;

import java.util.ArrayList;
import java.util.List;

/** In-memory stand-ins for Kafka, so tests need no broker. */
final class Fakes {
    private Fakes() {
    }

    static final class Sent {
        final String topic;
        final byte[] key;
        final byte[] value;
        final Integer partition;

        Sent(String topic, byte[] key, byte[] value, Integer partition) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.partition = partition;
        }
    }

    /** Queues deliveries until flush(), like the real client. The first failures fail, the rest succeed. */
    static final class Sink implements RecordSink {
        final List<Sent> sent = new ArrayList<>();
        int failuresLeft;
        private long offset;
        private final List<Runnable> pending = new ArrayList<>();

        Sink(int failures) {
            this.failuresLeft = failures;
        }

        @Override
        public void send(String topic, byte[] key, byte[] value, Integer partition, DeliveryCallback callback) {
            sent.add(new Sent(topic, key, value, partition));
            int part = partition == null ? 0 : partition;
            if (failuresLeft > 0) {
                failuresLeft--;
                pending.add(() -> callback.onDelivery(new RuntimeException("broker unavailable"), -1, -1));
            } else {
                long off = ++offset;
                pending.add(() -> callback.onDelivery(null, part, off));
            }
        }

        @Override
        public void flush() {
            List<Runnable> run = new ArrayList<>(pending);
            pending.clear();
            run.forEach(Runnable::run);
        }

        @Override
        public void close() {
        }
    }

    /** Hands out prepared batches, then nothing. Records the offsets it is asked to commit. */
    static final class Source implements MessageSource {
        final List<List<Message>> batches = new ArrayList<>();
        final List<java.util.Map<PartitionRef, Long>> commits = new ArrayList<>();
        final List<String> log;

        Source(List<String> log) {
            this.log = log;
        }

        @Override
        public List<Message> poll(int maxRecords, long timeoutMs) {
            return batches.isEmpty() ? List.of() : batches.remove(0);
        }

        @Override
        public void commit(java.util.Map<PartitionRef, Long> nextOffsets) {
            commits.add(new java.util.HashMap<>(nextOffsets));
            log.add("kafka_commit");
        }

        @Override
        public void close() {
        }
    }

    static MessageSource.Message message(byte[] value, long offset) {
        return new MessageSource.Message("digitalpayments.payment.lifecycle.v1", 0, offset, null, value, 0);
    }
}
