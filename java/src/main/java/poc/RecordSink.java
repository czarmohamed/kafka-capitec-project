package poc;

/** Where produced records go. The real one is a Kafka producer; tests use a fake. */
public interface RecordSink {
    interface DeliveryCallback {
        void onDelivery(Exception error, int partition, long offset);
    }

    /** partition may be null, to let the client choose. */
    void send(String topic, byte[] key, byte[] value, Integer partition, DeliveryCallback callback);

    /** Waits until every queued record has been acknowledged or has failed. */
    void flush();

    void close();
}
