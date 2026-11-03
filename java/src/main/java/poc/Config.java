package poc;

import java.util.Properties;

/** Shared settings. Mirrors src/common/config.py. */
public final class Config {
    public static final String BOOTSTRAP = env("KAFKA_BOOTSTRAP", "127.0.0.1:30092,127.0.0.1:30093,127.0.0.1:30094");

    /** Overridable, so the final test can run on its own topic. */
    public static final String LIFECYCLE_TOPIC = env("POC_LIFECYCLE_TOPIC", "digitalpayments.payment.lifecycle.v1");

    /** Folder that holds producer run folders. Overridable, so a test run keeps its files together. */
    public static final String RUN_ROOT = env("POC_RUN_ROOT", "output");
    /** The derived-event topics can be overridden, so a test run can use fresh topics. */
    public static final String FRAUD_SCORES_TOPIC = env("POC_FRAUD_SCORES_TOPIC", "digitalpayments.fraud.scores.v1");
    public static final String NOTIFICATION_EVENTS_TOPIC =
            env("POC_NOTIFICATION_EVENTS_TOPIC", "digitalpayments.notification.events.v1");
    public static final String HEALTH_WARM_UP_TOPIC = "digitalpayments.health.warmup.v1";

    /** Suffix for consumer group names, so a test run can start groups that read from the beginning. */
    public static final String GROUP_SUFFIX = env("POC_GROUP_SUFFIX", "");
    public static final String FRAUD_SCORING_GROUP = "fraud-scoring" + GROUP_SUFFIX;
    public static final String NOTIFICATION_SENDER_GROUP = "notification-sender" + GROUP_SUFFIX;
    public static final String AUDIT_ARCHIVER_GROUP = "audit-archiver" + GROUP_SUFFIX;

    public static final int WARM_UP_PARTITIONS = 3;
    public static final long FLUSH_TIMEOUT_MS = 150_000;

    private Config() {
    }

    /** Durable, idempotent producer settings, the same as producer_config() in Python. */
    public static Properties producerProps(String clientId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("client.id", clientId);
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("max.in.flight.requests.per.connection", "5");
        p.put("linger.ms", "5");
        p.put("batch.size", "262144");
        p.put("compression.type", "zstd");
        p.put("partitioner.class", Crc32Partitioner.class.getName());
        p.put("delivery.timeout.ms", "120000");
        p.put("request.timeout.ms", "30000");
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return p;
    }

    /** Consumer settings with manual offset commits, the same as consumer_config() in Python. */
    public static Properties consumerProps(String groupId, String clientId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("client.id", clientId);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        p.put("session.timeout.ms", "45000");
        p.put("max.poll.interval.ms", "300000");
        p.put("max.poll.records", "2000");
        p.put("fetch.min.bytes", "1");
        p.put("fetch.max.wait.ms", "100");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return p;
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
