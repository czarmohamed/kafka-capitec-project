package poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Sends payment lifecycle events, with acknowledgement, retries and delivery logs.
 * Mirrors src/payment_producer/producer.py.
 *
 * sendSync waits for each acknowledgement, so the next event for a payment goes out only after
 * the previous one is durable. sendAsync queues events without waiting, for throughput.
 */
public final class Producer implements AutoCloseable {
    static final long RETRY_WINDOW_MS = 600_000;
    static final long MAX_BACKOFF_MS = 30_000;

    private final RecordSink sink;
    private final BufferedWriter deliveries;
    private final BufferedWriter failures;
    private final Map<String, Long> stats = new TreeMap<>();
    private final Map<Integer, Long> partitions = new TreeMap<>();
    private final LongSupplier clockMillis;
    private final LongConsumer sleepMillis;

    public Producer(RecordSink sink, Path outputDir) {
        this(sink, outputDir, System::currentTimeMillis, Producer::sleep);
    }

    /** The clock and sleep are injectable so the retry window can be tested without waiting. */
    public Producer(RecordSink sink, Path outputDir, LongSupplier clockMillis, LongConsumer sleepMillis) {
        this.sink = sink;
        this.clockMillis = clockMillis;
        this.sleepMillis = sleepMillis;
        try {
            Files.createDirectories(outputDir);
            this.deliveries = Files.newBufferedWriter(outputDir.resolve("producer_deliveries.jsonl"), StandardCharsets.UTF_8);
            this.failures = Files.newBufferedWriter(outputDir.resolve("failed_events.jsonl"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, Long> stats() {
        return stats;
    }

    public Map<Integer, Long> partitions() {
        return partitions;
    }

    /** Sends one event and waits for its acknowledgement. Returns false if it was given up on. */
    public boolean sendSync(ObjectNode event) {
        byte[] key = event.get("customerId").asText().getBytes(StandardCharsets.UTF_8);
        byte[] value = Events.encode(event, Events.LIFECYCLE);
        long deadline = clockMillis.getAsLong() + RETRY_WINDOW_MS;
        int attempt = 0;
        while (true) {
            attempt++;
            Outcome outcome = sendAndWait(key, value);
            if (outcome.error == null) {
                recordDelivery(event, outcome.partition, outcome.offset, attempt);
                return true;
            }
            inc("retried_attempts");
            if (clockMillis.getAsLong() >= deadline) {
                recordFailure(event, "gave up after " + attempt + " attempts: " + outcome.error, attempt);
                return false;
            }
            sleepMillis.accept(Math.min((long) Math.pow(2, attempt) * 1000, MAX_BACKOFF_MS));
        }
    }

    /** Records an event that was not sent because an earlier event in its payment failed. */
    public void skip(ObjectNode event, String reason) {
        recordFailure(event, reason, 0);
    }

    /** Queues one event without waiting. The outcome is written when it arrives. */
    public void sendAsync(ObjectNode event) {
        byte[] key = event.get("customerId").asText().getBytes(StandardCharsets.UTF_8);
        byte[] value = Events.encode(event, Events.LIFECYCLE);
        sink.send(Config.LIFECYCLE_TOPIC, key, value, null, (error, partition, offset) -> {
            if (error == null) {
                recordDelivery(event, partition, offset, 1);
            } else {
                recordFailure(event, String.valueOf(error), 1);
            }
        });
    }

    /**
     * Opens broker connections and fetches the producer ID before any payment is sent. Sends one
     * message to each partition of the health-check topic. Throws if any is not acknowledged.
     */
    public void warmUp() {
        CopyOnWriteArrayList<Exception> errors = new CopyOnWriteArrayList<>();
        int[] acknowledged = {0};
        for (int partition = 0; partition < Config.WARM_UP_PARTITIONS; partition++) {
            byte[] key = ("warm-up-" + partition).getBytes(StandardCharsets.UTF_8);
            byte[] value = "{\"type\":\"WARM_UP\"}".getBytes(StandardCharsets.UTF_8);
            sink.send(Config.HEALTH_WARM_UP_TOPIC, key, value, partition, (error, p, o) -> {
                if (error != null) {
                    errors.add(error);
                } else {
                    synchronized (acknowledged) {
                        acknowledged[0]++;
                    }
                }
            });
        }
        sink.flush();
        if (!errors.isEmpty() || acknowledged[0] != Config.WARM_UP_PARTITIONS) {
            throw new IllegalStateException("warm-up failed: " + errors.size() + " failed, "
                    + acknowledged[0] + " of " + Config.WARM_UP_PARTITIONS + " acknowledged");
        }
    }

    /** Waits for queued messages, then closes the logs. */
    @Override
    public void close() {
        sink.flush();
        try {
            deliveries.close();
            failures.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            sink.close();
        }
    }

    private record Outcome(Exception error, int partition, long offset) {
    }

    private Outcome sendAndWait(byte[] key, byte[] value) {
        AtomicReference<Outcome> result = new AtomicReference<>();
        sink.send(Config.LIFECYCLE_TOPIC, key, value, null,
                (error, partition, offset) -> result.set(new Outcome(error, partition, offset)));
        sink.flush();
        Outcome outcome = result.get();
        return outcome != null ? outcome : new Outcome(new IllegalStateException("no delivery report"), -1, -1);
    }

    private synchronized void recordDelivery(ObjectNode event, int partition, long offset, int attempts) {
        partitions.merge(partition, 1L, Long::sum);
        inc("delivered");
        inc("delivered:" + event.get("eventType").asText());
        ObjectNode line = Events.object();
        line.put("eventId", event.get("eventId").asText());
        line.put("paymentId", event.get("paymentId").asText());
        line.put("customerId", event.get("customerId").asText());
        line.put("sequence", event.get("sequence").asInt());
        line.put("eventType", event.get("eventType").asText());
        line.put("partition", partition);
        line.put("offset", offset);
        line.put("attempts", attempts);
        writeLine(deliveries, line);
    }

    private synchronized void recordFailure(JsonNode event, String reason, int attempts) {
        inc("failed");
        ObjectNode line = Events.object();
        line.put("eventId", event.get("eventId").asText());
        line.put("paymentId", event.get("paymentId").asText());
        line.put("sequence", event.get("sequence").asInt());
        line.put("eventType", event.get("eventType").asText());
        line.put("attempts", attempts);
        line.put("reason", reason);
        writeLine(failures, line);
    }

    private static void writeLine(BufferedWriter writer, ObjectNode line) {
        try {
            writer.write(Events.MAPPER.writeValueAsString(line));
            writer.write('\n');
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void inc(String key) {
        stats.merge(key, 1L, Long::sum);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
