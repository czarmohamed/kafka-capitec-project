package poc;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The consumer loop. Per batch: handle each record, flush the handler's outputs, commit the
 * processed store, then commit Kafka offsets. Mirrors src/common/consumer.py.
 */
public final class Loop {
    static final int MAX_ATTEMPTS = 3;
    static final int BATCH_SIZE = 2000;
    static final long POLL_TIMEOUT_MS = 10;

    private final MessageSource source;
    private final Handler handler;
    private final ProcessedStore store;
    private final Path deadLetterFile;
    private final List<Map<String, Object>> profile;
    private final Map<String, Long> stats = new TreeMap<>();

    /** profile may be null. When set, each message and each batch is recorded for latency analysis. */
    public Loop(MessageSource source, Handler handler, ProcessedStore store, Path deadLetterFile,
                List<Map<String, Object>> profile) {
        this.source = source;
        this.handler = handler;
        this.store = store;
        this.deadLetterFile = deadLetterFile;
        this.profile = profile;
    }

    public Map<String, Long> stats() {
        return stats;
    }

    /** Consumes until no message has arrived for idleExitMillis. */
    public void run(long idleExitMillis) {
        long idleSince = System.currentTimeMillis();
        try {
            while (true) {
                List<MessageSource.Message> batch = source.poll(BATCH_SIZE, POLL_TIMEOUT_MS);
                if (batch.isEmpty()) {
                    if (System.currentTimeMillis() - idleSince >= idleExitMillis) {
                        break;
                    }
                    continue;
                }
                idleSince = System.currentTimeMillis();
                processBatch(batch);
            }
        } finally {
            source.close();
        }
    }

    void processBatch(List<MessageSource.Message> batch) {
        Map<MessageSource.PartitionRef, Long> next = new HashMap<>();
        for (MessageSource.Message m : batch) {
            next.merge(new MessageSource.PartitionRef(m.topic(), m.partition()), m.offset() + 1, Math::max);
            handle(m, batch.size());
        }
        handler.flush();
        store.commit();
        source.commit(next);
        inc("batches");
    }

    private void handle(MessageSource.Message m, int batchSize) {
        long arrived = System.currentTimeMillis();
        long started = System.nanoTime();
        try {
            handleEvent(m);
        } finally {
            if (profile != null) {
                Map<String, Object> entry = new TreeMap<>();
                entry.put("kind", "message");
                entry.put("partition", m.partition());
                entry.put("offset", m.offset());
                entry.put("queue_ms", arrived - m.timestampMs());
                entry.put("handle_ms", (System.nanoTime() - started) / 1e6);
                entry.put("batch_size", batchSize);
                profile.add(entry);
            }
        }
    }

    private void handleEvent(MessageSource.Message m) {
        JsonNode event;
        try {
            event = Events.decode(m.value(), Events.LIFECYCLE);
        } catch (Events.InvalidEvent e) {
            inc("invalid");
            deadLetter(m, null, e.getMessage());
            return;
        }
        String id = event.get("eventId").asText();
        if (store.seen(id)) {
            inc("duplicate");
            return;
        }
        if (!handler.accepts(event)) {
            inc("filtered_out");
            return;
        }
        RuntimeException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                handler.handle(event);
                inc("processed");
                store.mark(id, "PROCESSED");
                return;
            } catch (RuntimeException e) {
                last = e;
                inc("handler_failures");
                if (attempt < MAX_ATTEMPTS) {
                    sleepSeconds(Math.min(1L << attempt, 10));
                }
            }
        }
        deadLetter(m, id, String.valueOf(last));
        store.mark(id, "DEAD_LETTERED");
    }

    private void deadLetter(MessageSource.Message m, String eventId, String reason) {
        inc("dead_lettered");
        String payload = m.value() == null ? "" : new String(m.value(), StandardCharsets.UTF_8);
        String line = "{\"topic\":\"" + m.topic() + "\",\"partition\":" + m.partition() + ",\"offset\":" + m.offset()
                + ",\"eventId\":" + (eventId == null ? "null" : "\"" + eventId + "\"")
                + ",\"reason\":" + Events.jsonString(reason) + ",\"payload\":" + Events.jsonString(payload) + "}\n";
        try {
            Files.writeString(deadLetterFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void inc(String key) {
        stats.merge(key, 1L, Long::sum);
    }

    private static void sleepSeconds(long seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
