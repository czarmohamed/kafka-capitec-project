package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ProducerTest {
    static ObjectNode sample() {
        return (ObjectNode) Journeys.flatten(Journeys.generate(1, 11, Instant.parse("2026-11-02T00:00:00Z"),
                UUID.randomUUID().toString())).get(0);
    }

    @Test
    void retriesUntilAcknowledged(@TempDir Path dir) throws Exception {
        Fakes.Sink sink = new Fakes.Sink(2);
        Producer producer = new Producer(sink, dir, () -> 0L, ms -> {
        });
        ObjectNode event = sample();
        assertTrue(producer.sendSync(event));
        producer.close();
        assertEquals(3, sink.sent.size());
        List<String> lines = Files.readAllLines(dir.resolve("producer_deliveries.jsonl"));
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("\"attempts\":3"));
    }

    @Test
    void givesUpAfterTheRetryWindowAndLogsTheFailure(@TempDir Path dir) throws Exception {
        Fakes.Sink sink = new Fakes.Sink(1_000_000);
        AtomicLong clock = new AtomicLong();
        Producer producer = new Producer(sink, dir, () -> {
            long now = clock.get();
            clock.set(1_000_000);
            return now;
        }, ms -> {
        });
        assertFalse(producer.sendSync(sample()));
        producer.close();
        List<String> lines = Files.readAllLines(dir.resolve("failed_events.jsonl"));
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("gave up"));
    }

    @Test
    void messageKeyIsTheCustomerId(@TempDir Path dir) {
        Fakes.Sink sink = new Fakes.Sink(0);
        Producer producer = new Producer(sink, dir, () -> 0L, ms -> {
        });
        ObjectNode event = sample();
        producer.sendSync(event);
        producer.close();
        assertArrayEquals(event.get("customerId").asText().getBytes(StandardCharsets.UTF_8), sink.sent.get(0).key);
    }

    @Test
    void skippedEventsAreLoggedNotSent(@TempDir Path dir) throws Exception {
        Fakes.Sink sink = new Fakes.Sink(0);
        Producer producer = new Producer(sink, dir, () -> 0L, ms -> {
        });
        producer.skip(sample(), "skipped: an earlier event in this payment failed");
        producer.close();
        assertTrue(sink.sent.isEmpty());
        assertTrue(Files.readAllLines(dir.resolve("failed_events.jsonl")).get(0).contains("\"attempts\":0"));
    }

    @Test
    void warmUpSendsOnlyToTheHealthTopicOneMessagePerPartition(@TempDir Path dir) {
        Fakes.Sink sink = new Fakes.Sink(0);
        Producer producer = new Producer(sink, dir, () -> 0L, ms -> {
        });
        producer.warmUp();
        producer.close();
        assertEquals(3, sink.sent.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(Config.HEALTH_WARM_UP_TOPIC, sink.sent.get(i).topic);
            assertEquals(Integer.valueOf(i), sink.sent.get(i).partition);
        }
    }

    @Test
    void warmUpFailsWhenAMessageIsNotAcknowledged(@TempDir Path dir) {
        Fakes.Sink sink = new Fakes.Sink(1);
        Producer producer = new Producer(sink, dir, () -> 0L, ms -> {
        });
        IllegalStateException error = assertThrows(IllegalStateException.class, producer::warmUp);
        assertTrue(error.getMessage().contains("warm-up failed"));
    }
}
