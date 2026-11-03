package poc;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LoopTest {
    /** Records handled events. Fails the first `failures` calls, or always for one payment. */
    static final class Recording implements Handler {
        final List<String> handled = new ArrayList<>();
        final List<String> log;
        int failures;
        String poisonPayment;
        final List<String> rejectTypes;

        Recording(List<String> log, int failures, String poisonPayment, List<String> rejectTypes) {
            this.log = log;
            this.failures = failures;
            this.poisonPayment = poisonPayment;
            this.rejectTypes = rejectTypes;
        }

        @Override
        public boolean accepts(JsonNode event) {
            return !rejectTypes.contains(event.get("eventType").asText());
        }

        @Override
        public void handle(JsonNode event) {
            if (failures > 0) {
                failures--;
                throw new RuntimeException("temporary failure");
            }
            if (event.get("paymentId").asText().equals(poisonPayment)) {
                throw new RuntimeException("cannot process this payment");
            }
            handled.add(event.get("eventId").asText());
        }

        @Override
        public void flush() {
            log.add("handler_flush");
        }
    }

    static List<MessageSource.Message> messagesFor(int count, long startOffset, List<JsonNode> events) {
        List<MessageSource.Message> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(Fakes.message(Events.encode(events.get(i), Events.LIFECYCLE), startOffset + i));
        }
        return out;
    }

    static List<JsonNode> events(int journeys) {
        List<JsonNode> out = new ArrayList<>();
        Journeys.generate(journeys, 3, Instant.parse("2026-11-02T08:00:00Z"), UUID.randomUUID().toString())
                .forEach(j -> out.addAll(j));
        return out;
    }

    @Test
    void processesEachEventAndCommitsTheNextOffset(@TempDir Path dir) {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(2);
        Recording handler = new Recording(log, 0, null, List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertEquals(ev.size(), handler.handled.size());
            assertEquals(ev.size(), source.commits.get(0).values().iterator().next());
        }
    }

    @Test
    void replayedEventIsSkipped(@TempDir Path dir) {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(1);
        Recording handler = new Recording(log, 0, null, List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        source.batches.add(messagesFor(ev.size(), 100, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertEquals(ev.size(), handler.handled.size());
            assertEquals(ev.size(), (long) loop.stats().get("duplicate"));
        }
    }

    @Test
    void invalidPayloadIsDeadLetteredAndTheOffsetStillMovesOn(@TempDir Path dir) throws Exception {
        List<String> log = new ArrayList<>();
        Recording handler = new Recording(log, 0, null, List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(List.of(Fakes.message("{\"not\": \"an event\"}".getBytes(), 999)));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertEquals(1L, loop.stats().get("invalid"));
            assertTrue(Files.readString(dir.resolve("dead.jsonl")).contains("\"offset\":999"));
            assertEquals(1000L, source.commits.get(0).values().iterator().next());
        }
    }

    @Test
    void transientFailureIsRetriedThenSucceeds(@TempDir Path dir) {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(1);
        Recording handler = new Recording(log, 1, null, List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertEquals(1L, loop.stats().get("handler_failures"));
            assertEquals(ev.size(), handler.handled.size());
        }
    }

    @Test
    void persistentFailureIsDeadLetteredAndMarked(@TempDir Path dir) throws Exception {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(1);
        Recording handler = new Recording(log, 0, ev.get(0).get("paymentId").asText(), List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertTrue(loop.stats().get("dead_lettered") >= 1);
            assertTrue(store.seen(ev.get(0).get("eventId").asText()));
        }
    }

    @Test
    void filteredEventsAreNotHandled(@TempDir Path dir) {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(3);
        Recording handler = new Recording(log, 0, null, List.of("PAYMENT_INITIATED"));
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            long initiated = ev.stream().filter(e -> e.get("eventType").asText().equals("PAYMENT_INITIATED")).count();
            assertEquals(initiated, (long) loop.stats().get("filtered_out"));
        }
    }

    @Test
    void orderIsFlushThenStoreCommitThenKafkaCommit(@TempDir Path dir) {
        List<String> log = new ArrayList<>();
        List<JsonNode> ev = events(1);
        Recording handler = new Recording(log, 0, null, List.of());
        Fakes.Source source = new Fakes.Source(log);
        source.batches.add(messagesFor(ev.size(), 0, ev));
        try (ProcessedStore store = new ProcessedStore(dir.resolve("state.log"))) {
            Loop loop = new Loop(source, handler, store, dir.resolve("dead.jsonl"), null);
            loop.run(0);
            assertEquals(List.of("handler_flush", "kafka_commit"), log.subList(0, 2));
        }
    }
}
