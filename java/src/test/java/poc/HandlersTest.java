package poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class HandlersTest {
    static ObjectNode initiated(String eventId, String type) {
        ObjectNode e = EventsTest.lifecycle();
        e.put("eventId", eventId);
        e.put("eventType", type);
        e.put("eventTime", "2026-11-02T10:00:00.000Z");
        return e;
    }

    static String idWithBucket(java.util.function.LongPredicate test) {
        while (true) {
            String id = UUID.randomUUID().toString();
            long bucket = Gateway.bucket(id);
            if (test.test(bucket)) {
                return id;
            }
        }
    }

    @Test
    void bandThresholds() {
        assertEquals("LOW", Handlers.bandFor(0));
        assertEquals("LOW", Handlers.bandFor(39));
        assertEquals("MEDIUM", Handlers.bandFor(40));
        assertEquals("MEDIUM", Handlers.bandFor(74));
        assertEquals("HIGH", Handlers.bandFor(75));
        assertEquals("HIGH", Handlers.bandFor(100));
    }

    @Test
    void scoreAddsRiskForLargeCardNotPresentPaymentsWithoutDevice() {
        ObjectNode e = EventsTest.lifecycle();
        e.put("amountMinor", 1_500_000);
        e.put("paymentType", "CARD_NOT_PRESENT");
        e.putNull("deviceRef");
        assertEquals(10 + 30 + 15 + 10, Handlers.scorePayment(e, 0));
    }

    @Test
    void velocityRaisesTheScore() {
        ObjectNode e = EventsTest.lifecycle();
        e.put("amountMinor", 10_000);
        e.put("paymentType", "EFT");
        assertEquals(Handlers.scorePayment(e, 0) + 25, Handlers.scorePayment(e, 5));
    }

    @Test
    void maximumScoreFromAllRulesIsNinety() {
        ObjectNode e = EventsTest.lifecycle();
        e.put("amountMinor", 2_000_000);
        e.put("paymentType", "CARD_NOT_PRESENT");
        e.putNull("deviceRef");
        assertEquals(90, Handlers.scorePayment(e, 50));
    }

    @Test
    void fraudHandlerPublishesValidScoresAndCountsVelocity(@TempDir Path dir) {
        Fakes.Sink sink = new Fakes.Sink(0);
        Handlers.Fraud fraud = new Handlers.Fraud(new Publisher(sink), "fraud-out", dir);
        JsonNode last = null;
        for (int i = 0; i < 6; i++) {
            ObjectNode e = initiated(UUID.randomUUID().toString(), "PAYMENT_INITIATED");
            assertTrue(fraud.accepts(e));
            fraud.handle(e);
            last = e;
        }
        fraud.flush();
        Fakes.Sent published = sink.sent.get(sink.sent.size() - 1);
        assertEquals("fraud-out", published.topic);
        JsonNode score = Events.decode(published.value, Events.FRAUD_SCORE);
        assertEquals(last.get("customerId").asText(), score.get("customerId").asText());
        assertEquals(Handlers.scorePayment(last, 5), score.get("score").asInt());
    }

    @Test
    void fraudHandlerIgnoresOtherEventTypes(@TempDir Path dir) {
        Handlers.Fraud fraud = new Handlers.Fraud(new Publisher(new Fakes.Sink(0)), "fraud-out", dir);
        assertFalse(fraud.accepts(initiated(UUID.randomUUID().toString(), "PAYMENT_AUTHORISED")));
    }

    @Test
    void notificationSentOnFirstAttemptIsPublishedAsSent() {
        Fakes.Sink sink = new Fakes.Sink(0);
        Handlers.Notification notify = new Handlers.Notification(new Publisher(sink), "notify-out");
        String id = idWithBucket(b -> b % 60 != 0 && b % 25 != 0);
        notify.handle(initiated(id, "PAYMENT_VALIDATED"));
        notify.flush();
        JsonNode out = Events.decode(sink.sent.get(0).value, Events.NOTIFICATION);
        assertEquals("PAYMENT_APPROVED", out.get("notificationType").asText());
        assertEquals("SENT", out.get("deliveryStatus").asText());
        assertEquals(1, out.get("attempts").asInt());
        assertEquals(id, out.get("sourceEventId").asText());
    }

    @Test
    void transientGatewayFailureRecoversOnRetry() {
        Fakes.Sink sink = new Fakes.Sink(0);
        Handlers.Notification notify = new Handlers.Notification(new Publisher(sink), "notify-out");
        String id = idWithBucket(b -> b % 25 == 0 && b % 60 != 0);
        notify.handle(initiated(id, "PAYMENT_NOT_AUTHORISED"));
        notify.flush();
        JsonNode out = Events.decode(sink.sent.get(0).value, Events.NOTIFICATION);
        assertEquals("SENT", out.get("deliveryStatus").asText());
        assertEquals(2, out.get("attempts").asInt());
        assertEquals("PAYMENT_FAILED", out.get("notificationType").asText());
    }

    @Test
    void permanentGatewayFailureIsPublishedAsFailedAfterThreeAttempts() {
        Fakes.Sink sink = new Fakes.Sink(0);
        Handlers.Notification notify = new Handlers.Notification(new Publisher(sink), "notify-out");
        String id = idWithBucket(b -> b % 60 == 0);
        notify.handle(initiated(id, "PAYMENT_INVALIDATED"));
        notify.flush();
        JsonNode out = Events.decode(sink.sent.get(0).value, Events.NOTIFICATION);
        assertEquals("FAILED", out.get("deliveryStatus").asText());
        assertEquals(3, out.get("attempts").asInt());
    }

    @Test
    void completedPaymentsAreNotNotified() {
        assertFalse(new Handlers.Notification(new Publisher(new Fakes.Sink(0)), "x")
                .accepts(initiated(UUID.randomUUID().toString(), "PAYMENT_COMPLETED")));
    }

    @Test
    void auditArchiveStoresEachEventOnce(@TempDir Path dir) throws Exception {
        Handlers.Audit audit = new Handlers.Audit(dir);
        ObjectNode e = initiated(UUID.randomUUID().toString(), "PAYMENT_INITIATED");
        audit.handle(e);
        audit.handle(e);
        audit.flush();
        assertEquals(1, Files.readAllLines(dir.resolve("archive.jsonl")).size());
    }
}
