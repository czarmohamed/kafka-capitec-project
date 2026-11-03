package poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventsTest {
    static ObjectNode lifecycle() {
        return (ObjectNode) Journeys.flatten(Journeys.generate(1, 11, Instant.parse("2026-11-02T00:00:00Z"),
                UUID.randomUUID().toString())).get(0);
    }

    static byte[] bytes(JsonNode node) {
        try {
            return Events.MAPPER.writeValueAsBytes(node);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void validLifecycleEventRoundTrips() {
        ObjectNode event = lifecycle();
        JsonNode back = Events.decode(Events.encode(event, Events.LIFECYCLE), Events.LIFECYCLE);
        // Compare serialised JSON: a number read back may change width (long to int) without changing value.
        assertEquals(Events.MAPPER.valueToTree(event).toString(), back.toString());
    }

    @Test
    void amountMustBeIntegerMinorUnits() {
        ObjectNode event = lifecycle();
        event.put("amountMinor", 250.0);
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void missingRequiredFieldIsRejected() {
        ObjectNode event = lifecycle();
        event.remove("idempotencyKey");
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void unknownEventTypeIsRejected() {
        ObjectNode event = lifecycle();
        event.put("eventType", "PAYMENT_REFUNDED");
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void currencyMustBeAnIsoCode() {
        ObjectNode event = lifecycle();
        event.put("currency", "rand");
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void extraFieldsAreRejected() {
        ObjectNode event = lifecycle();
        event.put("cardNumber", "4111111111111111");
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void sequenceMustStartAtOne() {
        ObjectNode event = lifecycle();
        event.put("sequence", 0);
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(event, Events.LIFECYCLE));
    }

    @Test
    void invalidPayloadBytesAreRejected() {
        assertThrows(Events.InvalidEvent.class, () -> Events.decode("{not json".getBytes(), Events.LIFECYCLE));
    }

    @Test
    void schemaViolationOnDecodeIsRejected() {
        ObjectNode event = lifecycle();
        event.put("amountMinor", 0);
        assertThrows(Events.InvalidEvent.class, () -> Events.decode(bytes(event), Events.LIFECYCLE));
    }

    @Test
    void fraudScoreRoundTripsAndRejectsOutOfRange() {
        ObjectNode score = Events.object();
        score.put("eventId", UUID.randomUUID().toString());
        score.put("schemaVersion", 1);
        score.put("eventTime", "2026-11-02T10:15:30.200Z");
        score.put("sourceEventId", UUID.randomUUID().toString());
        score.put("paymentId", UUID.randomUUID().toString());
        score.put("customerId", "CUST-00000042");
        score.put("score", 12);
        score.put("band", "LOW");
        score.put("modelVersion", "rules-v1");
        assertEquals(score, Events.decode(Events.encode(score, Events.FRAUD_SCORE), Events.FRAUD_SCORE));
        score.put("score", 101);
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(score, Events.FRAUD_SCORE));
    }

    @Test
    void notificationAttemptsCappedAtTen() {
        ObjectNode n = Events.object();
        n.put("eventId", UUID.randomUUID().toString());
        n.put("schemaVersion", 1);
        n.put("eventTime", "2026-11-02T10:15:31.000Z");
        n.put("sourceEventId", UUID.randomUUID().toString());
        n.put("paymentId", UUID.randomUUID().toString());
        n.put("customerId", "CUST-00000042");
        n.put("notificationType", "PAYMENT_APPROVED");
        n.put("channel", "SMS");
        n.put("deliveryStatus", "SENT");
        n.put("attempts", 11);
        assertThrows(Events.InvalidEvent.class, () -> Events.encode(n, Events.NOTIFICATION));
    }

    @Test
    void timestampHasMillisecondsAndZ() {
        String stamp = Events.utcNowIso();
        assertTrue(stamp.endsWith("Z"));
        assertEquals("2026-11-02T10:15:30.123Z".length(), stamp.length());
    }
}
