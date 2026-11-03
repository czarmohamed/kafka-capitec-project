package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JourneysTest {
    static final Instant START = Instant.parse("2026-11-02T08:00:00Z");
    static final Set<String> FAILURES = Set.of(
            "PAYMENT_NOT_AUTHORISED", "PAYMENT_INVALIDATED", "PAYMENT_NOT_COMPLETED");

    @Test
    void everyEventPassesTheLifecycleCheck() {
        for (List<ObjectNode> journey : Journeys.generate(300, 1, START, "run-a")) {
            journey.forEach(e -> Events.encode(e, Events.LIFECYCLE));
        }
    }

    @Test
    void sequenceIsContiguousFromOne() {
        for (List<ObjectNode> journey : Journeys.generate(300, 2, START, "run-a")) {
            for (int i = 0; i < journey.size(); i++) {
                assertEquals(i + 1, journey.get(i).get("sequence").asInt());
            }
        }
    }

    @Test
    void aPaymentKeepsOneCustomerAndOnePaymentId() {
        for (List<ObjectNode> journey : Journeys.generate(300, 3, START, "run-a")) {
            assertEquals(1, journey.stream().map(e -> e.get("paymentId").asText()).distinct().count());
            assertEquals(1, journey.stream().map(e -> e.get("customerId").asText()).distinct().count());
            assertEquals(1, journey.stream().map(e -> e.get("idempotencyKey").asText()).distinct().count());
        }
    }

    @Test
    void eventTimesIncreaseWithinAPayment() {
        for (List<ObjectNode> journey : Journeys.generate(300, 4, START, "run-a")) {
            for (int i = 1; i < journey.size(); i++) {
                assertTrue(journey.get(i - 1).get("eventTime").asText().compareTo(journey.get(i).get("eventTime").asText()) < 0);
            }
        }
    }

    @Test
    void everyJourneyStartsWithInitiated() {
        for (List<ObjectNode> journey : Journeys.generate(300, 5, START, "run-a")) {
            assertEquals("PAYMENT_INITIATED", journey.get(0).get("eventType").asText());
        }
    }

    @Test
    void failureEventsHaveReasonCodesAndSuccessEventsDoNot() {
        for (List<ObjectNode> journey : Journeys.generate(300, 6, START, "run-a")) {
            for (ObjectNode e : journey) {
                if (FAILURES.contains(e.get("eventType").asText())) {
                    assertFalse(e.get("reasonCode").isNull());
                } else {
                    assertTrue(e.get("reasonCode").isNull());
                }
            }
        }
    }

    @Test
    void allEventIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        int total = 0;
        for (List<ObjectNode> journey : Journeys.generate(500, 7, START, "run-a")) {
            for (ObjectNode e : journey) {
                ids.add(e.get("eventId").asText());
                total++;
            }
        }
        assertEquals(total, ids.size());
        assertTrue(total > 1500);
    }

    @Test
    void sameSeedAndRunIdGiveIdenticalJourneys() {
        assertEquals(Journeys.generate(50, 99, START, "run-a").toString(),
                Journeys.generate(50, 99, START, "run-a").toString());
    }

    @Test
    void idsDoNotRepeatAcrossRuns() {
        Set<String> first = new HashSet<>();
        Journeys.generate(200, 7, START, "run-a").forEach(j -> j.forEach(e -> first.add(e.get("eventId").asText())));
        for (List<ObjectNode> journey : Journeys.generate(200, 7, START, "run-b")) {
            for (ObjectNode e : journey) {
                assertFalse(first.contains(e.get("eventId").asText()));
            }
        }
    }

    @Test
    void outcomeMixIsCloseToTheWeights() {
        List<List<ObjectNode>> journeys = Journeys.generate(4000, 8, START, UUID.randomUUID().toString());
        long completed = journeys.stream().filter(j -> j.get(j.size() - 1).get("eventType").asText()
                .equals("PAYMENT_COMPLETED")).count();
        double share = completed / (double) journeys.size();
        assertTrue(share >= 0.80 && share <= 0.88, "completed share " + share);
    }
}
