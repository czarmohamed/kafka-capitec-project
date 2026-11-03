package poc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The three consumer groups' handlers. Mirror src/fraud_consumer, src/notification_consumer and src/audit_consumer. */
public final class Handlers {
    static final String MODEL_VERSION = "rules-v1";
    static final long VELOCITY_WINDOW_MS = 10L * 60 * 1000;
    static final int MAX_GATEWAY_ATTEMPTS = 3;
    static final Map<String, String> ELIGIBLE = Map.of(
            "PAYMENT_VALIDATED", "PAYMENT_APPROVED",
            "PAYMENT_NOT_AUTHORISED", "PAYMENT_FAILED",
            "PAYMENT_INVALIDATED", "PAYMENT_FAILED",
            "PAYMENT_NOT_COMPLETED", "PAYMENT_FAILED");

    private Handlers() {
    }

    /** Rules from src/fraud_consumer/scoring.py. */
    public static int scorePayment(JsonNode event, int recentInitiations) {
        int score = 10;
        long amount = event.get("amountMinor").asLong();
        if (amount >= 1_000_000) {
            score += 30;
        } else if (amount >= 500_000) {
            score += 15;
        }
        String type = event.get("paymentType").asText();
        if (type.equals("CARD_NOT_PRESENT")) {
            score += 15;
        } else if (type.equals("QR")) {
            score += 5;
        }
        if (event.get("deviceRef").isNull()) {
            score += 10;
        }
        if (recentInitiations >= 5) {
            score += 25;
        } else if (recentInitiations >= 3) {
            score += 10;
        }
        return Math.min(score, 100);
    }

    public static String bandFor(int score) {
        if (score < 40) {
            return "LOW";
        }
        if (score < 75) {
            return "MEDIUM";
        }
        return "HIGH";
    }

    /** Scores each PAYMENT_INITIATED event and publishes a fraud score. */
    public static final class Fraud implements Handler {
        private final Publisher publisher;
        private final String outputTopic;
        private final AppendLog log;
        private final Map<String, List<Long>> initiations = new HashMap<>();

        public Fraud(Publisher publisher, String outputTopic, Path stateDir) {
            this.publisher = publisher;
            this.outputTopic = outputTopic;
            for (String line : AppendLog.readAll(stateDir.resolve("initiations.log"))) {
                String[] parts = line.split("\t");
                if (parts.length == 3) {
                    initiations.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(Long.parseLong(parts[1]));
                }
            }
            this.log = new AppendLog(stateDir.resolve("initiations.log"));
        }

        @Override
        public boolean accepts(JsonNode event) {
            return event.get("eventType").asText().equals("PAYMENT_INITIATED");
        }

        @Override
        public void handle(JsonNode event) {
            String customer = event.get("customerId").asText();
            long now = Events.toEpochMs(event.get("eventTime").asText());
            List<Long> times = initiations.computeIfAbsent(customer, k -> new ArrayList<>());
            int recent = 0;
            for (long t : times) {
                if (t >= now - VELOCITY_WINDOW_MS && t <= now) {
                    recent++;
                }
            }
            int score = scorePayment(event, recent);
            times.add(now);
            log.append(customer + "\t" + now + "\t" + event.get("eventId").asText());

            ObjectNode out = Events.object();
            out.put("eventId", Events.newEventId());
            out.put("schemaVersion", 1);
            out.put("eventTime", Events.utcNowIso());
            out.put("sourceEventId", event.get("eventId").asText());
            out.put("paymentId", event.get("paymentId").asText());
            out.put("customerId", customer);
            out.put("score", score);
            out.put("band", bandFor(score));
            out.put("modelVersion", MODEL_VERSION);
            publisher.publish(outputTopic, customer, out, Events.FRAUD_SCORE);
        }

        @Override
        public void flush() {
            publisher.flush();
            log.commit();
        }
    }

    /** Sends an approved or failed notice through the gateway, then publishes the outcome. */
    public static final class Notification implements Handler {
        private final Publisher publisher;
        private final String outputTopic;

        public Notification(Publisher publisher, String outputTopic) {
            this.publisher = publisher;
            this.outputTopic = outputTopic;
        }

        @Override
        public boolean accepts(JsonNode event) {
            return ELIGIBLE.containsKey(event.get("eventType").asText());
        }

        @Override
        public void handle(JsonNode event) {
            String eventId = event.get("eventId").asText();
            String customer = event.get("customerId").asText();
            String status = "FAILED";
            int attempts = MAX_GATEWAY_ATTEMPTS;
            for (int attempt = 1; attempt <= MAX_GATEWAY_ATTEMPTS; attempt++) {
                if (Gateway.send(eventId, attempt)) {
                    status = "SENT";
                    attempts = attempt;
                    break;
                }
            }
            ObjectNode out = Events.object();
            out.put("eventId", Events.newEventId());
            out.put("schemaVersion", 1);
            out.put("eventTime", Events.utcNowIso());
            out.put("sourceEventId", eventId);
            out.put("paymentId", event.get("paymentId").asText());
            out.put("customerId", customer);
            out.put("notificationType", ELIGIBLE.get(event.get("eventType").asText()));
            out.put("channel", "SMS");
            out.put("deliveryStatus", status);
            out.put("attempts", attempts);
            publisher.publish(outputTopic, customer, out, Events.NOTIFICATION);
        }

        @Override
        public void flush() {
            publisher.flush();
        }
    }

    /** Archives every event, one JSON line each. Replaces the audit SQLite table. */
    public static final class Audit implements Handler {
        private final AppendLog archive;
        private final java.util.Set<String> archived = new java.util.HashSet<>();

        public Audit(Path stateDir) {
            Path file = stateDir.resolve("archive.jsonl");
            for (String line : AppendLog.readAll(file)) {
                try {
                    archived.add(Events.MAPPER.readTree(line).get("eventId").asText());
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("corrupt archive line", e);
                }
            }
            this.archive = new AppendLog(file);
        }

        @Override
        public boolean accepts(JsonNode event) {
            return true;
        }

        @Override
        public void handle(JsonNode event) {
            if (!archived.add(event.get("eventId").asText())) {
                return;
            }
            try {
                archive.append(Events.MAPPER.writeValueAsString(event));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void flush() {
            archive.commit();
        }
    }
}
