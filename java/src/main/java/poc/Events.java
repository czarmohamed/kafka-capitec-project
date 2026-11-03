package poc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Encodes and checks events. Mirrors src/common/events.py and the three schemas in schemas/.
 *
 * The rules are hand-written. The Python version uses a JSON Schema validator, which the artifact
 * mirror blocks. The checks cover required fields, no extra fields, types, enums, patterns, ranges,
 * and no floats (money is always integer minor units).
 */
public final class Events {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String LIFECYCLE = "payment_lifecycle_event";
    public static final String FRAUD_SCORE = "fraud_score_event";
    public static final String NOTIFICATION = "notification_event";

    private static final DateTimeFormatter MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private static final Set<String> LIFECYCLE_FIELDS = Set.of(
            "eventId", "schemaVersion", "eventType", "eventTime", "paymentId", "customerId", "merchantId",
            "channel", "paymentType", "amountMinor", "currency", "sequence", "idempotencyKey",
            "reasonCode", "deviceRef");
    private static final Set<String> EVENT_TYPES = Set.of(
            "PAYMENT_INITIATED", "PAYMENT_AUTHORISED", "PAYMENT_NOT_AUTHORISED", "PAYMENT_VALIDATED",
            "PAYMENT_INVALIDATED", "PAYMENT_COMPLETED", "PAYMENT_NOT_COMPLETED");
    private static final Set<String> CHANNELS = Set.of("MOBILE_APP", "WEB", "POS", "QR");
    private static final Set<String> PAYMENT_TYPES = Set.of("EFT", "CARD_PRESENT", "CARD_NOT_PRESENT", "QR");

    private static final Set<String> FRAUD_FIELDS = Set.of(
            "eventId", "schemaVersion", "eventTime", "sourceEventId", "paymentId", "customerId",
            "score", "band", "modelVersion");
    private static final Set<String> BANDS = Set.of("LOW", "MEDIUM", "HIGH");

    private static final Set<String> NOTIFICATION_FIELDS = Set.of(
            "eventId", "schemaVersion", "eventTime", "sourceEventId", "paymentId", "customerId",
            "notificationType", "channel", "deliveryStatus", "attempts");
    private static final Set<String> NOTIFICATION_TYPES = Set.of("PAYMENT_APPROVED", "PAYMENT_FAILED");
    private static final Set<String> NOTIFICATION_CHANNELS = Set.of("SMS", "PUSH");
    private static final Set<String> DELIVERY = Set.of("SENT", "FAILED");

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern CUSTOMER = Pattern.compile("^CUST-[0-9]{8}$");
    private static final Pattern MERCHANT = Pattern.compile("^MERCH-[0-9]{6}$");
    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");
    private static final Pattern DEVICE = Pattern.compile("^[a-f0-9]{64}$");

    /** Thrown when an event does not match its schema. */
    public static final class InvalidEvent extends RuntimeException {
        public InvalidEvent(String message) {
            super(message);
        }
    }

    private Events() {
    }

    public static JsonNode decode(byte[] payload, String schema) {
        JsonNode node;
        try {
            node = MAPPER.readTree(payload);
        } catch (java.io.IOException e) {
            throw new InvalidEvent(schema + ": payload is not valid JSON: " + e.getMessage());
        }
        check(node, schema);
        return node;
    }

    public static byte[] encode(JsonNode node, String schema) {
        check(node, schema);
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new InvalidEvent(schema + ": cannot serialise: " + e.getMessage());
        }
    }

    public static void check(JsonNode node, String schema) {
        if (node == null || !node.isObject()) {
            throw new InvalidEvent(schema + ": not an object");
        }
        checkNoFloats(node, "<root>", schema);
        switch (schema) {
            case LIFECYCLE -> checkLifecycle(node);
            case FRAUD_SCORE -> checkFraudScore(node);
            case NOTIFICATION -> checkNotification(node);
            default -> throw new IllegalArgumentException("unknown schema " + schema);
        }
    }

    private static void checkLifecycle(JsonNode n) {
        requireExactly(n, LIFECYCLE_FIELDS, LIFECYCLE);
        requireInt(n, "schemaVersion", 1, 1);
        requireIn(n, "eventType", EVENT_TYPES, LIFECYCLE);
        requireIn(n, "channel", CHANNELS, LIFECYCLE);
        requireIn(n, "paymentType", PAYMENT_TYPES, LIFECYCLE);
        requireUuid(n, "eventId", LIFECYCLE);
        requireUuid(n, "paymentId", LIFECYCLE);
        requireMatch(n, "customerId", CUSTOMER, LIFECYCLE);
        requireMatch(n, "merchantId", MERCHANT, LIFECYCLE);
        requireMatch(n, "currency", CURRENCY, LIFECYCLE);
        requireInt(n, "amountMinor", 1, Long.MAX_VALUE);
        requireInt(n, "sequence", 1, Long.MAX_VALUE);
        requireString(n, "eventTime", LIFECYCLE);
        requireLength(n, "idempotencyKey", 8, 64, LIFECYCLE);
        nullableString(n, "reasonCode", 32, LIFECYCLE);
        JsonNode device = n.get("deviceRef");
        if (!device.isNull() && !(device.isTextual() && DEVICE.matcher(device.asText()).matches())) {
            throw new InvalidEvent(LIFECYCLE + ": deviceRef must be a 64-character SHA-256 hex string or null");
        }
    }

    private static void checkFraudScore(JsonNode n) {
        requireExactly(n, FRAUD_FIELDS, FRAUD_SCORE);
        requireInt(n, "schemaVersion", 1, 1);
        requireUuid(n, "eventId", FRAUD_SCORE);
        requireUuid(n, "sourceEventId", FRAUD_SCORE);
        requireUuid(n, "paymentId", FRAUD_SCORE);
        requireMatch(n, "customerId", CUSTOMER, FRAUD_SCORE);
        requireString(n, "eventTime", FRAUD_SCORE);
        requireInt(n, "score", 0, 100);
        requireIn(n, "band", BANDS, FRAUD_SCORE);
        requireLength(n, "modelVersion", 1, 32, FRAUD_SCORE);
    }

    private static void checkNotification(JsonNode n) {
        requireExactly(n, NOTIFICATION_FIELDS, NOTIFICATION);
        requireInt(n, "schemaVersion", 1, 1);
        requireUuid(n, "eventId", NOTIFICATION);
        requireUuid(n, "sourceEventId", NOTIFICATION);
        requireUuid(n, "paymentId", NOTIFICATION);
        requireMatch(n, "customerId", CUSTOMER, NOTIFICATION);
        requireString(n, "eventTime", NOTIFICATION);
        requireIn(n, "notificationType", NOTIFICATION_TYPES, NOTIFICATION);
        requireIn(n, "channel", NOTIFICATION_CHANNELS, NOTIFICATION);
        requireIn(n, "deliveryStatus", DELIVERY, NOTIFICATION);
        requireInt(n, "attempts", 1, 10);
    }

    public static String utcNowIso() {
        return MILLIS.format(Instant.now());
    }

    public static String newEventId() {
        return UUID.randomUUID().toString();
    }

    public static long toEpochMs(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    public static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    private static void requireExactly(JsonNode n, Set<String> fields, String schema) {
        Iterator<String> names = n.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            if (!fields.contains(name)) {
                throw new InvalidEvent(schema + ": unexpected field " + name);
            }
        }
        for (String name : fields) {
            if (!n.has(name)) {
                throw new InvalidEvent(schema + ": " + name + " is a required property");
            }
        }
    }

    private static void requireInt(JsonNode n, String field, long min, long max) {
        JsonNode v = n.get(field);
        if (v == null || !v.isIntegralNumber() || v.asLong() < min || v.asLong() > max) {
            throw new InvalidEvent(field + " must be an integer between " + min + " and " + max);
        }
    }

    private static void requireString(JsonNode n, String field, String schema) {
        if (!n.get(field).isTextual()) {
            throw new InvalidEvent(schema + ": " + field + " must be a string");
        }
    }

    private static void requireUuid(JsonNode n, String field, String schema) {
        requireMatch(n, field, UUID_PATTERN, schema);
    }

    private static void requireMatch(JsonNode n, String field, Pattern pattern, String schema) {
        JsonNode v = n.get(field);
        if (!v.isTextual() || !pattern.matcher(v.asText()).matches()) {
            throw new InvalidEvent(schema + ": " + field + " has the wrong format");
        }
    }

    private static void requireIn(JsonNode n, String field, Set<String> allowed, String schema) {
        JsonNode v = n.get(field);
        if (!v.isTextual() || !allowed.contains(v.asText())) {
            throw new InvalidEvent(schema + ": " + field + " is not an allowed value");
        }
    }

    private static void requireLength(JsonNode n, String field, int min, int max, String schema) {
        JsonNode v = n.get(field);
        if (!v.isTextual() || v.asText().length() < min || v.asText().length() > max) {
            throw new InvalidEvent(schema + ": " + field + " has the wrong length");
        }
    }

    private static void nullableString(JsonNode n, String field, int max, String schema) {
        JsonNode v = n.get(field);
        if (v.isNull()) {
            return;
        }
        if (!v.isTextual() || v.asText().length() > max) {
            throw new InvalidEvent(schema + ": " + field + " must be a short string or null");
        }
    }

    private static void checkNoFloats(JsonNode node, String path, String schema) {
        if (node.isFloatingPointNumber()) {
            throw new InvalidEvent(schema + ": float values not allowed, found at " + path);
        }
        if (node.isObject()) {
            node.fields().forEachRemaining(e -> checkNoFloats(e.getValue(), path + "/" + e.getKey(), schema));
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                checkNoFloats(node.get(i), path + "/" + i, schema);
            }
        }
    }

    /** A JSON string literal for value, escaping quotes, backslashes and control characters. */
    public static String jsonString(String value) {
        try {
            return MAPPER.writeValueAsString(value == null ? "" : value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
