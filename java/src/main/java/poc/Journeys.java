package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Mock payment journeys. Mirrors src/payment_producer/journeys.py. The random sequence differs from
 * Python's, so the same seed gives different data than the Python generator did.
 */
public final class Journeys {
    static final int CUSTOMER_COUNT = 200;
    static final int MERCHANT_COUNT = 50;
    static final int DEVICE_COUNT = 5000;
    private static final UUID NAMESPACE = UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
    private static final DateTimeFormatter MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    record Outcome(String name, double weight, List<String> path, String terminal, List<String> reasons) {
    }

    static final List<Outcome> OUTCOMES = List.of(
            new Outcome("completed", 0.84,
                    List.of("PAYMENT_INITIATED", "PAYMENT_AUTHORISED", "PAYMENT_VALIDATED"),
                    "PAYMENT_COMPLETED", List.of()),
            new Outcome("not_authorised", 0.08,
                    List.of("PAYMENT_INITIATED"),
                    "PAYMENT_NOT_AUTHORISED", List.of("INSUFFICIENT_FUNDS", "CARD_DECLINED", "DAILY_LIMIT_EXCEEDED")),
            new Outcome("invalidated", 0.05,
                    List.of("PAYMENT_INITIATED", "PAYMENT_AUTHORISED"),
                    "PAYMENT_INVALIDATED", List.of("ACCOUNT_CLOSED", "INVALID_BENEFICIARY")),
            new Outcome("not_completed", 0.03,
                    List.of("PAYMENT_INITIATED", "PAYMENT_AUTHORISED", "PAYMENT_VALIDATED"),
                    "PAYMENT_NOT_COMPLETED", List.of("SETTLEMENT_TIMEOUT", "SETTLEMENT_REJECTED")));

    static final List<String> CHANNELS = List.of("MOBILE_APP", "WEB", "POS", "QR");
    static final List<List<String>> PAYMENT_TYPES = List.of(
            List.of("EFT", "CARD_NOT_PRESENT", "QR"),
            List.of("EFT", "CARD_NOT_PRESENT"),
            List.of("CARD_PRESENT"),
            List.of("QR"));

    private Journeys() {
    }

    /** Builds the journeys one at a time, and keeps the clock across them. */
    public static List<List<ObjectNode>> generate(int count, long seed, Instant start, String runId) {
        Random rng = new Random(seed);
        IdFactory newId = new IdFactory(runId);
        Instant clock = start;
        List<List<ObjectNode>> journeys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            clock = clock.plusMillis(randInt(rng, 10, 500));
            Built built = buildJourney(rng, clock, newId);
            journeys.add(built.events());
            clock = built.clock();
        }
        return journeys;
    }

    public static List<ObjectNode> flatten(List<List<ObjectNode>> journeys) {
        List<ObjectNode> events = new ArrayList<>();
        journeys.forEach(events::addAll);
        return events;
    }

    record Built(List<ObjectNode> events, Instant clock) {
    }

    static Built buildJourney(Random rng, Instant clock, IdFactory newId) {
        Outcome outcome = pickOutcome(rng);
        String paymentId = newId.next();
        String customer = String.format("CUST-%08d", randInt(rng, 1, CUSTOMER_COUNT));
        String merchant = String.format("MERCH-%06d", randInt(rng, 1, MERCHANT_COUNT));
        int channelIndex = rng.nextInt(CHANNELS.size());
        String channel = CHANNELS.get(channelIndex);
        List<String> types = PAYMENT_TYPES.get(channelIndex);
        String paymentType = types.get(rng.nextInt(types.size()));
        long amount = randInt(rng, 5_000, 2_000_000);
        String device = null;
        if (channel.equals("MOBILE_APP") || channel.equals("WEB")) {
            device = sha256Hex("device-" + randInt(rng, 1, DEVICE_COUNT));
        }
        String idempotencyKey = "idem-" + paymentId.replace("-", "").substring(0, 24);

        List<String> steps = new ArrayList<>(outcome.path());
        steps.add(outcome.terminal());
        String finalReason = outcome.reasons().isEmpty() ? null
                : outcome.reasons().get(rng.nextInt(outcome.reasons().size()));

        List<ObjectNode> events = new ArrayList<>();
        Instant at = clock;
        for (int sequence = 1; sequence <= steps.size(); sequence++) {
            at = at.plusMillis(randInt(rng, 5, 400));
            String type = steps.get(sequence - 1);
            boolean last = sequence == steps.size();
            ObjectNode e = Events.object();
            e.put("eventId", newId.next());
            e.put("schemaVersion", 1);
            e.put("eventType", type);
            e.put("eventTime", MILLIS.format(at));
            e.put("paymentId", paymentId);
            e.put("customerId", customer);
            e.put("merchantId", merchant);
            e.put("channel", channel);
            e.put("paymentType", paymentType);
            e.put("amountMinor", amount);
            e.put("currency", "ZAR");
            e.put("sequence", sequence);
            e.put("idempotencyKey", idempotencyKey);
            if (last && finalReason != null) {
                e.put("reasonCode", finalReason);
            } else {
                e.putNull("reasonCode");
            }
            if (device == null) {
                e.putNull("deviceRef");
            } else {
                e.put("deviceRef", device);
            }
            events.add(e);
        }
        return new Built(events, at);
    }

    static Outcome pickOutcome(Random rng) {
        double total = OUTCOMES.stream().mapToDouble(Outcome::weight).sum();
        double roll = rng.nextDouble() * total;
        double acc = 0;
        for (Outcome o : OUTCOMES) {
            acc += o.weight();
            if (roll < acc) {
                return o;
            }
        }
        return OUTCOMES.get(OUTCOMES.size() - 1);
    }

    static int randInt(Random rng, int lo, int hi) {
        return lo + rng.nextInt(hi - lo + 1);
    }

    static long randInt(Random rng, long lo, long hi) {
        return lo + (long) (rng.nextDouble() * (hi - lo + 1));
    }

    static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Deterministic UUIDs for one run: unique across runs, repeatable for the same run id. */
    static final class IdFactory {
        private final UUID namespace;
        private long count;

        IdFactory(String runId) {
            this.namespace = uuid5(NAMESPACE, "digitalpayments-poc:" + runId);
        }

        String next() {
            count++;
            return uuid5(namespace, Long.toString(count)).toString();
        }

        static UUID uuid5(UUID namespace, String name) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                sha1.update(toBytes(namespace));
                sha1.update(name.getBytes(StandardCharsets.UTF_8));
                byte[] hash = sha1.digest();
                hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
                hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
                long msb = 0;
                long lsb = 0;
                for (int i = 0; i < 8; i++) {
                    msb = (msb << 8) | (hash[i] & 0xff);
                }
                for (int i = 8; i < 16; i++) {
                    lsb = (lsb << 8) | (hash[i] & 0xff);
                }
                return new UUID(msb, lsb);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        private static byte[] toBytes(UUID uuid) {
            byte[] out = new byte[16];
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();
            for (int i = 0; i < 8; i++) {
                out[i] = (byte) (msb >>> (8 * (7 - i)));
                out[8 + i] = (byte) (lsb >>> (8 * (7 - i)));
            }
            return out;
        }
    }

    static Instant startNow() {
        return Instant.now().minus(Duration.ZERO);
    }
}
