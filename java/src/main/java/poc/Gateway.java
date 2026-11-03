package poc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Stand-in for the SMS provider. Failures come from a hash of the event id, so runs are repeatable:
 * about 1 in 25 events fail once, and about 1 in 60 always fail. Mirrors src/notification_consumer/gateway.py.
 */
public final class Gateway {
    private Gateway() {
    }

    /** Returns true if the send succeeds on this attempt. */
    public static boolean send(String eventId, int attempt) {
        long bucket = bucket(eventId);
        if (bucket % 60 == 0) {
            return false;
        }
        return !(bucket % 25 == 0 && attempt == 1);
    }

    static long bucket(String eventId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(eventId.getBytes(StandardCharsets.UTF_8));
            return ((digest[0] & 0xFFL) << 24) | ((digest[1] & 0xFFL) << 16)
                    | ((digest[2] & 0xFFL) << 8) | (digest[3] & 0xFFL);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
