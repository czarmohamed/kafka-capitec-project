package poc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {
    @Test
    void producerBatchesFitUnderTheTopicMessageLimit() {
        int limit = 1_048_576; // max.message.bytes on the payment topics
        assertTrue(Integer.parseInt(Config.producerProps("test").getProperty("batch.size")) <= limit);
    }

    @Test
    void producerIsDurable() {
        assertEquals("all", Config.producerProps("test").getProperty("acks"));
        assertEquals("true", Config.producerProps("test").getProperty("enable.idempotence"));
    }

    @Test
    void consumerCommitsManually() {
        assertEquals("false", Config.consumerProps("g", "c").getProperty("enable.auto.commit"));
    }
}
