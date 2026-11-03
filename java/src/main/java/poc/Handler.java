package poc;

import com.fasterxml.jackson.databind.JsonNode;

/** One consumer group's business logic. Mirrors the Python handler classes. */
public interface Handler {
    boolean accepts(JsonNode event);

    void handle(JsonNode event);

    /** Called once per batch, after the handler's outputs have been queued. */
    default void flush() {
    }
}
