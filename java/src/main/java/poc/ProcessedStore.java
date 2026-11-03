package poc;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/** Event ids already handled by one consumer group. Mirrors src/common/state.py. */
public final class ProcessedStore implements AutoCloseable {
    private final Set<String> seen = new HashSet<>();
    private final AppendLog log;

    public ProcessedStore(Path file) {
        for (String line : AppendLog.readAll(file)) {
            int tab = line.indexOf('\t');
            if (tab > 0) {
                seen.add(line.substring(0, tab));
            }
        }
        this.log = new AppendLog(file);
    }

    public boolean seen(String eventId) {
        return seen.contains(eventId);
    }

    public void mark(String eventId, String status) {
        if (seen.add(eventId)) {
            log.append(eventId + "\t" + status + "\t" + Instant.now());
        }
    }

    public void commit() {
        log.commit();
    }

    @Override
    public void close() {
        log.close();
    }
}
