package poc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Append-only text file. Lines are buffered until commit(), which writes them in one call and forces
 * them to disk on an open channel. Replaces the SQLite files used by the Python consumers, because the
 * SQLite driver is blocked by the artifact mirror.
 */
public final class AppendLog implements AutoCloseable {
    private final FileChannel channel;
    private final StringBuilder pending = new StringBuilder();

    public AppendLog(Path path) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<String> readAll(Path path) {
        try {
            return Files.exists(path) ? Files.readAllLines(path, StandardCharsets.UTF_8) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void append(String line) {
        pending.append(line).append('\n');
    }

    /** Writes the buffered lines and forces them to disk. Offsets may be committed only after this returns. */
    public void commit() {
        if (pending.length() == 0) {
            // Nothing new since the last commit, so that commit already forced everything written so far.
            return;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(pending.toString().getBytes(StandardCharsets.UTF_8));
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            pending.setLength(0);
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        commit();
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
