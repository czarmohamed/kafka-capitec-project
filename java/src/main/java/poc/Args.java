package poc;

import java.util.HashMap;
import java.util.Map;

/** Parses "--name value" pairs. */
public final class Args {
    private Args() {
    }

    public static Map<String, String> parse(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected an option, found " + args[i]);
            }
            out.put(args[i].substring(2), args[i + 1]);
        }
        return out;
    }
}
