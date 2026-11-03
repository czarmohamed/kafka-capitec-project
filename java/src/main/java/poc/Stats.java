package poc;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Percentile summaries. Same method as scripts/measure_latency.py. */
final class Stats {
    private Stats() {
    }

    static double percentile(List<Double> sorted, double fraction) {
        int index = (int) Math.round(fraction * (sorted.size() - 1));
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
    }

    static Map<String, Object> summarise(List<Double> values, double slaMs) {
        List<Double> sorted = values.stream().sorted().toList();
        Map<String, Object> out = new TreeMap<>();
        if (sorted.isEmpty()) {
            out.put("count", 0);
            return out;
        }
        long over = sorted.stream().filter(v -> v > slaMs).count();
        out.put("count", sorted.size());
        out.put("p50_ms", round(percentile(sorted, 0.50)));
        out.put("p95_ms", round(percentile(sorted, 0.95)));
        out.put("p99_ms", round(percentile(sorted, 0.99)));
        out.put("max_ms", round(sorted.get(sorted.size() - 1)));
        out.put("mean_ms", round(sorted.stream().mapToDouble(Double::doubleValue).average().orElse(0)));
        out.put("sla_ms", slaMs);
        out.put("over_sla", over);
        out.put("within_sla_percent", Math.round(10000.0 * (sorted.size() - over) / sorted.size()) / 100.0);
        return out;
    }

    static double round(double value) {
        return Math.round(value * 100) / 100.0;
    }
}
