package poc;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Checks that related lifecycle events stay together and in order. Mirrors scripts/verify_ordering.py. */
public final class VerifyOrdering {
    private static final Set<String> FULL_SHAPES = new HashSet<>();

    static {
        for (Journeys.Outcome o : Journeys.OUTCOMES) {
            List<String> shape = new ArrayList<>(o.path());
            shape.add(o.terminal());
            FULL_SHAPES.add(String.join(",", shape));
        }
    }

    private VerifyOrdering() {
    }

    static boolean isValidJourneyOrPrefix(List<String> types) {
        String joined = String.join(",", types);
        for (String shape : FULL_SHAPES) {
            if (shape.startsWith(joined)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        List<Reader.Record> records = Reader.read(Config.LIFECYCLE_TOPIC, Events.LIFECYCLE, null);
        Map<String, List<Reader.Record>> byPayment = new HashMap<>();
        Map<String, Set<Integer>> partitionsByCustomer = new HashMap<>();
        long keyMismatches = 0;
        for (Reader.Record r : records) {
            byPayment.computeIfAbsent(r.event().get("paymentId").asText(), k -> new ArrayList<>()).add(r);
            partitionsByCustomer.computeIfAbsent(r.event().get("customerId").asText(), k -> new HashSet<>())
                    .add(r.partition());
            if (!r.event().get("customerId").asText().equals(r.key())) {
                keyMismatches++;
            }
        }

        TreeMap<String, Long> counts = new TreeMap<>();
        for (List<Reader.Record> recs : byPayment.values()) {
            recs.sort((a, b) -> Long.compare(a.offset(), b.offset()));
            List<String> types = recs.stream().map(r -> r.event().get("eventType").asText()).toList();
            List<String> times = recs.stream().map(r -> r.event().get("eventTime").asText()).toList();
            List<Long> offsets = recs.stream().map(Reader.Record::offset).toList();
            counts.merge("payments", 1L, Long::sum);
            if (recs.stream().map(Reader.Record::partition).distinct().count() == 1) {
                counts.merge("one_partition", 1L, Long::sum);
            }
            boolean sequenceOk = true;
            for (int i = 0; i < recs.size(); i++) {
                if (recs.get(i).event().get("sequence").asInt() != i + 1) {
                    sequenceOk = false;
                }
            }
            if (sequenceOk) {
                counts.merge("sequence_in_offset_order", 1L, Long::sum);
            }
            boolean timesIncrease = true;
            for (int i = 1; i < times.size(); i++) {
                if (times.get(i - 1).compareTo(times.get(i)) >= 0) {
                    timesIncrease = false;
                }
            }
            if (timesIncrease) {
                counts.merge("event_time_strictly_increasing", 1L, Long::sum);
            }
            if (types.get(0).equals("PAYMENT_INITIATED") && isValidJourneyOrPrefix(types)) {
                counts.merge("valid_journey_or_prefix", 1L, Long::sum);
            }
            if (FULL_SHAPES.contains(String.join(",", types))) {
                counts.merge("complete_journeys", 1L, Long::sum);
            } else {
                counts.merge("partial_journeys", 1L, Long::sum);
            }
            boolean contiguous = true;
            for (int i = 1; i < offsets.size(); i++) {
                if (offsets.get(i) != offsets.get(i - 1) + 1) {
                    contiguous = false;
                }
            }
            if (contiguous) {
                counts.merge("contiguous_offsets", 1L, Long::sum);
            }
        }

        long split = partitionsByCustomer.values().stream().filter(s -> s.size() > 1).count();
        long payments = counts.getOrDefault("payments", 0L);
        Map<String, Boolean> checks = new TreeMap<>();
        checks.put("every_payment_in_one_partition", counts.getOrDefault("one_partition", 0L) == payments);
        checks.put("every_sequence_in_offset_order", counts.getOrDefault("sequence_in_offset_order", 0L) == payments);
        checks.put("every_event_time_increases", counts.getOrDefault("event_time_strictly_increasing", 0L) == payments);
        checks.put("every_payment_is_a_valid_journey_or_prefix",
                counts.getOrDefault("valid_journey_or_prefix", 0L) == payments);
        checks.put("every_customer_in_one_partition", split == 0);
        checks.put("every_record_key_is_customer_id", keyMismatches == 0);
        boolean passed = checks.values().stream().allMatch(Boolean::booleanValue);

        TreeMap<String, Object> report = new TreeMap<>();
        report.put("topic", Config.LIFECYCLE_TOPIC);
        report.put("records", records.size());
        report.put("customers", partitionsByCustomer.size());
        report.put("customers_split_across_partitions", split);
        report.put("payments", payments);
        report.put("complete_journeys", counts.getOrDefault("complete_journeys", 0L));
        report.put("partial_journeys", counts.getOrDefault("partial_journeys", 0L));
        report.put("payments_with_contiguous_offsets", counts.getOrDefault("contiguous_offsets", 0L));
        report.put("checks", checks);
        report.put("passed", passed);
        Path out = Path.of("evidence/phase6_ordering_report_java.json");
        Files.writeString(out, Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        if (!passed) {
            System.exit(1);
        }
    }
}
