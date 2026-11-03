package poc;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/** Reconciles every stage by event id. Mirrors scripts/reconcile.py. */
public final class Reconcile {
    static final Set<String> ELIGIBLE = Set.of(
            "PAYMENT_VALIDATED", "PAYMENT_NOT_AUTHORISED", "PAYMENT_INVALIDATED", "PAYMENT_NOT_COMPLETED");

    private Reconcile() {
    }

    static Map<String, Object> compare(Set<String> expected, List<String> actual) {
        Set<String> distinct = new HashSet<>(actual);
        Set<String> missing = new HashSet<>(expected);
        missing.removeAll(distinct);
        Set<String> unexpected = new HashSet<>(distinct);
        unexpected.removeAll(expected);
        Map<String, Object> out = new TreeMap<>();
        out.put("expected", expected.size());
        out.put("actual_records", actual.size());
        out.put("distinct_actual", distinct.size());
        out.put("missing", missing.size());
        out.put("unexpected", unexpected.size());
        out.put("duplicates", actual.size() - distinct.size());
        out.put("exact_match", distinct.equals(expected) && actual.size() == distinct.size());
        return out;
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args.length > 0 ? args[0] : Config.RUN_ROOT);
        List<Path> producerFiles;
        try (Stream<Path> files = Files.walk(root)) {
            producerFiles = files.filter(p -> p.getFileName().toString().equals("producer_deliveries.jsonl")).toList();
        }
        Set<String> produced = new HashSet<>();
        Set<String> initiated = new HashSet<>();
        Set<String> eligible = new HashSet<>();
        long deliveries = 0;
        for (Path file : producerFiles) {
            for (String line : Files.readAllLines(file)) {
                JsonNode d = Events.MAPPER.readTree(line);
                deliveries++;
                String id = d.get("eventId").asText();
                produced.add(id);
                String type = d.get("eventType").asText();
                if (type.equals("PAYMENT_INITIATED")) {
                    initiated.add(id);
                }
                if (ELIGIBLE.contains(type)) {
                    eligible.add(id);
                }
            }
        }
        long failed = 0;
        for (Path file : producerFiles) {
            Path failedFile = file.resolveSibling("failed_events.jsonl");
            if (Files.exists(failedFile)) {
                failed += Files.readAllLines(failedFile).stream().filter(s -> !s.isBlank()).count();
            }
        }

        List<Reader.Record> lifecycle = Reader.read(Config.LIFECYCLE_TOPIC, Events.LIFECYCLE, null);
        List<String> lifecycleIds = lifecycle.stream().map(r -> r.event().get("eventId").asText()).toList();
        long brokerTotal = Reader.endOffsets(Config.LIFECYCLE_TOPIC).values().stream().mapToLong(Long::longValue).sum();
        List<String> fraudSources = Reader.read(Config.FRAUD_SCORES_TOPIC, Events.FRAUD_SCORE, null).stream()
                .map(r -> r.event().get("sourceEventId").asText()).toList();
        List<String> notificationSources = Reader.read(Config.NOTIFICATION_EVENTS_TOPIC, Events.NOTIFICATION, null)
                .stream().map(r -> r.event().get("sourceEventId").asText()).toList();

        Path archive = Path.of("output", Config.AUDIT_ARCHIVER_GROUP, "archive.jsonl");
        List<String> auditIds = new ArrayList<>();
        if (Files.exists(archive)) {
            for (String line : Files.readAllLines(archive)) {
                auditIds.add(Events.MAPPER.readTree(line).get("eventId").asText());
            }
        }

        Map<String, Object> stages = new TreeMap<>();
        stages.put("lifecycle_topic", compare(produced, lifecycleIds));
        stages.put("fraud_scores", compare(initiated, fraudSources));
        stages.put("notification_events", compare(eligible, notificationSources));
        stages.put("audit_archive", compare(produced, auditIds));
        Map<String, Boolean> checks = new TreeMap<>();
        checks.put("producer_failed_events_is_zero", failed == 0);
        checks.put("broker_end_offsets_equal_producer_deliveries", brokerTotal == deliveries);
        checks.put("lifecycle_topic_matches_producer_ids_exactly", (Boolean) ((Map<?, ?>) stages.get("lifecycle_topic")).get("exact_match"));
        checks.put("fraud_scores_cover_every_initiated_payment_exactly", (Boolean) ((Map<?, ?>) stages.get("fraud_scores")).get("exact_match"));
        checks.put("notifications_cover_every_eligible_payment_exactly", (Boolean) ((Map<?, ?>) stages.get("notification_events")).get("exact_match"));
        checks.put("audit_archive_matches_producer_ids_exactly", (Boolean) ((Map<?, ?>) stages.get("audit_archive")).get("exact_match"));
        boolean passed = checks.values().stream().allMatch(Boolean::booleanValue);

        TreeMap<String, Object> report = new TreeMap<>();
        report.put("producer_deliveries", deliveries);
        report.put("producer_failed", failed);
        report.put("broker_lifecycle_end_offsets_total", brokerTotal);
        report.put("stages", stages);
        report.put("checks", checks);
        report.put("passed", passed);
        Files.writeString(Path.of("evidence/phase6_reconciliation_java.json"),
                Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println(Events.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        if (!passed) {
            System.exit(1);
        }
    }
}
