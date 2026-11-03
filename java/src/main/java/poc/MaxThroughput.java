package poc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures how fast this machine can produce, with threads. Payloads are encoded before the timed
 * window, so the window measures the client and the brokers. Mirrors scripts/max_throughput.py.
 *
 *   --label NAME  --processes N  --events TOTAL  --compression zstd|lz4
 *   --batch-size BYTES  --linger-ms MS  --topic TOPIC (default digitalpayments.loadtest.v1)
 */
public final class MaxThroughput {
    static final int WARM_UP_PARTITIONS = 12;

    private MaxThroughput() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = Args.parse(args);
        String label = opts.get("label");
        int threads = Integer.parseInt(opts.getOrDefault("processes", "1"));
        long total = Long.parseLong(opts.getOrDefault("events", "200000"));
        String compression = opts.getOrDefault("compression", "zstd");
        String batch = opts.getOrDefault("batch-size", "65536");
        String linger = opts.getOrDefault("linger-ms", "5");
        String topic = opts.getOrDefault("topic", "digitalpayments.loadtest.v1");
        long perThread = total / threads;

        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int index = i;
            Thread t = new Thread(() -> {
                try {
                    results.add(run(index, perThread, compression, batch, linger, topic, barrier));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            workers.add(t);
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }

        double window = results.stream().mapToDouble(r -> (double) r.get("elapsed_s")).max().orElse(0);
        long delivered = results.stream().mapToLong(r -> (long) r.get("delivered")).sum();
        long failed = results.stream().mapToLong(r -> (long) r.get("failed")).sum();
        long bytes = results.stream().mapToLong(r -> (long) r.get("bytes")).sum();
        ObjectNode report = Events.object();
        report.put("label", label);
        report.put("implementation", "java");
        report.put("processes", threads);
        report.put("topic", topic);
        ObjectNode settings = report.putObject("settings");
        settings.put("compression", compression);
        settings.put("batch_size", Long.parseLong(batch));
        settings.put("linger_ms", Long.parseLong(linger));
        report.put("events_requested", perThread * threads);
        report.put("delivered", delivered);
        report.put("failed", failed);
        report.put("window_seconds", Math.round(window * 100) / 100.0);
        report.put("events_per_second", Math.round(delivered / window));
        report.put("payload_mb_per_second", Math.round(bytes / window / 1e6 * 10) / 10.0);
        report.put("payload_mbit_per_second", Math.round(bytes * 8 / window / 1e6 * 10) / 10.0);
        Path dir = Path.of("evidence/max_throughput");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(label + ".json"), Events.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(report));
        System.out.println(Events.MAPPER.writeValueAsString(report));
    }

    private static Map<String, Object> run(int index, long count, String compression, String batch, String linger,
                                           String topic, CyclicBarrier barrier) throws Exception {
        List<byte[][]> payloads = new ArrayList<>();
        for (ObjectNode event : Journeys.flatten(Journeys.generate((int) (count / 3) + 10, 1000 + index,
                Instant.now(), UUID.randomUUID().toString()))) {
            if (payloads.size() == count) {
                break;
            }
            payloads.add(new byte[][]{event.get("customerId").asText().getBytes(StandardCharsets.UTF_8),
                    Events.encode(event, Events.LIFECYCLE)});
        }

        Properties p = Config.producerProps("loadtest-" + index);
        p.put("compression.type", compression);
        p.put("batch.size", batch);
        p.put("linger.ms", linger);
        RecordSink sink = KafkaSinks.producer(p);

        for (int partition = 0; partition < WARM_UP_PARTITIONS; partition++) {
            sink.send(topic, "warm-up".getBytes(StandardCharsets.UTF_8), "{}".getBytes(StandardCharsets.UTF_8),
                    partition, (error, pt, off) -> {
                    });
        }
        sink.flush();

        AtomicLong delivered = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        List<String> errors = new CopyOnWriteArrayList<>();
        barrier.await();
        long started = System.nanoTime();
        for (byte[][] payload : payloads) {
            sink.send(topic, payload[0], payload[1], null, (error, pt, off) -> {
                if (error == null) {
                    delivered.incrementAndGet();
                } else {
                    failed.incrementAndGet();
                    if (errors.size() < 3) {
                        errors.add(String.valueOf(error));
                    }
                }
            });
        }
        sink.flush();
        double elapsed = (System.nanoTime() - started) / 1e9;
        sink.close();

        long bytes = 0;
        for (byte[][] payload : payloads) {
            bytes += payload[1].length;
        }
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("delivered", delivered.get());
        result.put("failed", failed.get());
        result.put("bytes", bytes);
        result.put("elapsed_s", elapsed);
        return result;
    }
}
