package com.github.nlayna.kraken.executor.kafka;

import com.github.nlayna.kraken.executor.records.ResultRow;
import com.github.nlayna.kraken.executor.records.TopicSpec;
import com.github.nlayna.kraken.executor.utils.Assertion;
import com.github.nlayna.kraken.executor.utils.CsvFeeder;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.nlayna.kraken.executor.utils.Utils.*;

@SuppressWarnings("UnstableApiUsage")
public class KafkaTransportRunner implements Callable<Integer> {

    private final String bootstrapServers;
    private final List<String> topicSpecs;
    private final String responseTopic;
    private final String groupId;
    private final String correlationHeader;   // <<< NEW: header name for corr-id

    private final String durationStr;
    private final String timeoutStr;
    private final int maxInFlight;

    private final Path templatePath;
    private final String dateFormat;
    private final Path csvPath;
    private final List<String> assertions;

    private final Map<String, String> kafkaHeaders;
    private final Map<String, String> producerConf;
    private final Map<String, String> consumerConf;
    private final Path reportOut;

    public KafkaTransportRunner(String bootstrapServers, List<String> topicSpecs, String responseTopic, String groupId,
                                String correlationHeader, String durationStr, String timeoutStr, int maxInFlight,
                                Path templatePath, String dateFormat, Path csvPath, List<String> assertions,
                                Map<String, String> kafkaHeaders, Map<String, String> producerConf, Map<String, String> consumerConf,
                                Path reportOut) {
        this.bootstrapServers = bootstrapServers;
        this.topicSpecs = topicSpecs;
        this.responseTopic = responseTopic;
        this.groupId = groupId;
        this.correlationHeader = (correlationHeader == null || correlationHeader.isBlank())
                ? "kafka_correlationId" : correlationHeader;

        this.durationStr = durationStr;
        this.timeoutStr = timeoutStr;
        this.maxInFlight = maxInFlight;
        this.templatePath = templatePath;
        this.dateFormat = (dateFormat == null) ? "yyyy-MM-dd'T'HH:mm:ss" : dateFormat;
        this.csvPath = csvPath;
        this.assertions = (assertions == null) ? List.of() : assertions;
        this.kafkaHeaders = (kafkaHeaders == null) ? Map.of() : kafkaHeaders;
        this.producerConf = (producerConf == null) ? Map.of() : producerConf;
        this.consumerConf = (consumerConf == null) ? Map.of() : consumerConf;
        this.reportOut = reportOut;
    }

    @Override
    public Integer call() throws Exception {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("--bootstrap-servers is required when --transport KAFKA");
        }

        final var dateFormatter = DateTimeFormatter.ofPattern(this.dateFormat);
        final var duration = parseDuration(durationStr);
        final var end = Instant.now().plus(duration);
        final var timeout = parseDuration(timeoutStr);

        final var template = templatePath != null ? Files.readString(templatePath) : null;
        final var feeder = new CsvFeeder(loadCsv(csvPath));
        final var checks = new ArrayList<Assertion>();
        for (var a : assertions) {
            checks.add(new Assertion(a));
        }

        // --- Kafka clients
        var prodProps = getProducerProps();
        prodProps.putAll(producerConf);
        var producer = new KafkaProducer<String, String>(prodProps);

        KafkaConsumer<String, String> consumer = null;
        final boolean awaitResponse = responseTopic != null && !responseTopic.isBlank();
        if (awaitResponse) {
            var consProps = getConsProps();
            consProps.putAll(consumerConf);
            consumer = new KafkaConsumer<>(consProps);
            consumer.subscribe(List.of(responseTopic));
        }

        // corr map
        final var awaiting = new ConcurrentHashMap<String, CompletableFuture<Resp>>();

        // consumer loop completes futures based on header value
        Thread consumerThread = null;
        if (awaitResponse) {
            var c = consumer;
            consumerThread = Thread.ofPlatform().name("kraken-consumer").start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        var records = c.poll(java.time.Duration.ofMillis(200));
                        for (ConsumerRecord<String, String> rec : records) {
                            var corrId = readHeader(rec.headers(), correlationHeader);
                            if (corrId == null) {
                                continue; // not for us
                            }

                            var fut = awaiting.remove(corrId);
                            if (fut != null) {
                                var body = rec.value() == null ? "" : rec.value();
                                fut.complete(new Resp(body, body.getBytes(StandardCharsets.UTF_8).length));
                            }
                        }
                    } catch (Exception ignore) {
                    }
                }
            });
        }

        // RPS per topic
        var topics = new ArrayList<TopicSpec>();
        var limiters = new LinkedHashMap<TopicSpec, RateLimiter>();
        for (var spec : topicSpecs) {
            var ts = parseTopic(spec);
            topics.add(ts);
            limiters.put(ts, RateLimiter.create(Math.max(0.000001, ts.rps())));
        }

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var inFlight = new Semaphore(Math.max(1, maxInFlight));

            try (var writer = Files.newBufferedWriter(reportOut, StandardCharsets.UTF_8);
                 var out = new PrintWriter(writer)) {
                out.println("timestamp,startEpochMs,latencyMs,status,success,error,method,path,vu,requestBytes,responseBytes");
                out.flush();

                var dispatchers = new ArrayList<Future<?>>();
                var vuCounter = new AtomicInteger(0);

                for (var ts : topics) {
                    var rl = limiters.get(ts);
                    Future<?> f = pool.submit(() -> {
                        while (Instant.now().isBefore(end)) {
                            rl.acquire();
                            if (Instant.now().isAfter(end)) break;
                            inFlight.acquireUninterruptibly();
                            int vu = vuCounter.incrementAndGet();
                            pool.submit(() -> {
                                try {
                                    var row = sendOnce(producer, ts.topic(), awaitResponse, awaiting, template, feeder,
                                            kafkaHeaders, checks, correlationHeader,
                                            timeout, vu, dateFormatter);
                                    synchronized (out) {
                                        out.printf(Locale.ROOT,
                                                "%s,%d,%d,%d,%s,%s,%s,%s,%d,%d,%d%n",
                                                row.isoTs(), row.startEpochMs(), row.latencyMs(), row.status(), row.success(),
                                                escape(row.error()), row.method(), row.path(), row.vu(), row.reqBytes(), row.respBytes());
                                        out.flush();
                                    }
                                } finally {
                                    inFlight.release();
                                }
                            });
                        }
                    });
                    dispatchers.add(f);
                }

                for (Future<?> f : dispatchers) {
                    f.get();
                }

                pool.shutdown();
                pool.awaitTermination(1, TimeUnit.HOURS);
            }
        } finally {
            if (consumerThread != null) consumerThread.interrupt();
            if (consumer != null) consumer.close();
            producer.close();
        }

        System.out.println("Done. Report → " + reportOut.toAbsolutePath());
        return 0;
    }

    private Properties getConsProps() {
        var consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consProps.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return consProps;
    }

    private Properties getProducerProps() {
        var prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        prodProps.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "5");
        prodProps.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        return prodProps;
    }

    record Resp(String body, long bytes) {
    }

    ResultRow sendOnce(KafkaProducer<String, String> producer,
                       String topic,
                       boolean awaitResponse,
                       ConcurrentMap<String, CompletableFuture<Resp>> awaiting,
                       String template,
                       CsvFeeder feeder,
                       Map<String, String> extraHeaders,
                       List<Assertion> checks,
                       String correlationHeader,
                       java.time.Duration timeout,
                       int vu,
                       DateTimeFormatter dateFormatter) {
        var bindings = new HashMap<>(feeder.next());
        var body = template != null ? renderTemplate(template, bindings, dateFormatter) : "";
        long reqBytes = body.getBytes(StandardCharsets.UTF_8).length;

        // key is optional, still allowed for partitioning
        var key = UUID.randomUUID().toString();

        // create correlation id in header
        var corrId = UUID.randomUUID().toString();
        var record = new ProducerRecord<>(topic, key, body);
        // user headers first
        for (var e : extraHeaders.entrySet()) {
            record.headers().add(new RecordHeader(e.getKey(), e.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        // our correlation header last (so consumer's lastHeader() finds ours)
        record.headers().add(new RecordHeader(correlationHeader, corrId.getBytes(StandardCharsets.UTF_8)));

        long startMs = System.currentTimeMillis();
        long nanoStart = System.nanoTime();

        int status = 0; // N/A for Kafka
        boolean success = false;
        String error = null;
        long respBytes = 0;

        try {
            if (awaitResponse) {
                var fut = new CompletableFuture<Resp>();
                awaiting.put(corrId, fut);               // register BEFORE send to avoid race
                producer.send(record).get(30, TimeUnit.SECONDS);
                Resp resp = fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                String text = resp.body();
                respBytes = resp.bytes();
                success = true;
                if (!checks.isEmpty()) {
                    boolean all = true;
                    for (Assertion a : checks) {
                        try {
                            boolean ok = a.evaluate(text);
                            if (!ok) {
                                all = false;
                                error = append(error, "assertion failed: " + a);
                            }
                        } catch (Exception ex) {
                            all = false;
                            error = append(error, "assertion error: " + a + " → " + ex.getMessage());
                        }
                    }
                    success = success && all;
                }
            } else {
                producer.send(record).get(30, TimeUnit.SECONDS);
                success = true;
            }
        } catch (Exception e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            success = false;
        } finally {
            awaiting.remove(corrId);
        }

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoStart);
        return new ResultRow(dateFormatter.format(LocalDateTime.now()), startMs, latencyMs, status, success, error,
                "KAFKA", topic, vu, reqBytes, respBytes);
    }

    private TopicSpec parseTopic(String spec) {
        var parts = spec.split("@");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Bad topic spec (expect topic@RPS): " + spec);
        }
        return new TopicSpec(parts[0].trim(), Integer.parseInt(parts[1].trim()));
    }

    private String readHeader(Headers headers, String name) {
        var h = headers.lastHeader(name);
        return (h == null) ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
