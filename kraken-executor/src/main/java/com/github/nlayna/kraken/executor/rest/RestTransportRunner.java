package com.github.nlayna.kraken.executor.rest;

import com.github.nlayna.kraken.executor.records.Endpoint;
import com.github.nlayna.kraken.executor.records.ResultRow;
import com.github.nlayna.kraken.executor.utils.Assertion;
import com.github.nlayna.kraken.executor.utils.CsvFeeder;
import com.google.common.util.concurrent.RateLimiter;

import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
public class RestTransportRunner implements Callable<Integer> {

    // options copied from old class
    final String baseUrl;
    final List<String> endpointSpecs;
    final String durationStr;
    final int maxInFlight;
    final Path templatePath;
    final String dateFormat;
    final Path csvPath;
    final List<String> assertions;
    final Map<String,String> headers;
    final Path keystorePath;
    final String keystorePassword;
    final Path truststorePath;
    final String truststorePassword;
    final Path reportOut;

    public RestTransportRunner(String baseUrl, List<String> endpointSpecs, String durationStr, int maxInFlight,
                               Path templatePath, String dateFormat, Path csvPath, List<String> assertions,
                               Map<String, String> headers,
                               Path keystorePath, String keystorePassword, Path truststorePath, String truststorePassword,
                               Path reportOut) {
        this.baseUrl = baseUrl;
        this.endpointSpecs = endpointSpecs;
        this.durationStr = durationStr;
        this.maxInFlight = maxInFlight;
        this.templatePath = templatePath;
        this.dateFormat = dateFormat == null ? "yyyy-MM-dd'T'HH:mm:ss" : dateFormat;
        this.csvPath = csvPath;
        this.assertions = assertions;
        this.headers = headers == null ? new LinkedHashMap<>() : headers;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.reportOut = reportOut;
    }

    @Override public Integer call() throws Exception {
        var dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        var duration = parseDuration(durationStr);
        var end = Instant.now().plus(duration);

        var limiters = new LinkedHashMap<Endpoint, RateLimiter>();
        List<Endpoint> endpoints = new ArrayList<>();
        for (var spec : endpointSpecs) {
            var ep = parseEndpoint(spec);
            endpoints.add(ep);
            limiters.put(ep, RateLimiter.create(Math.max(0.000001, ep.rps())));
        }

        final String template = templatePath != null ? Files.readString(templatePath) : null;
        var feeder = new CsvFeeder(loadCsv(csvPath));

        var checks = new ArrayList<Assertion>();
        if (assertions != null) for (var a : assertions) checks.add(new Assertion(a));

        var clientBuilder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30));
        if (keystorePath != null || truststorePath != null) {
            var ssl = buildSslContext(keystorePath, keystorePassword, truststorePath, truststorePassword);
            clientBuilder.sslContext(ssl);
        }

        try (var client = clientBuilder.build()) {
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                var inFlight = new Semaphore(Math.max(1, maxInFlight));

                try (var writer = Files.newBufferedWriter(reportOut, StandardCharsets.UTF_8);
                     var out = new PrintWriter(writer)) {
                    out.println("timestamp,startEpochMs,latencyMs,status,success,error,method,path,vu,requestBytes,responseBytes");
                    out.flush();

                    var dispatchers = new ArrayList<Future<?>>();
                    var vuCounter = new AtomicInteger(0);

                    for (var ep : endpoints) {
                        var rl = limiters.get(ep);
                        Future<?> f = pool.submit(() -> {
                            while (Instant.now().isBefore(end)) {
                                rl.acquire();
                                if (Instant.now().isAfter(end)) break;
                                inFlight.acquireUninterruptibly();
                                int vu = vuCounter.incrementAndGet();
                                pool.submit(() -> {
                                    try {
                                        var row = executeOnce(client, baseUrl, ep, template, feeder, headers, checks, vu, dateFormatter);
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
                    for (Future<?> f : dispatchers) f.get();

                    pool.shutdown();
                    pool.awaitTermination(1, TimeUnit.HOURS);
                }
            }
        }
        System.out.println("Done. Report → " + reportOut.toAbsolutePath());
        return 0;
    }

    ResultRow executeOnce(HttpClient client, String baseUrl, Endpoint ep, String template,
                          CsvFeeder feeder, Map<String,String> headers, List<Assertion> checks,
                          int vu, java.time.format.DateTimeFormatter dateTimeFormatter) {
        var bindings = new HashMap<>(feeder.next());
        var body = template != null ? renderTemplate(template, bindings, dateTimeFormatter) : null;
        long reqBytes = body == null ? 0 : body.getBytes(StandardCharsets.UTF_8).length;

        var rb = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + ep.path()))
                .timeout(Duration.ofSeconds(60));

        headers.forEach(rb::header);
        if ("GET".equalsIgnoreCase(ep.method())) {
            rb.GET();
        } else {
            if (headers.keySet().stream().noneMatch(h -> h.equalsIgnoreCase("Content-Type"))) {
                rb.header("Content-Type", "application/json");
            }
            rb.method(ep.method(), HttpRequest.BodyPublishers.ofString(body != null ? body : ""));
        }

        var req = rb.build();
        var start = System.currentTimeMillis();
        var nanoStart = System.nanoTime();
        int status = -1;
        boolean success = false;
        String error = null;
        long respBytes = 0;
        try {
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            status = resp.statusCode();
            respBytes = resp.body() == null ? 0 : resp.body().length;
            var text = resp.body() == null ? "" : new String(resp.body(), StandardCharsets.UTF_8);
            success = status >= 200 && status < 400;
            if (checks != null && !checks.isEmpty()) {
                boolean all = true;
                for (Assertion a : checks) {
                    try {
                        boolean ok = a.evaluate(text);
                        if (!ok) { all = false; error = append(error, "assertion failed: " + a); }
                    } catch (Exception ex) {
                        all = false; error = append(error, "assertion error: " + a + " → " + ex.getMessage());
                    }
                }
                success = success && all;
            }
        } catch (Exception e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoStart);
        return new ResultRow(dateTimeFormatter.format(LocalDateTime.now()), start, latencyMs, status, success, error,
                ep.method(), ep.path(), vu, reqBytes, respBytes);
    }

    Endpoint parseEndpoint(String spec) {
        var parts = spec.split("@");
        if (parts.length != 2) throw new IllegalArgumentException("Bad endpoint spec (expect METHOD:/path@RPS): " + spec);
        var mp = parts[0].split(":", 2);
        if (mp.length != 2) throw new IllegalArgumentException("Bad endpoint METHOD:/path in: " + spec);
        var method = mp[0].trim().toUpperCase(Locale.ROOT);
        var path = mp[1].trim();
        int rps = Integer.parseInt(parts[1].trim());
        if (!path.startsWith("/")) path = "/" + path;
        return new Endpoint(method, path, rps);
    }
}
