package com.github.nlayna.kraken.executor.utils;

import com.opencsv.CSVReader;

import javax.net.ssl.*;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static final Pattern TOKEN = Pattern.compile("\\{\\{([A-Za-z0-9_]+)\\}\\}");

    public static Duration parseDuration(String s) {
        try {
            if (s == null) throw new IllegalArgumentException("duration null");
            s = s.trim();
            if (s.startsWith("PT") || s.startsWith("pt")) {
                return Duration.parse(s);
            }

            if (s.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
            }
            long seconds = Long.parseLong(s.substring(0, s.length() - 1));
            if (s.endsWith("s")) {
                return Duration.ofSeconds(seconds);
            }
            if (s.endsWith("m")) {
                return Duration.ofMinutes(seconds);
            }
            if (s.endsWith("h")) {
                return Duration.ofHours(seconds);
            }

            return Duration.parse(s); // last resort
        } catch (Exception e) {
            throw new IllegalArgumentException("Bad duration '" + s + "'", e);
        }
    }

    public static String renderTemplate(String template,
                                        Map<String, String> bindings,
                                        DateTimeFormatter dateTimeFormatter) {
        Matcher m = TOKEN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String value = switch (key) {
                case "RQUID" -> UUID.randomUUID().toString().replace("-", "");
                case "RQTM" -> LocalDateTime.now().format(dateTimeFormatter);
                default -> bindings.getOrDefault(key, m.group(0));
            };
            m.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static SSLContext buildSslContext(Path keystore, String ksPass, Path truststore, String tsPass)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        KeyManager[] kms = null;
        TrustManager[] tms = null;

        if (keystore != null) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(keystore)) {
                ks.load(in, ksPass != null ? ksPass.toCharArray() : new char[0]);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, ksPass != null ? ksPass.toCharArray() : new char[0]);
            kms = kmf.getKeyManagers();
        }
        if (truststore != null) {
            KeyStore ts = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(truststore)) {
                ts.load(in, tsPass != null ? tsPass.toCharArray() : new char[0]);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            tms = tmf.getTrustManagers();
        }
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kms, tms, new SecureRandom());
        return ctx;
    }

    public static String escape(String s) {
        if (s == null) return "";
        return '"' + s.replace("\"", "''").replace("\n", " ") + '"';
    }

    public static String append(String s, String extra) {
        return (s == null || s.isEmpty()) ? extra : (s + "; " + extra);
    }

    public static List<Map<String, String>> loadCsv(Path path) {
        if (path == null) {
            return List.of();
        }

        try (CSVReader r = new CSVReader(new FileReader(path.toFile(), StandardCharsets.UTF_8))) {
            List<String[]> all = r.readAll();
            if (all.isEmpty()) {
                return List.of();
            }

            String[] header = all.getFirst();
            List<Map<String, String>> rows = new ArrayList<>();
            for (int i = 1; i < all.size(); i++) {
                String[] row = all.get(i);
                Map<String, String> m = new HashMap<>();
                for (int c = 0; c < header.length && c < row.length; c++) {
                    m.put(header[c], row[c]);
                }
                rows.add(m);
            }

            return rows;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV: " + path + ", " + e.getMessage(), e);
        }
    }
}
