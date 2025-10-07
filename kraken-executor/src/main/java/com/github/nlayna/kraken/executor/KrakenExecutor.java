package com.github.nlayna.kraken.executor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.github.nlayna.kraken.executor.rest.RestTransportRunner;
import com.github.nlayna.kraken.executor.kafka.KafkaTransportRunner;

@Command(
        name = "kraken-executor",
        mixinStandardHelpOptions = true,
        version = "1.1.0",
        description = "Virtual-threads load tester with REST or Kafka transport."
)
public class KrakenExecutor implements Callable<Integer> {

    public enum Transport { REST, KAFKA }

    // -------- shared options ----------
    @Option(names = "--transport", defaultValue = "REST", description = "Transport to use: REST or KAFKA")
    Transport transport;

    @Option(names = "--duration", required = true, description = "Test duration: 30s, 2m, 1h or ISO-8601 PT…")
    String durationStr;

    @Option(names = "--threads", defaultValue = "100", description = "Max in-flight requests (virtual users)")
    int maxInFlight;

    @Option(names = "--body-template", description = "Path to request JSON template")
    Path templatePath;

    @Option(names = "--date-format", defaultValue = "yyyy-MM-dd'T'HH:mm:ss", description = "Date/time format for {{RQTM}}")
    String dateFormat;

    @Option(names = "--csv", description = "Path to CSV file (headers used as {{tokens}})")
    Path csvPath;

    @Option(names = "--assert", description = "JSONPath assertion like $.statusCode==1 (repeatable)")
    List<String> assertions;

    @Option(names = "--report", description = "CSV report output path", defaultValue = "report.csv")
    Path reportOut;

    // -------- REST-only ----------
    @Option(names = "--base-url", description = "Base URL, e.g. https://api.example.com")
    String baseUrl;

    @Option(names = "--endpoint", required = true, description = "REST: METHOD:/path@RPS  |  Kafka: topic@RPS (repeatable)", split = ",")
    List<String> endpointSpecs;

    @Option(names = "--header", description = "REST header key=value (repeatable)")
    Map<String,String> headers;

    @Option(names = "--keystore", description = "PKCS12 client key store (for mTLS)")
    Path keystorePath;
    @Option(names = "--keystore-password", description = "Client keystore password")
    String keystorePassword;
    @Option(names = "--truststore", description = "PKCS12 trust store")
    Path truststorePath;
    @Option(names = "--truststore-password", description = "Truststore password")
    String truststorePassword;

    // -------- Kafka-only ----------
    @Option(names = "--bootstrap-servers", description = "Kafka bootstrap servers (Kafka only)")
    String bootstrapServers;

    @Option(names = "--response-topic", description = "Kafka response topic to await (optional). If absent, we measure produce-ack only.")
    String responseTopic;

    @Option(names = "--group-id", defaultValue = "kraken-load-test", description = "Kafka consumer group id (when using --response-topic)")
    String groupId;

    @Option(names = "--correlation-header", defaultValue = "kafka_correlationId", description = "Kafka header name to carry the correlation UUID")
    String correlationHeader;

    @Option(names = "--kafka-header", description = "Kafka record header key=value (repeatable)")
    Map<String,String> kafkaHeaders;

    @Option(names = "--producer-conf", description = "Kafka ProducerConfig key=value (repeatable)")
    Map<String,String> producerConf;

    @Option(names = "--consumer-conf", description = "Kafka ConsumerConfig key=value (repeatable)")
    Map<String,String> consumerConf;

    @Option(names = "--timeout", defaultValue = "60s", description = "Max time to await a response per message, e.g. 5s, 60s, 2m")
    String timeoutStr;

    @Override public Integer call() throws Exception {
        return switch (transport) {
            case REST -> new RestTransportRunner(
                    baseUrl, endpointSpecs, durationStr, maxInFlight, templatePath, dateFormat, csvPath,
                    assertions, headers, keystorePath, keystorePassword, truststorePath, truststorePassword, reportOut
            ).call();
            case KAFKA -> new KafkaTransportRunner(
                    bootstrapServers, endpointSpecs, responseTopic, groupId,
                    correlationHeader, durationStr, timeoutStr, maxInFlight,
                    templatePath, dateFormat, csvPath, assertions,
                    kafkaHeaders, producerConf, consumerConf, reportOut
            ).call();
        };
    }

    public static void main(String[] args) {
        int code = new CommandLine(new KrakenExecutor()).execute(args);
        System.exit(code);
    }
}
