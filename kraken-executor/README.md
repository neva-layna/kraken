# Kraken Executor

A Java 21 virtual‑threads load tester that can drive **HTTP/REST** *and* **Kafka** with one CLI. It reuses the same features across transports: RPS limiting, duration, virtual users, request templating with CSV feeders, JSONPath assertions, mTLS (REST), configurable Kafka Producer/Consumer settings, and a unified CSV report.

---

## ✨ Features

* **Transports**: `--transport REST` or `--transport KAFKA`
* **RPS limiting per target**: `METHOD:/path@RPS` or `topic@RPS`
* **Virtual users (Java 21 virtual threads)** with `--threads` cap on in‑flight requests/messages
* **Duration control**: `30s`, `2m`, `1h`, or ISO‑8601 `PT…`
* **Templated request bodies** with CSV data feed and built‑ins `{{RQUID}}`, `{{RQTM}}`
* **JSONPath assertions** on responses (works for REST responses or Kafka response messages)
* **mTLS for REST** via P12 keystore/truststore
* **Kafka header‑based correlation** (default header `kafka_correlationId` with a UUID)
* **Unified CSV report** compatible with spreadsheet/BI tools

---

## Quick start

### Build

```bash
mvn -DskipTests package
```

Jar will be under `target/` (e.g. `kraken-executor.jar`).

### REST example

```bash
java -jar target/kraken-executor.jar \
  --transport REST \
  --base-url https://api.example.com \
  --endpoint POST:/v1/process@10 \
  --duration 60s \
  --threads 200 \
  --body-template request.json \
  --csv data.csv \
  --assert "$.statusCode==1" \
  --keystore client.p12 --keystore-password changeit \
  --truststore truststore.p12 --truststore-password changeit \
  --report out.csv
```

### Kafka example (end‑to‑end request/response)

```bash
java -jar target/kraken-executor.jar \
  --transport KAFKA \
  --bootstrap-servers localhost:9092 \
  --endpoint request-topic@500 \
  --response-topic response-topic \
  --group-id kraken-load \
  --duration 2m \
  --threads 200 \
  --timeout 30s \
  --body-template request.json \
  --csv data.csv \
  --assert "$.statusCode==1" \
  --key-jsonpath "$.rqUid" \
  --correlation-header kafka_correlationId \
  --kafka-header app=kraken \
  --producer-conf acks=1 --producer-conf linger.ms=5 --producer-conf compression.type=zstd \
  --consumer-conf max.poll.records=1000 \
  --report out.csv
```

### Kafka example (produce‑ack latency only)

If you don’t have a response topic, omit `--response-topic` to measure only broker ack latency:

```bash
java -jar target/kraken-executor.jar \
  --transport KAFKA \
  --bootstrap-servers localhost:9092 \
  --endpoint request-topic@2000 \
  --duration 60s \
  --threads 1000 \
  --body-template request.json \
  --csv data.csv \
  --report out.csv
```

---

## CLI overview

### Common

* `--transport` : `REST` (default) or `KAFKA`
* `--endpoint` :

    * REST: `METHOD:/path@RPS` (repeatable, comma‑separated)
    * Kafka: `topic@RPS` (repeatable, comma‑separated)
* `--duration` : e.g. `60s`, `2m`, `1h`, or ISO‑8601 `PT1M`
* `--threads` : max in‑flight operations (virtual users)
* `--body-template` : JSON template file
* `--date-format` : format for `{{RQTM}}` (default `yyyy-MM-dd'T'HH:mm:ss`)
* `--csv` : CSV file used as data feeder
* `--assert` : JSONPath assertion (repeatable), e.g. `$.statusCode==1`
* `--report` : CSV report path (default `report.csv`)

### REST‑only

* `--base-url` : e.g. `https://api.example.com`
* `--header` : HTTP header `k=v` (repeatable)
* `--keystore` / `--keystore-password` : client P12 for mTLS
* `--truststore` / `--truststore-password` : truststore P12

### Kafka‑only

* `--bootstrap-servers` : bootstrap list
* `--response-topic` : response topic to await; if absent, only produce‑ack is measured
* `--group-id` : consumer group when awaiting responses (default `kraken-loadtest`)
* `--timeout` : max time to await a response per message (e.g. `30s`)
* `--kafka-header` : add record header `k=v` (repeatable)
* `--key-jsonpath` : derive Kafka key from request JSON (optional; falls back to random UUID)
* `--correlation-header` : header name carrying UUID correlation id (default `kafka_correlationId`)
* `--producer-conf` : pass any `ProducerConfig` as `k=v` (repeatable)
* `--consumer-conf` : pass any `ConsumerConfig` as `k=v` (repeatable)

---

## Templates & CSV feeder

**Template example** (`request.json`):

```json
{
  "rqUid": "{{RQUID}}",
  "rqTm": "{{RQTM}}",
  "fl1": "{{c1}}",
  "fl2": "{{c2}}"
}
```

**CSV example** (`data.csv`):

```csv
c1,c2
some-id1,some-data1
some-id2,some-data2
```

Built‑ins:

* `{{RQUID}}` → random UUID
* `{{RQTM}}` → current timestamp formatted via `--date-format`
* `{{<csvHeader>}}` → value from current CSV row

Each send fills a new set of bindings and renders the body. For REST the body is the HTTP payload. For Kafka it is the record value.

---

## Assertions (JSONPath)

Add one or more `--assert` checks that must all pass for a row to be marked `success=true`.

Examples:

```bash
--assert "$.statusCode==1"
--assert "$.payload.data.length()>0"
--assert "$.errors.size()==0"
```

If evaluation throws (bad JSON, missing path), the assertion is recorded as an error in the report.

---

## RPS & virtual users

* Per‑target RPS is enforced by Guava `RateLimiter` using the `@RPS` portion of `--endpoint`.
* `--threads` caps the number of in‑flight operations (virtual users). With Java 21 virtual threads, blocking for I/O or response waits is cheap.

---

## TLS/mTLS (REST)

Provide P12 keystore/truststore to enable client cert auth and custom trust roots:

```bash
--keystore client.p12 --keystore-password secret \
--truststore truststore.p12 --truststore-password secret
```

---

## Kafka correlation via header

For end‑to‑end timing, the tool **creates a UUID** per request and puts it into the header named by `--correlation-header` (default `kafka_correlationId`). The consumer thread looks for the same header on the response message and matches it to the request.

> **Backend requirement**: echo the `kafka_correlationId` header from the request to the response you publish on `--response-topic`.

**Partitioning key**: optionally derive the Kafka key from the request body using `--key-jsonpath` (e.g., `$.rqUid`). If absent, a random UUID is used.

---

## Reporting

A single CSV schema for both transports:

Header:

```
timestamp,startEpochMs,latencyMs,status,success,error,method,path,vu,requestBytes,responseBytes
```

Field notes:

* `timestamp` : ISO string when the row was recorded
* `startEpochMs` : epoch milliseconds at send start
* `latencyMs` :

    * REST: total HTTP round‑trip time
    * Kafka (with `--response-topic`): time from send to receiving the correlated response
    * Kafka (without `--response-topic`): broker ack latency for the produce
* `status` : HTTP status for REST; **0** for Kafka rows (not applicable)
* `success` : `true` if transport succeeded **and** all assertions passed
* `error` : message if send/eval failed (escaped)
* `method` : HTTP method for REST, or `KAFKA`
* `path` : HTTP path for REST, or Kafka topic
* `vu` : virtual user id at the time of dispatch
* `requestBytes` / `responseBytes` : serialized sizes

Example row:

```
2025-01-01T12:00:00,1735732800000,42,200,true,,POST,/v1/process,17,182,512
```

---

## Tips & recipes

* **Multiple targets**: pass comma‑separated `--endpoint` values to mix RPS per path/topic.
* **Headers**: `--header k=v` (REST) and `--kafka-header k=v` (Kafka) are repeatable.
* **SASL/SSL for Kafka**: use `--producer-conf`/`--consumer-conf` to supply properties such as `security.protocol`, `sasl.mechanism`, `ssl.truststore.location`, etc.
* **Time windows**: prefer `--consumer-conf auto.offset.reset=latest` for response consumers in load tests.
* **Backpressure**: increase `--threads` when responses are slower than your target RPS; decrease when brokers/servers throttle.

---

## Troubleshooting

* **No responses matched**: ensure the backend **echoes the `kafka_correlationId` header** onto the response topic.
* **Assertion failures**: dump a sample response and validate your JSONPath expression offline.
* **mTLS handshake errors**: verify P12 contents and passwords; ensure truststore contains the server CA.
* **Kafka timeouts**: bump `--timeout`, `max.poll.records`, or consumer fetch sizes via `--consumer-conf`.
