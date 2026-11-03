# Digital Payments Event Streaming: Setup and Verification

**Project:** Digital Payments Event Streaming Infrastructure (proof of concept)
**Cluster:** local 3-broker Kafka (KRaft) on Rancher Desktop Kubernetes, namespace `default`
**Status:** the Java implementation is complete. The Python implementation has been removed. The fraud p99 SLA is met (section 6.5). The producer exceeds 1000 Mb/s in bursts (section 6.6). The 1000 Mb/s target for the consumers is not met on this machine. Each group needs about 274,000 events per second, and the best durable runs reached 257,909 (notification), 212,585 (fraud) and 274,826 (audit, one run) (section 9.5). The 7-year retention is configured but not exercised (section 9.3).

---

## Contents

1. [Summary](#1-summary)
2. [How to run it](#2-how-to-run-it)
3. [Design decisions](#3-design-decisions)
4. [Topic creation](#4-topic-creation)
5. [Producer setup](#5-producer-setup)
6. [Consumer groups and verification](#6-consumer-groups-and-verification)
7. [Trade-offs and justifications](#7-trade-offs-and-justifications)
8. [Issues encountered](#8-issues-encountered)
9. [Conclusions and open items](#9-conclusions-and-open-items)
10. [Evidence index](#10-evidence-index)

---

## 1. Summary

| Acceptance criterion (brief, section 6) | Result |
|---|---|
| Design decisions documented | Section 3, with trade-offs in section 7 |
| Topic created with chosen configuration | 3 topics created; `--describe` confirms RF 3, min ISR 2, zstd, delete cleanup, 7-year or 30-day retention (section 4) |
| Producer running and sending mock events | 52,538 events delivered, 0 failed, 0 unsent (section 5) |
| Multiple payment flows, at least 2 complete journeys | 13,845 payments: 13,844 complete journeys and 1 partial, from the final burst (section 6.2) |
| At least 3 independent consumer groups | `fraud-scoring`, `notification-sender`, `audit-archiver` (section 6.1) |
| Consumers processing, output meaningful | Fraud scores 13,845 (one per initiated payment); notifications 14,300; audit archive 52,538 (section 6.3) |
| Event ordering verified | All 13,845 payments in one partition, sequence in offset order (section 6.2) |
| Consumer lag low or zero | Lag 0 on all 36 partitions across the three groups (section 6.1) |
| Trade-offs explained | Section 7 |

**Headline results**

- **Zero loss and zero duplicates end to end.** Event IDs match exactly at every stage: producer, lifecycle topic, fraud scores, notifications, and audit archive. Dead-letter count is 0.
- **Broker failure survived.** Deleting broker `kafka-1` during a live run delivered all 3,047 events with no failures and no application-level retries.
- **Producer acknowledgement is fast.** A payment event becomes durable in a median of 1.2 ms, with p99 at 4.6 ms, against a 150 ms budget (section 5.5). The cluster is one node, so this is a lower bound.
- **Notification latency meets its 2 s SLA** with a wide margin: p99 281 ms and 100% within SLA.
- **Fraud latency meets its SLA at p99.** Across five standard runs, p99 was 25 to 37 ms against a 50 ms SLA, and 99.3% to 99.7% of scores were within 50 ms. The earlier 200 ms tail came from clients connecting over IPv6 first (section 6.5).

---

## 2. How to run it

Run everything from the project root. The project is Java only; the commands below use the `java/` build.

### 2.1 Cluster

The cluster manifest is `/Users/AzharMohamed/dev/kafka-setup/kafka-statefulset.yml`. Brokers reach the host through NodePorts 30092, 30093, and 30094. The UI is on NodePort 30080.

```bash
kubectl get pods -l app=kafka
kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server kafka-service:9092 describe --status
```

#### Starting the cluster from cold

The cluster runs Kafka in KRaft mode, so there is no ZooKeeper to start. Everything runs as Kubernetes objects in Rancher Desktop.

1. **Start Rancher Desktop** and wait until Kubernetes reports running. Select its context and check the node:

   ```bash
   kubectl config use-context rancher-desktop
   kubectl get nodes
   ```

   The node `lima-rancher-desktop` should show `Ready`.

2. **Validate both manifests** without changing anything:

   ```bash
   cd /Users/AzharMohamed/dev/kafka-setup
   kubectl apply --dry-run=server -f kafka-statefulset.yml
   kubectl apply --dry-run=server -f kafka-Ui.yml
   ```

3. **Apply them:**

   ```bash
   kubectl apply -f kafka-statefulset.yml
   kubectl apply -f kafka-Ui.yml
   ```

   The first file creates the three-broker StatefulSet, the headless and internal services, and the three external NodePort services for ports 30092 to 30094. Each broker's data volume is created from `volumeClaimTemplates` on the `local-path` storage class. The second file creates the Kafka UI pod and its NodePort service on port 30080.

4. **Wait for the brokers:**

   ```bash
   kubectl rollout status statefulset/kafka --timeout=300s
   kubectl get pods -l app=kafka
   ```

   All three pods should show `1/1 Running`.

5. **Check the controller quorum and the client path:**

   ```bash
   kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server kafka-service:9092 describe --status
   kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --list
   nc -z localhost 30092 && echo "broker 0 reachable from the host"
   ```

   The quorum output should show a `LeaderId` and three `CurrentVoters`. The topic list should include the three POC topics.

6. **Check the UI:**

   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" http://localhost:30080/
   ```

   This should print `200`.

**Restart behaviour.** The `kafka-ui` pod is a bare pod, not managed by a controller, so if it disappears (for example after a node restart), run `kubectl apply -f kafka-Ui.yml` again. Broker data survives pod restarts. The failure drill in section 6.4 confirms this: offsets written before the deleted broker restarted were still present afterwards. A full Rancher Desktop restart has not been tested.

**Client connections.** Brokers advertise `localhost`, which resolves to `::1` first. Every Java command therefore passes `-Djava.net.preferIPv4Stack=true`, and the bootstrap addresses are IPv4 NodePorts, so connections go straight to IPv4 and do not wait out a retry.

Broker settings used for this POC:

- Memory limit 1536Mi, request 1Gi, CPU limit 1
- `KAFKA_HEAP_OPTS=-Xms512m -Xmx512m`
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, so topics are only created explicitly
- `min.insync.replicas=2` and replication factor 3 for internal topics

### 2.2 Topics

```bash
./scripts/create_topics.sh
```

### 2.3 Build

Run every command from the project root. The project is Java only. Maven resolves the dependencies into `java/target/cp.txt`, and `javac` compiles the code, because the artifact mirror blocks the Maven build plugins.

```bash
CP="java/target/classes:$(cat java/target/cp.txt)"
javac -encoding UTF-8 -d java/target/classes -cp "$CP" $(find java/src/main/java -name '*.java')
```

### 2.4 Producer

```bash
# 1,000 complete payment journeys, sent in order, one acknowledgement at a time
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ProducerMain --journeys 1000 --seed 42

# 20,000 events, queued without waiting, for throughput
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ProducerMain --burst 20000 --seed 7
```

Each run writes to `output/journeys-<timestamp>` or `output/burst-<timestamp>` unless `--output-dir` is given.

### 2.5 Consumers

Each consumer group runs as its own process. `--idle-exit` stops it after that many seconds without messages.

```bash
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group fraud --idle-exit 30
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group notification --idle-exit 30
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group audit --idle-exit 30
```

### 2.6 Verification

```bash
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.VerifyOrdering
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.Reconcile
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.MeasureAckLatency
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.MeasureLatency
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.MaxThroughput --label <name> --processes 1 --compression lz4 --batch-size 262144 --linger-ms 20 --events 1500000
CONSUMER_BACKLOG_TOPIC=<backlog-topic> java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerThroughput --group fraud --instances 12 --backlog 1500000
scripts/failure_drill.sh
kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group fraud-scoring
```

### 2.7 Tests

```bash
CP="java/target/classes:$(cat java/target/cp.txt)"
javac -encoding UTF-8 -d java/target/test-classes -cp "$CP" $(find java/src/test/java -name '*.java')
java -cp "java/target/test-classes:$CP" poc.TestRunner
```

There are 49 unit tests. They cover the event checks, the journey generator, producer retries, the consumer loop, and each handler.

---

## 3. Design decisions

### 3.1 Topics

| Topic | Purpose | Partitions | RF | min ISR | Retention | Cleanup | Compression | Max message |
|---|---|---|---|---|---|---|---|---|
| `digitalpayments.payment.lifecycle.v1` | Payment state transitions (producer output) | 12 | 3 | 2 | 220,752,000,000 ms (7 years) | delete | zstd | 1,048,576 bytes |
| `digitalpayments.fraud.scores.v1` | Fraud scores (fraud producer output) | 12 | 3 | 2 | 220,752,000,000 ms (7 years) | delete | zstd | 1,048,576 bytes |
| `digitalpayments.notification.events.v1` | Notification outcomes (notification producer output) | 12 | 3 | 2 | 2,592,000,000 ms (30 days) | delete | zstd | 1,048,576 bytes |

Every topic has `retention.bytes=-1`, so size never deletes data. Retention is time-based only.

A fourth topic, `digitalpayments.health.warmup.v1` (3 partitions, 1-hour retention), holds only the producer warm-up messages from section 6.5.3. No consumer reads it, and it is not part of the payment data.

**Topic name.** Format is `<domain>.<entity>.<stream>.<version>`, lowercase, dot-separated, with hyphens only inside words. The `.v1` suffix allows a breaking schema change later as `.v2` without disturbing consumers. The domain prefix `digitalpayments` matches the existing team naming.

**Partitions: 12 for the POC, 100 for production.** The brief's assumptions are 1000 Mb/s peak and 10 Mb/s per consumer instance. That implies about 100 consumer instances in one group, and one partition can feed only one instance in a group. So production needs about 100 partitions. I used 12 here because the local cluster has 1.5Gi brokers, and 100 partitions at RF 3 is 300 partition replicas on three small brokers. The count cannot be raised later without changing how keys map to partitions, so the production number should be fixed at creation.

**Replication factor 3 and min ISR 2.** With `acks=all`, a write is acknowledged once two of three replicas hold it. One broker can fail with no data loss and no write outage. Losing two brokers stops writes, which is deliberate: for payments, unavailability is safer than loss.

**Retention: time-based, 7 years.** The brief requires a 7-year hold for payment and fraud events. Notification events have no stated requirement, so I used 30 days.

I did not use size-based retention as a safety net. A size cap would delete compliance data silently once the disk filled. Disk growth should be monitored and alerted on instead.

**Cleanup: `delete`, not compaction.** Payment events are immutable facts in an event stream, not rows in a state table. Compaction would discard history that the audit requirement needs.

**Compression: zstd.** JSON compresses well, so zstd reduces network and storage cost for a modest CPU cost. I set it on the topic as well as the producer, so the topic setting holds whatever the producer does.

**Max message size: 1 MB.** Real events are about 0.5 KB. The limit is 1 MB so the producer can send 256 KB batches, which the throughput in section 6.6 depends on. A 1 MB limit catches oversized records less tightly than 16 KB did. The schema caps individual fields but not the size of a whole record.

### 3.2 Message schema

Every event is validated against a JSON Schema before it is serialised, and again after it is deserialised. Schemas are in [schemas/](schemas/). Each has `additionalProperties: false`, so unexpected fields such as a card number are rejected.

**Lifecycle event** (`payment_lifecycle_event.schema.json`)

| Field | Type | Purpose |
|---|---|---|
| `eventId` | UUID | Unique per event; consumers dedupe on it |
| `schemaVersion` | const 1 | Allows schema evolution |
| `eventType` | enum | One of the seven state transitions |
| `eventTime` | UTC timestamp, ms | When the transition happened |
| `paymentId` | UUID | Groups the events of one payment |
| `customerId` | `CUST-` + 8 digits | Tokenised customer reference; also the partition key |
| `merchantId` | `MERCH-` + 6 digits | Merchant |
| `channel` | enum | `MOBILE_APP`, `WEB`, `POS`, `QR` |
| `paymentType` | enum | `EFT`, `CARD_PRESENT`, `CARD_NOT_PRESENT`, `QR` |
| `amountMinor` | integer, at least 1 | Amount in cents. Never a float |
| `currency` | ISO 4217 | For example `ZAR` |
| `sequence` | integer, at least 1 | Position within the payment; used to verify ordering |
| `idempotencyKey` | string, 8 to 64 chars | Identical on every event of one payment |
| `reasonCode` | string or null | Set on failure events, for example `CARD_DECLINED` |
| `deviceRef` | SHA-256 hex or null | Hashed device identifier, never the raw ID |

**Fraud score event** (`fraud_score_event.schema.json`): `eventId`, `sourceEventId` (the scored `PAYMENT_INITIATED` event), `paymentId`, `customerId`, `score` (0 to 100), `band` (`LOW`, `MEDIUM`, `HIGH`), `modelVersion`.

**Notification event** (`notification_event.schema.json`): `eventId`, `sourceEventId`, `paymentId`, `customerId`, `notificationType` (`PAYMENT_APPROVED` or `PAYMENT_FAILED`), `channel`, `deliveryStatus` (`SENT` or `FAILED`), `attempts`.

**Why these fields.** Fraud needs the amount, channel, payment type, device, and recent activity per customer. Notifications need the customer, the outcome, and a reason. Reconciliation needs `eventId`, `paymentId`, `sequence`, and the timestamps. Audit needs everything, so the audit consumer stores the full payload.

**Money.** Amounts are integers in minor units, with an explicit currency. A float amount would be accepted by a naive JSON Schema check, because `250.0` is a whole number, so the encoder rejects any float in an event.

**Privacy.** The schema holds no card numbers, account numbers, or raw device identifiers. Customer and merchant IDs are tokens. This follows POPIA's data-minimisation principle.

### 3.3 Producer

**Event types.** The producer publishes seven types, one per state transition in the brief:

```
PAYMENT_INITIATED
  -> PAYMENT_AUTHORISED                  -> PAYMENT_VALIDATED -> PAYMENT_COMPLETED
  -> PAYMENT_NOT_AUTHORISED                                   -> PAYMENT_NOT_COMPLETED
  -> PAYMENT_AUTHORISED -> PAYMENT_INVALIDATED
```

Every payment starts with `PAYMENT_INITIATED` and ends in exactly one terminal state. The four journey shapes are:

| Outcome | Weight | Events | Terminal event and reason codes |
|---|---|---|---|
| Completed | 84% | 4 | `PAYMENT_COMPLETED` |
| Not authorised | 8% | 2 | `PAYMENT_NOT_AUTHORISED`: `INSUFFICIENT_FUNDS`, `CARD_DECLINED`, `DAILY_LIMIT_EXCEEDED` |
| Invalidated | 5% | 3 | `PAYMENT_INVALIDATED`: `ACCOUNT_CLOSED`, `INVALID_BENEFICIARY` |
| Not completed | 3% | 4 | `PAYMENT_NOT_COMPLETED`: `SETTLEMENT_TIMEOUT`, `SETTLEMENT_REJECTED` |

**Partition key: `customerId`.** The brief asks for related events to stay together. Keying by `customerId` puts all of a customer's events in one partition, which keeps every payment's events together (a payment belongs to one customer). It also gives per-customer ordering, which velocity rules in fraud scoring need. The trade-off is that a very active customer, such as a large merchant account, creates a hot partition. Keying by `paymentId` would avoid that, but it would split a customer's payments across partitions and lose per-customer ordering.

**Durability: `acks=all`.** The brief asks whether producers should wait for all replicas. With `min.insync.replicas=2`, `acks=all` waits for two acknowledgements, not three, so one slow broker does not stall writes. The cost is one extra network round trip per write. The measured median is about 1 ms, which leaves most of the 150 ms payment budget free (section 5.5).

**Idempotence.** `enable.idempotence=true` stops retries inside one producer session from creating duplicates. It does not survive a producer restart. So each event carries a stable `eventId`, generated once and reused on every retry. Consumers dedupe on `eventId`, which covers restarts.

**Duplicate payments.** A duplicate payment needs a duplicate `paymentId` from the initiating system. The producer does not create `paymentId`s from retries. `idempotencyKey` is supplied by the initiating system and is identical on every event of one payment, so a downstream payment system can reject a second submission with the same key.

**Serialization: JSON.** No Schema Registry is deployed on this cluster (port 8081 is closed), and JSON is readable in the console tools and the evidence files. Avro or Protobuf would be smaller and would enforce compatibility, and I would choose them for production with a registry in place.

**Error handling.** The ordered producer sends one event at a time, waits for the acknowledgement, and retries with exponential backoff (2, 4, 8, ... up to 30 seconds) for up to 10 minutes. After that, the event goes to `failed_events.jsonl`, and the remaining events of that payment are logged as skipped, so no later event for that payment is sent out of order. The flush timeout is 150 seconds, longer than `delivery.timeout.ms`, so the final result is always seen.

**Stuck window.** A payment can be "stuck" in the producer for up to 10 minutes before it is logged as failed. In production this would be a transactional outbox: the payment is recorded in the database first, and a relay publishes it, so nothing depends on the producer process staying up.

**Volume.** The mock data is 1,000 complete journeys (3,826 events) for the ordered run, plus 20,000-event bursts for throughput. The final dataset is 52,538 events. Section 5 has the breakdown.

### 3.4 Consumer groups

Three independent groups, each with its own offsets, all reading the lifecycle topic:

| Group | Purpose | Reads | Writes | Lag SLA | Why this SLA |
|---|---|---|---|---|---|
| `fraud-scoring` | Real-time fraud scoring | `PAYMENT_INITIATED` only | `fraud.scores.v1` | under 50 ms | Fraud decisions must be made before the payment is authorised. The brief sets 50 ms. |
| `notification-sender` | Customer notifications | `PAYMENT_VALIDATED` (approved), and `PAYMENT_NOT_AUTHORISED`, `PAYMENT_INVALIDATED`, `PAYMENT_NOT_COMPLETED` (failed) | `notification.events.v1` | under 2 s | The brief sets 2 s, which is slow enough that batching is not needed. |
| `audit-archiver` | Regulatory audit archive | All events | SQLite archive (simulated) | under 1 hour in production | Audit only needs completeness, not speed. |

The brief lists the fraud and notification consumers and the audit consumer is an addition. It covers the "regulatory compliance and audit" requirement, and it gives the topic a third independent reader.

**Naming.** Group names are lowercase and hyphen-separated, as the brief asks.

**Offsets.** Offsets are committed manually, only after the handler's output is delivered and the dedupe store is committed. If the process crashes before the commit, the batch is replayed. The dedupe store skips events already handled, so replay does not duplicate work.

**Exactly-once.** Full exactly-once across Kafka and external side effects would need transactions, and the SMS and fraud outputs are not transactional. So the design is at-least-once delivery with an idempotent consumer. Downstream consumers dedupe on `sourceEventId`.

**Ordering within a consumer.** Each partition is read in offset order, and per-customer order is preserved, because a customer's events share a partition. A consumer never handles a later event before an earlier one from the same partition.

**Scalability.** Each group can run up to 12 instances on the 12-partition topics, one per partition. Instances in a group share partitions automatically, using the cooperative-sticky assignment strategy. On this machine, 12 instances per group gave the best results, and 16 and 24 instances were slower (section 9.5). Production needs about 100 partitions for 100 instances. Measured here, an instance manages about 10 Mb/s when all twelve run at once, which matches the brief's assumption (section 6.6).

---

## 4. Topic creation

The script is [scripts/create_topics.sh](scripts/create_topics.sh). It runs `kafka-topics` inside broker `kafka-0`, using `kubectl exec`, so no local Kafka install is needed. It is safe to re-run, because `--if-not-exists` skips existing topics.

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --create --if-not-exists \
  --topic digitalpayments.payment.lifecycle.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=220752000000 \
  --config retention.bytes=-1 \
  --config compression.type=zstd \
  --config max.message.bytes=1048576
```

The fraud and notification topics use the same command with their own names, and notifications use `--config retention.ms=2592000000`.

**Describe output** for the lifecycle topic (captured at creation, when the limit was 16,384 bytes; it was raised to 1,048,576 bytes in section 6.6) (the full output is in [evidence/phase7_describe_digitalpayments.payment.lifecycle.v1.txt](evidence/phase7_describe_digitalpayments.payment.lifecycle.v1.txt)):

```
kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --describe --topic digitalpayments.payment.lifecycle.v1

Topic: digitalpayments.payment.lifecycle.v1	TopicId: kh1Jz8LeRAefF1fsWVhoyw	PartitionCount: 12	ReplicationFactor: 3	Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=16384,retention.bytes=-1
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 0	Leader: 2	Replicas: 2,0,1	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 1	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 2	Leader: 1	Replicas: 1,2,0	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 3	Leader: 2	Replicas: 2,1,0	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 4	Leader: 1	Replicas: 1,0,2	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 5	Leader: 0	Replicas: 0,2,1	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 6	Leader: 0	Replicas: 0,2,1	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 7	Leader: 2	Replicas: 2,1,0	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 8	Leader: 1	Replicas: 1,0,2	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 9	Leader: 2	Replicas: 2,1,0	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 10	Leader: 1	Replicas: 1,0,2	Isr: 0,1,2	Elr: 	LastKnownElr: 
	Topic: digitalpayments.payment.lifecycle.v1	Partition: 11	Leader: 0	Replicas: 0,2,1	Isr: 0,1,2	Elr: 	LastKnownElr: 
```

**Describe output for the other two topics** (headers only; partitions are identical in form):

```
Topic: digitalpayments.fraud.scores.v1        PartitionCount: 12  ReplicationFactor: 3  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220752000000,max.message.bytes=16384,retention.bytes=-1
Topic: digitalpayments.notification.events.v1 PartitionCount: 12  ReplicationFactor: 3  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,retention.ms=2592000000,max.message.bytes=16384,retention.bytes=-1
```

Every partition on all three topics has its full replica set in the in-sync replica set.

**Setup note.** The original payment, fraud, and notification markdown templates had unusable command blocks. Their `--property` flags are not valid for client settings on the console tools, and `--config retention.ms= <value>` breaks the shell because of the space. The scripts in this project supersede them, and the template files are unchanged.

---

## 5. Producer setup

### 5.1 Commands and counts

| Run | Command | Journeys | Events delivered | Failed | Retries | Unsent |
|---|---|---|---|---|---|---|
| Ordered journeys | `--journeys 1000 --seed 42` | 1,000 | 3,826 | 0 | 0 | 0 |
| Burst | `--burst 20000 --seed 7` | (events) | 20,000 | 0 | 0 | 0 |
| Failure drill | `--journeys 800 --seed 99` | 800 | 3,047 | 0 | 0 | 0 |
| Latency runs (5 batches) | `--journeys 300 --seed 77` | 300 each | 5 × 1,133 = 5,665 | 0 | 0 | 0 |
| Throughput burst | `--burst 20000 --seed 123` | (events) | 20,000 | 0 | 0 | 0 |
| **Total on the topic** | | | **52,538** | **0** | **0** | **0** |

The producer's summary of the 20,000-event burst recorded 13,355 events per second with `acks=all` and idempotence, on a single machine.

### 5.2 Example events

The first `PAYMENT_INITIATED` event in the topic (partition 1, offset 0):

```json
{
  "amountMinor": 101100,
  "channel": "WEB",
  "currency": "ZAR",
  "customerId": "CUST-00000093",
  "deviceRef": "df72b1a5e25610ea7603b0346fd6e2fc7f9a8b76c6e3b489e72702b3f2bdde5e",
  "eventId": "8b44d242-30c2-552a-8c81-d05bae03dff6",
  "eventTime": "2026-11-02T08:53:06.087Z",
  "eventType": "PAYMENT_INITIATED",
  "idempotencyKey": "idem-48a181a24d755a21bffdb120",
  "merchantId": "MERCH-000037",
  "paymentId": "48a181a2-4d75-5a21-bffd-b120b154d958",
  "paymentType": "EFT",
  "reasonCode": null,
  "schemaVersion": 1,
  "sequence": 1
}
```

The same payment's `PAYMENT_COMPLETED` event, which is the fourth in the journey (partition 1, offset 3, `sequence` 4):

```json
{
  "amountMinor": 101100,
  "customerId": "CUST-00000093",
  "eventType": "PAYMENT_COMPLETED",
  "paymentId": "48a181a2-4d75-5a21-bffd-b120b154d958",
  "sequence": 4,
  "reasonCode": null
}
```

A failed payment, `PAYMENT_NOT_AUTHORISED` (partition 1, offset 113). Its `reasonCode` is set, and its `deviceRef` is null because the channel is a point-of-sale terminal:

```json
{
  "amountMinor": 243723,
  "channel": "POS",
  "customerId": "CUST-00000024",
  "deviceRef": null,
  "eventType": "PAYMENT_NOT_AUTHORISED",
  "paymentType": "CARD_PRESENT",
  "reasonCode": "CARD_DECLINED",
  "sequence": 2
}
```

### 5.3 Evidence of production

Broker end offsets, summed over the 12 partitions, equal the producer's delivery count after every run. The last check was 52,538 on both sides. Per-partition end offsets match the producer's own per-partition counts. Each delivery is logged with its partition and offset, in `output/<run>/producer_deliveries.jsonl`.

Sample messages from the first ordered run show one payment on one partition, with consecutive offsets (partition 0, offsets 0 to 3, customer `CUST-00000008`). The raw output is in [evidence/phase4_sample_messages.txt](evidence/phase4_sample_messages.txt).

### 5.4 Decisions made during testing

- **Ordered vs burst modes.** The ordered run waits for each acknowledgement, which keeps order without application-level locking. The burst run queues events without waiting, for throughput. Order within a payment still holds, because librdkafka preserves order per partition under idempotence.
- **Generated timestamps.** Event times are generated in advance, from a simulated clock that starts at the moment of generation. The measured latency therefore does not use `eventTime`. Section 6.5 explains the method.

---

### 5.5 Acknowledgement latency

The brief requires real-time payments with a lag under 150 ms. For the producer, that is the time for a payment event to become durable on the broker. [MeasureAckLatency.java](java/src/main/java/poc/MeasureAckLatency.java) sends each event on the ordered path, waits for the `acks=all` acknowledgement, and records how long that took.

| Measure | Value |
|---|---|
| Events sent | 1,144 (300 journeys, seed 88) |
| Discarded as warm-up | 50. The first produce pays a one-time cost to fetch the producer ID. |
| Measured | 1,094 |
| Not acknowledged | 0 |
| p50 | 1.19 ms |
| p95 | 1.81 ms |
| p99 | 4.57 ms |
| Mean | 1.32 ms |
| Maximum | 26.09 ms |
| Within 150 ms | 100% |

Report: [evidence/ack_latency_report.json](evidence/ack_latency_report.json).

**Caveat.** All three brokers run on one Kubernetes node, so replication traffic never crosses a real network. A multi-node deployment would be slower. These figures are a lower bound for this design, not a production estimate. The burst path is asynchronous, so it was not timed for latency.

**Effect on the totals.** This run added 1,144 events to the lifecycle topic after the verification in section 6 had finished. The topic now holds 53,682 events across 14,145 payments. The three groups caught up with zero lag, reconciliation still matches exactly at every stage, and ordering still passes. The figures in sections 5 and 6 come from the verification run (52,538 events, 13,845 payments) and are unchanged.

---

## 6. Consumer groups and verification

### 6.1 Consumer groups: commands, configuration, and lag

**Commands** (each runs as its own process):

```bash
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group fraud --idle-exit 30
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group notification --idle-exit 30
java -Djava.net.preferIPv4Stack=true -cp "$CP" poc.ConsumerMain --group audit --idle-exit 30
```

**Consumer configuration** (from [Config.java](java/src/main/java/poc/Config.java) and [Loop.java](java/src/main/java/poc/Loop.java)):

| Setting | Value | Reason |
|---|---|---|
| `enable.auto.commit` | `false` | Offsets commit only after output is delivered |
| `auto.offset.reset` | `earliest` | The POC reads from the beginning. Production would use `latest` for live consumers |
| `partition.assignment.strategy` | `cooperative-sticky` | Rebalances move only the partitions that change owner |
| `session.timeout.ms` | 45000 | Tolerates a slow batch without triggering a rebalance |
| `max.poll.interval.ms` | 300000 | Allows a long batch before the broker removes the consumer |
| `max.poll.records` | 2000 | Each batch covers 2,000 events, so one state fsync and one offset commit serve the whole batch. Section 9.5 has the measurements |
| Poll timeout (application) | 10 ms | See section 6.5. A longer timeout added about 1 s of latency per message at low load |
| Batch size (application) | 200 | Full batches under load |

**Final lag**, after every group had caught up:

```bash
kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group fraud-scoring
```

| Group | Partitions | Committed offsets total | Lag total | Max partition lag |
|---|---|---|---|---|
| `fraud-scoring` | 12 | 52,538 | 0 | 0 |
| `notification-sender` | 12 | 52,538 | 0 | 0 |
| `audit-archiver` | 12 | 52,538 | 0 | 0 |

Raw output is in [evidence/phase6_final_consumer_lag.txt](evidence/phase6_final_consumer_lag.txt).

#### Sample output from each consumer

Each sample is an actual record from the topic or archive. The input and output are linked by `eventId`, so each consumer's behaviour can be traced end to end.

**`fraud-scoring`** consumed `PAYMENT_INITIATED` event `8b44d242-30c2-552a-8c81-d05bae03dff6` (partition 1, offset 0 of the lifecycle topic). It published this score to `digitalpayments.fraud.scores.v1` (partition 1, offset 0):

```json
{
  "band": "LOW",
  "customerId": "CUST-00000093",
  "eventId": "6e2828f2-fb19-4aeb-a93e-75a8df05aef7",
  "eventTime": "2026-11-02T08:53:42.277Z",
  "modelVersion": "rules-v1",
  "paymentId": "48a181a2-4d75-5a21-bffd-b120b154d958",
  "schemaVersion": 1,
  "score": 10,
  "sourceEventId": "8b44d242-30c2-552a-8c81-d05bae03dff6"
}
```

The score is the base of 10. No rule added to it: the amount is below R5,000, the payment type is EFT, the device is present, and the customer has fewer than three recent initiations.

**`notification-sender`** consumed `PAYMENT_VALIDATED` event `4c3a0f59-7230-56b6-8683-359f5d1fe02c`, which is an approved payment. It published this outcome to `digitalpayments.notification.events.v1` (partition 1, offset 49). The gateway rejected all three attempts, so this record is also the error-handling example:

```json
{
  "attempts": 3,
  "channel": "SMS",
  "customerId": "CUST-00000026",
  "deliveryStatus": "FAILED",
  "eventId": "d3144f99-711c-46fd-ae4a-f4eae8088110",
  "eventTime": "2026-11-02T08:53:40.939Z",
  "notificationType": "PAYMENT_APPROVED",
  "paymentId": "1fe93c9d-8f79-5a77-98f3-a12ddda921b6",
  "schemaVersion": 1,
  "sourceEventId": "4c3a0f59-7230-56b6-8683-359f5d1fe02c"
}
```

A transient failure would instead show `deliveryStatus` `SENT` with `attempts` 2. Failed outcomes are counted in section 6.3. None of the three groups wrote a dead letter in any run.

**`audit-archiver`** writes one row per event to its archive table. These are the rows for the two events above:

| event_id | payment_id | customer_id | event_type | sequence | event_time |
|---|---|---|---|---|---|
| `8b44d242-30c2-552a-8c81-d05bae03dff6` | `48a181a2-4d75-5a21-bffd-b120b154d958` | CUST-00000093 | PAYMENT_INITIATED | 1 | 2026-11-02T08:53:06.087Z |
| `4c3a0f59-7230-56b6-8683-359f5d1fe02c` | `1fe93c9d-8f79-5a77-98f3-a12ddda921b6` | CUST-00000026 | PAYMENT_VALIDATED | 3 | 2026-11-02T09:07:59.587Z |

The archive holds all 52,538 events, one row each. Each row also stores the full event as JSON, so the record can be reproduced exactly.

#### Error handling in action

The table lists each failure path in the design and what the runs showed. The live runs exercised the first four paths. The last three were checked with unit tests or not at all, and the table says which.

| Failure | Handling | What happened |
|---|---|---|
| Producer send fails | Retry with backoff for up to 10 minutes, then log the event to `failed_events.jsonl` and skip the rest of that payment | No send failed in any run: 0 retries, 0 failed. |
| Broker goes down during writes | librdkafka retries inside `delivery.timeout.ms`; the application retries after that | Deleting `kafka-1` during writes gave 0 failures and 0 application retries (section 6.4). |
| Gateway fails transiently | Gateway call retried, up to three attempts | 530 notifications succeeded on attempt 2. Example below. |
| Gateway fails permanently | Recorded as `FAILED` after three attempts, and the outcome is still published | 267 notifications were `FAILED` after three attempts. The example is in the sample output above. |
| Handler raises an exception | Retried up to three times with backoff, then dead-lettered and marked `DEAD_LETTERED` | Not triggered in any live run. Covered by unit tests. |
| Invalid event on the topic | Dead-lettered without being handled, and the offset is still committed | Not triggered in any live run. Covered by a unit test. |
| Output delivery fails | `flush` raises, so the batch is not committed and replays on restart | Not triggered in any live run, and no test covers it. The design is described in section 3.4 only. |

Dead letters in all three groups: 0.

**A notification that recovered on retry.** The gateway failed once and then succeeded. This record was published to `digitalpayments.notification.events.v1` (partition 0, offset 9):

```json
{
  "attempts": 2,
  "channel": "SMS",
  "customerId": "CUST-00000131",
  "deliveryStatus": "SENT",
  "eventId": "24ecf876-68a3-4f9b-8378-e8ccc6058bc5",
  "eventTime": "2026-11-02T08:53:41.852Z",
  "notificationType": "PAYMENT_APPROVED",
  "paymentId": "c3baadc3-3ee4-5258-b9c9-04c38b9b5e54",
  "schemaVersion": 1,
  "sourceEventId": "7306d11d-d3ad-50ef-932e-1ed75aac2001"
}
```

**Unit tests for the dead-letter path** (all pass in the 49-test Java suite, [LoopTest.java](java/src/test/java/poc/LoopTest.java)):

- `test_transient_handler_failures_are_retried`: a handler fails twice, then succeeds. The event is processed, with no dead letter.
- `test_persistent_failure_is_dead_lettered_after_three_attempts`: a handler fails every time. The event is dead-lettered, and it is marked so it is not retried on every replay.
- `test_invalid_payload_is_dead_lettered_and_offset_still_moves_on`: a payload that fails schema validation is dead-lettered, and the offset still advances past it.

### 6.2 Ordering

[VerifyOrdering.java](java/src/main/java/poc/VerifyOrdering.java) reads the whole lifecycle topic and checks each payment.

| Check | Result |
|---|---|
| Records read | 52,538 |
| Customers | 200 |
| Payments | 13,845 |
| Every payment in one partition | Pass |
| Sequence 1..n in offset order | Pass, for all 13,845 payments |
| `eventTime` strictly increasing within each payment | Pass |
| Every customer in one partition | Pass (0 customers split) |
| Every record key equals `customerId` | Pass |
| Complete journeys | 13,844 |
| Partial journeys | 1 (the final payment of the 20,000-event burst, which the burst stopped mid-journey) |
| Payments with contiguous offsets | 13,845 |

The partial journey is expected. The burst stops at an event count, not at a journey boundary. The check accepts a valid prefix of a journey and reports the count. Report: [evidence/phase6_ordering_report.json](evidence/phase6_ordering_report.json).

### 6.3 Reconciliation by event ID

[Reconcile.java](java/src/main/java/poc/Reconcile.java) compares the exact set of event IDs at each stage, not only counts. It reports missing, unexpected, and duplicate records.

| Stage | Expected | Actual | Missing | Unexpected | Duplicates |
|---|---|---|---|---|---|
| Lifecycle topic (all events) | 52,538 | 52,538 | 0 | 0 | 0 |
| Fraud scores (one per `PAYMENT_INITIATED`) | 13,845 | 13,845 | 0 | 0 | 0 |
| Notification events (one per eligible event) | 14,300 | 14,300 | 0 | 0 | 0 |
| Audit archive (one row per event) | 52,538 | 52,538 | 0 | 0 | 0 |

Broker end offsets equal producer deliveries (52,538), and producer failures are 0. The result is saved to [evidence/phase6_reconciliation.json](evidence/phase6_reconciliation.json).

**Consumer outputs.**

- **Fraud bands:** LOW 2,670, MEDIUM 8,652, HIGH 2,523. These reflect the synthetic timeline, which compresses many payments into a few minutes, so the velocity rule fires often. They do not describe real customer behaviour.
- **Notifications:** approved 12,055 and failed 2,245. Of the 14,300 notifications, 530 succeeded on a retry (a simulated transient failure, about 4%) and 267 failed permanently after three attempts (about 2%, all counted as `FAILED`).

### 6.4 Failure drill: broker loss during production

[scripts/failure_drill.sh](scripts/failure_drill.sh) starts an 800-journey run, deletes broker pod `kafka-1` five seconds in, and checks the result. The full log is [evidence/phase6_failure_drill.log](evidence/phase6_failure_drill.log).

```
11:00:12 deleting pod kafka-1 while the producer is writing
11:00:22 under-replicated partitions during outage: 86
producer exit status: 0
11:00:24 kafka-1 is ready again; under-replicated partitions now: 0
lifecycle end offsets before drill: 26092
lifecycle end offsets after drill:  29139
offsets added during drill: 3047
producer summary: delivered 3047, failed 0, retried_attempts 0, unsent_after_flush 0
```

**Result.** All 3,047 events were delivered, and the offsets rose by exactly 3,047. The outage left 86 partitions under-replicated, then returned to zero. No application-level retry was needed, because leader election was fast enough for librdkafka's internal retries to cover the gap.

**Not tested.** Losing two brokers would leave one controller, which loses the KRaft quorum, and that would take down the whole cluster. I did not run that test. By design, writes would stop with `NotEnoughReplicas`, and the producer would retry for up to 10 minutes and then log failures. That is the behaviour I expect, but it is not verified here.

### 6.5 Latency

**Method.** [MeasureLatency.java](java/src/main/java/poc/MeasureLatency.java) does the following:

1. It records the current offsets of each topic.
2. It starts the fraud and notification consumers.
3. It waits until both groups are `STABLE` with members. Starting the producer earlier would count group-join time as latency, which my first attempt did.
4. It produces 300 fresh journeys (1,133 events).
5. It pairs each input event with its output event by `sourceEventId`.
6. Latency is the output message timestamp minus the input message timestamp, both as set by the producer (Kafka `CreateTime`).

`eventTime` is not used. It was generated in advance, so it measures the generator, not the pipeline.

**Results.** The table compares the baseline with the tuned consumer. The baseline is [evidence/tuning_baseline_latency.json](evidence/tuning_baseline_latency.json), and the tuned figures come from [evidence/tuning_after_latency.json](evidence/tuning_after_latency.json).

| Consumer | SLA | Run | p50 | p95 | p99 | Within SLA |
|---|---|---|---|---|---|---|
| Fraud | 50 ms | Baseline (poll timeout 1 s) | 154 ms | 985 ms | 1,034 ms | 8% |
| Fraud | 50 ms | Tuned, attempt 2 | 13 ms | 115 ms | 207 ms | 90% |
| Fraud | 50 ms | Tuned, attempt 3 (saved as the "after" file) | 13 ms | 180 ms | 236 ms | 79% |
| Notification | 2 s | Baseline (poll timeout 1 s) | 152 ms | 989 ms | 1,043 ms | 100% |
| Notification | 2 s | Tuned, attempt 3 | 13 ms | 222 ms | 281 ms | 100% |
| Fraud | 50 ms | Tuned, with IPv4 fix (standard run) | 11 ms | 22 ms | 30 ms | 99.33% |
| Notification | 2 s | Tuned, with IPv4 fix (standard run) | 12 ms | 207 ms | 227 ms | 100% |

Attempt 2's notification figures were p95 278 ms, p99 421 ms, and 100% within SLA. Both tuned attempts were run on the same code, so the spread is run-to-run variation on this machine.

**Result.** The fraud consumer meets its 50 ms SLA at p99 in every run: 25 to 37 ms across five standard runs after the IPv4 fix (section 6.5). Between 99.3% and 99.7% of scores were within 50 ms. Read "lag under 50 ms" as a p99 target, since no finite sample can guarantee every single score. The notification consumer meets its 2 s SLA with a wide margin. The explanation of the earlier tail and the fix are in the next subsection.

**History of the latency runs.** There were five 1,133-event latency batches on the topic. The first ran before the warm-up fix, and its numbers are not valid for steady state. The second is the baseline. The third and fourth are the tuned attempts. One further attempt failed before it produced any events, and I did not capture its error. The failing attempt's folder is empty, and it is not part of the 52,538.

#### Repeat runs

The standard measurement was repeated to check the spread. Each run used the method in section 6.5 with 300 fresh journeys. Run 1 is the standard run from the previous subsection, and runs 2 to 5 are repeats. Each run's report is in `evidence/`.

| Run | Fraud p50 | Fraud p95 | Fraud p99 | Fraud max | Within 50 ms | Notification p50 | Notification p99 |
|---|---|---|---|---|---|---|---|
| 1 | 11 ms | 22 ms | 30 ms | 147 ms | 99.33% | 12 ms | 227 ms |
| 2 | 10 ms | 19 ms | 37 ms | 78 ms | 99.67% | 9 ms | 27 ms |
| 3 | 10 ms | 18 ms | 27 ms | 66 ms | 99.67% | 9 ms | 23 ms |
| 4 | 10 ms | 17 ms | 29 ms | 70 ms | 99.67% | 9 ms | 21 ms |
| 5 | 9 ms | 17 ms | 25 ms | 536 ms | 99.67% | 9 ms | 28 ms |

Fraud p99 ranged from 25 to 37 ms across the five runs. Run 1 had two scores over 50 ms, and each of runs 2 to 5 had one. Run 5 had one score at 536 ms, the largest outlier in the series. Section 6.5.3 explains it. It does not affect p99.

Notification p99 in run 1 was 227 ms, against 21 to 28 ms in runs 2 to 5. Its cause has not been investigated either. It is well inside the 2 s SLA.

Reports: [evidence/fraud_ipv4_latency.json](evidence/fraud_ipv4_latency.json) (run 1), and [evidence/fraud_repeat_run1.json](evidence/fraud_repeat_run1.json) to [evidence/fraud_repeat_run4.json](evidence/fraud_repeat_run4.json) (runs 2 to 5).

#### The 536 ms outlier

**Where it was.** The outlier was the first event sent by the producer in its run (partition 1, offset 3887). Within a second of it, 135 scores across all 12 partitions were produced, and each of them took 30 ms or less. So the delay was not a consumer stall. It affected that one event.

**What it was.** A fresh idempotent producer needs a producer ID before its first write, and the first event waits for that. In the traced runs (section 6.5.2 and the trace script), the first event of each run took 51 to 64 ms to be acknowledged, against at most 18 ms for every other event. The outlier was the same first event, with a much longer wait.

**Why the wait varies.** librdkafka's debug log shows the producer connecting to a broker at about 59 ms, then waiting about 450 ms before it sent its producer-ID request. The broker answered that request in 2 ms. Some starts hit this wait and others did not. Of 30 starts run with debug logging on, 12 took about 520 ms to acknowledge the first event. Without debug logging, most starts took 55 to 65 ms, but one warm-up start took 511 ms. The sample is small, and the trigger for the wait has not been identified. Evidence: [evidence/first_send_probe.json](evidence/first_send_probe.json).

**What it means.** The cost is paid once per producer process, on its first event. A payment that is the first after a service starts can therefore exceed the 150 ms budget. Steady-state events were not affected.

**Warm-up test.** Twenty fresh producer processes sent a journey with no warm-up, and twenty more sent a warm-up journey first. Each process sent two complete journeys, and the table compares the first payment event each one sent after start-up. This version used a journey as the warm-up, and its raw values are in [evidence/warm_up_test.json](evidence/warm_up_test.json). The script now tests the built warm-up, described below.

| Start | Runs | Median | Maximum | Over 100 ms |
|---|---|---|---|---|
| No warm-up | 20 | 63 ms | 528 ms | 7 (six at 514 to 528 ms, one at 122 ms) |
| Warm-up journey first | 20 | 4.6 ms | 8.5 ms | 0 |

The warm-up removed the delay in every warmed start. With 20 trials in each group, the rate of slow starts after a warm-up is probably low, but the test does not prove it is zero.

**Built.** The warm-up is now part of the producer. `Producer.warmUp()` ([Producer.java](java/src/main/java/poc/Producer.java)) sends one message to each partition of the health-check topic `digitalpayments.health.warmup.v1` and waits for every acknowledgement. The payment CLI calls it before any payment is sent, and it raises if a warm-up message is not acknowledged, so the service does not start. Warm-up messages never reach a payment topic.

**Built warm-up test.** The measurement was made with a Python script, since removed. Its Java equivalent is [MeasureWarmUp.java](java/src/main/java/poc/MeasureWarmUp.java). It started 20 fresh producer processes in each group, sending two complete journeys in each. The measured event is the first payment event after start-up. Raw values are in [evidence/warm_up_built_test.json](evidence/warm_up_built_test.json).

| Start | Runs | Median | Maximum | Over 100 ms |
|---|---|---|---|---|
| No warm-up | 20 | 118 ms | 524 ms | 10 |
| Built warm-up | 20 | 12.3 ms | 13.7 ms | 0 |

The no-warm-up group was slower in this run than in the earlier one (median 63 ms, 7 slow starts), so the cold rate varies between runs. The built warm-up was slower at the median than the journey-based warm-up (4.6 ms), and I have not explained that difference. Both are well inside the 150 ms budget.

#### Why the tail was high, and the fix

[ProfileFraud.java](java/src/main/java/poc/ProfileFraud.java) splits each initiated payment's latency into two parts: `queue`, the wait before the handler starts on the event, and `handle`, the time inside the handler. It also times each batch's output flush, state commit, and Kafka commit. The earlier Python profiler (`scripts/profile_fraud.py`) was removed; the figures in this section are from that run.

**Before the fix**, in a live run of 300 scores:

- Handler time was small: p99 about 1 ms. So the latency was almost all queue time.
- Per-batch costs were small at the median: about 2 ms for the output flush, 1 ms for the SQLite commit, and 1 ms for the Kafka commit.
- One batch stalled for about 270 ms in its output flush. Events arriving during that stall waited for it. With 300 scores, the slowest 1% is only three or four events, so that one stall set the p99.

**Cause.** The client logs showed each new broker connection first trying `[::1]`, which is refused, because brokers advertise `localhost`. librdkafka then waits out a reconnect backoff before retrying on `127.0.0.1`. The stalled batch was the first to use a new broker connection.

**Fix.** Set `broker.address.family=v4` on the producer, consumer, and verification clients. It is a client-side setting, so the cluster was not changed.

**After the fix:**

| Run | Scores | p50 | p95 | p99 | Max | Largest output flush |
|---|---|---|---|---|---|---|
| Before the fix (profile run) | 300 | 13 ms | 172 ms | 216 ms | 392 ms | 222 ms |
| After the fix (profile run 1) | 300 | 11 ms | 19 ms | 26 ms | 75 ms | 14 ms |
| After the fix (profile run 2) | 300 | 10 ms | 18 ms | 25 ms | 65 ms | 13 ms |
| After the fix (standard latency run) | 300 | 11 ms | 22 ms | 30 ms | 147 ms | not recorded |

The raw reports are in [evidence/profile_fraud_before_ipv4.json](evidence/profile_fraud_before_ipv4.json), [evidence/profile_fraud_after_ipv4_run1.json](evidence/profile_fraud_after_ipv4_run1.json), and [evidence/profile_fraud_report.json](evidence/profile_fraud_report.json). The per-message timings for the last profile run are in [evidence/profile_fraud_after_ipv4_raw.json](evidence/profile_fraud_after_ipv4_raw.json). The standard run's report is [evidence/fraud_ipv4_latency.json](evidence/fraud_ipv4_latency.json).

**Effect on the totals.** These runs added events to the lifecycle topic, which now holds 176,018 events across 46,468 payments. It includes two 50,000-event payment bursts through the fixed producer (section 6.6). The 45 partial journeys are the 42 probes and the final journey of three of the bursts. That includes 42 single-event payments from the start-up probes in section 6.5.3, which the ordering check counts as partial journeys, along with the one partial journey from the burst. The warm-up tests in section 6.5.3 added 80 processes, each sending two complete journeys, and a 5-journey command-line check added 5 more. The three groups caught up with zero lag, reconciliation still matches exactly at every stage, and ordering still passes. The totals in sections 5 and 6.1 to 6.4 are from the earlier verification run and are unchanged.

### 6.6 Throughput

**Result.** On this machine, the producer's acknowledged throughput exceeds the brief's 1000 Mb/s target in short bursts, with the producer's own settings (zstd, 256 KB batches) on a topic with the 1 MB limit that the payment topics now use. The fastest setting reached about 1,000,000 events per second, which is about 3,660 Mb/s of event payload. Every event was acknowledged with `acks=all` and replication factor 3, and the broker's offset count matches the producers' count exactly: 14,052,093 events across fourteen runs.

Earlier versions of this section reported 13,355 events per second, about 49 Mb/s, and said the target could not be reached here. That figure was wrong. It measured the Python client encoding and generating each event inside the timed window, not Kafka. The corrected measurement is [MaxThroughput.java](java/src/main/java/poc/MaxThroughput.java), which encodes payloads before timing. The earlier Python version was removed; the figures below are from it.

Single process, 1.5 million events per run, on the throwaway topic `digitalpayments.loadtest.v1`:

| Setting | Events per second | Payload Mb/s | Window |
|---|---|---|---|
| zstd, 64 KB batch, 5 ms linger (current payment producer) | 359,953 | 1,315 | 4.2 s |
| zstd, 256 KB batch, 20 ms linger | 848,653 | 3,100 | 1.8 s |
| lz4, 256 KB batch, 20 ms linger | 708,693 | 2,589 | 2.1 s |

Process count, with lz4, 256 KB batch, 20 ms linger, 1.5 million events per run:

| Processes | Events per second | Payload Mb/s |
|---|---|---|
| 1 | 708,693 | 2,589 |
| 2 | 703,683 | 2,570 |
| 4 | 825,044 | 3,014 |

**What the numbers do and do not show.**

- Two processes did not beat one, and four added only about 16%. So the limit is probably shared across the machine rather than inside one client. I have not identified which component sets it. Broker CPU from `kubectl top` stayed under 250 millicores, but that metric averages over too long a window to rule out brief saturation.
- The windows are 1.8 to 4.2 seconds, so these are bursts, not sustained load. At 850,000 events per second, the three replicas write about 650 MB/s to disk. That would fill the 98 GB volume in about 2.5 minutes, so a sustained test is limited by disk, not CPU.
- The 1000 Mb/s target is the speed of a 1 Gb/s network link. Every test here ran over loopback inside one machine, so no network link limited the result. On a real 1 Gb/s link, the target would be the ceiling before replication or consumers added any traffic.
- Consumers were measured separately, below. Each group must read the whole stream, so they set the real limit.
- The zstd and lz4 results differ by about as much as the run-to-run variation, so this test does not rank them.

**The batch-size defect, and how it was fixed.** The payment topics limited messages to 16 KB, but the payment producer's batch size was 64 KB. Under load, a larger batch could be rejected with `MSG_SIZE_TOO_LARGE`. The fix raised the payment topics' limit to 1 MB and set the producer's batch size to 256 KB, so every batch fits with room to spare. A unit test enforces that the batch size stays under the topic limit.

**Measured with the final settings.** With 256 KB batches on the 1 MB limit, the producer delivered 828,310 events per second with one process (3,025 Mb/s), and 1,002,583 with four (3,662 Mb/s). There were no errors, and the broker counts match. A 50,000-event payment burst through the producer on the payment topics had no rejections. Before the limit was raised, a 16 KB batch cap held throughput to about 470 to 510 Mb/s.

#### Consumer throughput

Each consumer group reads the whole stream, so the groups set the requirement. The test ran the real fraud, notification, and audit handlers over a 300,000-event backlog, with their output going to a separate test topic. The Python benchmark was removed; the Java equivalent is [ConsumerThroughput.java](java/src/main/java/poc/ConsumerThroughput.java). Its rate is events read divided by the time from the first event to the last, so group join time and the idle exit are excluded.

| Group | 1 instance | 4 instances | 12 instances |
|---|---|---|---|
| Fraud scoring | 9,003 events/s (33 Mb/s) | 23,460 events/s (86 Mb/s) | 46,727 events/s (170 Mb/s) |
| Notification sender | 9,518 events/s (35 Mb/s) | 32,563 events/s (119 Mb/s) | 49,493 events/s (181 Mb/s) |
| Audit archiver | 6,529 events/s (24 Mb/s) | 22,913 events/s (84 Mb/s) | 35,095 events/s (128 Mb/s) |

All three groups ran concurrently with four instances each, so they competed for the same machine:

| Group | Events per second | Payload Mb/s |
|---|---|---|
| Fraud scoring | 10,876 | 40 |
| Notification sender | 11,203 | 41 |
| Audit archiver | 9,684 | 35 |

Together, the three groups read about 31,800 events per second, or about 116 Mb/s.

**Against the target.** 1000 Mb/s of payload is about 274,000 events per second, and each group must read all of them. The best single group reached 49,493 events per second with twelve instances, about 5.5 times short of that. With the three groups running together, the total is about 25 times short.

**What this shows.**

- The consumers, not the producers, are the bottleneck. The producer sustains about 1,000,000 events per second, well above what any consumer group reads.
- Scaling is close to linear up to four instances and then flattens at twelve. Section 9.5 shows that 16 and 24 instances were slower on the Java consumers. The 10-core host runs the brokers, the consumers, and the test driver together, so the flattening probably reflects that shared load. I haven't identified which component limits it.
- With all twelve instances running at once, each instance managed about 10 Mb/s. That matches the brief's per-consumer assumption. At that rate, 1000 Mb/s per group needs about 100 instances, and so about 100 partitions. The topics have 12 partitions, and this machine can't run 100 instances.
- These figures are from the Python consumers, measured before the Java port. A faster client would raise these numbers, but I haven't tested one.

The sequential four-instance reports were restored from console output, because the concurrent run overwrote their files. The reports are in [evidence/consumer_throughput/](evidence/consumer_throughput/).

**Java consumers, with durable state.** The three consumer groups are in Java ([java/](java/)). The Java consumers write their state to disk and force it at every batch, so these figures are durable. The Python consumers used SQLite for the same purpose. They were measured on a 1,500,000-event backlog, one run per cell:

| Group | 1 instance | 12 instances |
|---|---|---|
| Fraud scoring | 13,395 events/s (49 Mb/s) | 42,791 events/s (156 Mb/s) |
| Notification sender | 20,081 events/s (73 Mb/s) | 84,890 events/s (310 Mb/s) |
| Audit archiver | 15,616 events/s (57 Mb/s) | 46,890 events/s (171 Mb/s) |

An earlier Java figure of about 148,000 events per second for the audit archiver was measured before state was written to disk, so it is not comparable and has been withdrawn. Against the target, each group needs about 274,000 events per second for 1000 Mb/s, so no group reaches the target on this machine. Java at one instance is roughly 1.5 to 2 times the Python production rate measured earlier.

**Test topic.** `digitalpayments.loadtest.v1` has a one-hour retention, so its data expires on its own. It uses the same 1 MB limit as the payment topics.

---

## 7. Trade-offs and justifications

**Partition count: 12 now, 100 for production.** Twelve partitions give enough parallelism for the POC and fit the cluster's memory limits. A production system needs about 100, from the brief's assumptions. The count is fixed at creation, because raising it changes which partition each key maps to. So I'd set 100 on day one, not 12.

**Keying by `customerId`.** This gives per-customer ordering, which velocity rules need, and keeps each payment's events together. The cost is hot partitions for very active customers. A production system would watch partition skew and might split heavy customers across partitions, at the cost of per-customer ordering.

**`acks=all` with `min.insync.replicas=2`.** This accepts a small write-latency cost, a few milliseconds, to survive a broker loss without data loss. The cost of the choice is that losing two brokers stops writes. For payments, I chose that over data loss.

**At-least-once with an idempotent consumer, not exactly-once.** Exactly-once would need Kafka transactions, and the fraud and notification outputs go to systems that cannot take part in a transaction. The consumers dedupe on `eventId`, which gives the same effect for Kafka-to-Kafka flows, and it is simpler to operate.

**Synchronous offset commits per batch.** The commit cost is about 2 ms per batch, so an asynchronous commit would save little. Keeping it synchronous means an offset is never committed before its output is durable.

**Poll timeout of 10 ms.** A short timeout returns quickly when traffic is light, which cut fraud p50 from 154 ms to 13 ms. The cost is more small batches at low load, each with about 20 ms of fixed overhead. Under load, batches still fill to 200, so throughput is not affected.

**Time-based retention, no size cap.** Seven years of payment events cannot be held on local storage at production volumes. At the brief's peak of 1000 Mb/s, seven years is roughly 27.6 PB before replication and compression. A production system would need tiered storage or an archive sink to object storage with immutability. This POC sets the 7-year retention correctly, but the hold is not exercised, because the cluster holds only tens of thousands of events.

**JSON, not Avro or Protobuf.** JSON needs no Schema Registry, and the evidence is human-readable. The cost is larger messages and no enforced compatibility rules. The JSON Schemas partly cover that, but production should move to Avro or Protobuf with a registry.

**SQLite for dedupe and archive.** This keeps the POC self-contained. It is a single-node store, not a durable archive. The audit table is not immutable, so it does not meet a 7-year WORM requirement.

**Rules-based fraud scoring.** The rules are readable and easy to test. A real model would replace `score_payment`, and the event contract would stay the same. The velocity rule depends on event timing, so the synthetic data's compressed timeline makes it fire more often than real traffic would.

**Simulated SMS gateway.** Failures are deterministic per event, about 4% transient and 2% permanent, so runs are repeatable. It does not test a real provider's behaviour.

**Dead-letter file, not a topic.** A dead-letter topic per group is the production pattern. For the POC, a local JSON Lines file was simpler, and no dead letters occurred in any run.

---

## 8. Issues encountered

| # | Issue | Cause | Resolution |
|---|---|---|---|
| 1 | Broker `kafka-2` restarted 36 times, last reason `OOMKilled` | The 1Gi container limit was below the JVM's default 1 GB heap plus page cache | Raised the limit to 1536Mi, capped the heap at 512 MB, raised the CPU limit to 1, and set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. Restarts stopped. |
| 2 | Role templates (`payment.md`, `fraud.md`, `notification.md`) had invalid commands | Client settings were passed as `--property` flags that the console tools do not accept, and `retention.ms= <value>` broke the shell | Replaced with scripted topic creation and Python clients. Templates left unchanged. |
| 3 | Existing `digitalpayments.payment.*` topics had RF 1 on one and a 1,024-byte message cap | Leftover experiments | Deleted with approval, so the new topics would start clean. |
| 4 | A float amount passed validation | jsonschema accepts `250.0` as an integer | `encode` now rejects any float anywhere in an event. A unit test covers it. |
| 5 | A reason code of 18 characters failed validation | The schema limit of 16 was too short | Raised the limit to 32. |
| 6 | Virtual environment is Python 3.9, and the code used 3.10 syntax | The venv was created before I checked for newer Python | Adjusted the code to 3.9. Python 3.13 is available, but I was not able to rebuild the venv (see item 8). |
| 7 | Producer output was written outside the project | `PROJECT_ROOT` used `parents[3]`, one level too high | Fixed to `parents[2]`, and moved the misplaced files into the project. |
| 8 | Shell commands containing `rm -rf` were denied twice | Harness policy on deletion | Worked around without deleting anything. Output was moved aside instead. |
| 9 | The burst summary lacked timing fields | `_finish` wrote the summary before the timing fields were added | Fixed. The one burst run that predates the fix has no timing, and the throughput figure uses a later run. |
| 10 | **Duplicate event IDs across runs.** 20,000 events were duplicates of the first burst | Two runs used seed 7, and IDs were drawn from the seeded random source | Caught by the consumers' dedupe counts. Fixed IDs to a per-run namespace, reset the three POC topics with approval, and reran Phases 4 and 5. Reconciliation by ID now checks this. |
| 11 | Latency reported in seconds | Group-join time was counted as latency, because the producer started before the consumers were stable | Wait for the groups to be `STABLE` before producing. |
| 12 | Latency script failed intermittently with "broker handle destroyed" | A transient metadata error | Added retries to the watermark query. One attempt still failed before producing. Its error went to a suppressed stream, so the cause was not captured. |
| 13 | Fraud p99 was about 1 second | `consume(200, timeout=1.0)` waits for a full batch or the timeout, so each message waited up to a second at low load. My first hypothesis, per-batch commits, was disproven: they cost about 20 ms | Reduced the poll timeout to 10 ms. |
| 14 | Terminal output full of IPv6 "connection refused" lines | librdkafka tries `::1` before `127.0.0.1`, and the brokers advertise `localhost` | Harmless. The client falls back to IPv4 and connects. |
| 15 | Final ordering check showed one partial journey | The 20,000-event burst stops at an event count, not a journey boundary | The check accepts a valid prefix, and the count is reported. |
| 16 | Fraud p99 was 200 to 250 ms, with one ~270 ms stall in the output flush | Each new broker connection first tried IPv6 `::1`, which is refused, and then waited out a reconnect backoff before trying IPv4. The brokers advertise `localhost`. | Set `broker.address.family=v4` on all clients. Fraud p99 fell to 25 to 37 ms across five runs (section 6.5). |
| 17 | A single 536 ms event in one run | The first event sent by a fresh idempotent producer waited about 450 ms before its producer-ID request was sent. The broker answered the request in 2 ms. Steady-state events were not affected. | Explained (section 6.5.3). The warm-up is built into the producer, and it reduced the first-event median from 118 ms to 12 ms in 20 starts. |
| 18 | The seed-42 producer log was overwritten by a 5-journey test run | The journey command wrote to a fixed output folder, so a later run overwrote the earlier run's delivery log. The events were still on the broker. | Rebuilt the log from the broker and the deterministic generator, labelled as reconstructed in `output/journeys-reconstructed/`. Every run now writes to a timestamped folder. |
| 19 | The payment producer's 64 KB batches exceeded the 16 KB topic limit | The payment producer's batch size was larger than the topic's message limit, so batches under load could be rejected with `MSG_SIZE_TOO_LARGE`. The throughput test exposed it. | Resolved. The topic limit is now 1 MB, and the producer's batches are 256 KB. A test enforces the batch size (section 6.6). |

---

## 9. Conclusions and open items

### 9.1 What the POC shows

- Keying by customer, with `acks=all`, idempotent producers, and replication factor 3, gives ordered, complete, loss-free delivery. This holds through a broker outage.
- Each consumer group can read the same stream independently, with its own offsets and its own handling.
- Dedupe on `eventId` is essential. A seeded ID generator produced duplicates that the consumers caught, which shows that the dedupe path works, and it was a bug in the data generator.
- Latency needs a method that matches the claim. Measuring from generated `eventTime`, or from a cold consumer start, gave seconds where the real figure is tens of milliseconds.

### 9.2 What I'd do differently

- **Partition count.** Set 100 partitions for production from day one.
- **Test ID uniqueness across runs** at the start, not after the duplicates appeared.
- **Never hide stderr** in a verification script. The unexplained failure in section 6.5 would have been explained by its error output.
- **Read client library semantics first.** The `consume()` wait is in the library docs, and I found it only after measuring.
- **Use a real schema registry** and Avro or Protobuf, so compatibility is enforced rather than documented.
- **Measure throughput** with the consumers in parallel and the producer at several rates, not as one burst.

### 9.3 Open items

| Item | Status | Next step |
|---|---|---|
| Fraud p99 latency | Met at p99: 25 to 37 ms across five standard runs. Between 99.3% and 99.7% of scores are within 50 ms. One run had a single 536 ms outlier, the first event sent by a new producer process (section 6.5.3). | Keep measuring in production. The warm-up is built into the producer (section 6.5.3). |
| Two-broker failure | Not tested | Run in a disposable cluster, because it would stop the shared quorum |
| 7-year retention at production volume | Configured, not exercised | Tiered storage or an archive sink with immutability |
| Full-pipeline throughput at 1000 Mb/s | The producer exceeds the target in bursts. The best durable consumer runs reached 257,909 (notification), 212,585 (fraud) and 274,826 (audit, one run), against 274,000 needed per group (section 9.5). | Accepted as a gap for this POC. For production, size partitions and instances for the target, then measure sustained runs |
| Dead-letter topic | File-based in the POC | Per-group DLQ topics |
| Real SMS and fraud integrations | Simulated | Integrate providers and test their failure modes |
| Security | PLAINTEXT listeners and no authentication | TLS, SASL, and ACLs per topic and group |
| Role template files | Unchanged, superseded by the scripts | Update or remove, as the team prefers |

---

### 9.4 Java port and final test

The whole project was ported to Java: the producer, the shared code, the three consumers, the verification and measurement tools, and the unit tests. The Python implementation has since been removed, so the Java project is the only implementation. Code is in [java/src/main/java/poc/](java/src/main/java/poc/) and tests are in [java/src/test/java/poc/](java/src/test/java/poc/).

**Deviations forced by the artifact mirror.** The mirror blocks the JSON Schema validator, the SQLite driver, H2, and the Maven build plugins. So:

- Event checks are hand-written against the same rules as the schemas in `schemas/`. They are not a schema validator.
- Consumer state is kept in append-only files, forced to disk at each batch, instead of SQLite.
- The project is built with `javac`. Tests run through a small JUnit launcher, because the Maven test plugin is blocked.
- The random sequence differs from Python's, so the same seed no longer gives the same data.

**Final test on a clean topic.** The run used a new lifecycle topic, `digitalpayments.payment.lifecycle.java-clean.v1`, and fresh consumer groups and output topics, so no Python-era state was reused.

| Check | Result |
|---|---|
| Unit tests | 49 of 49 pass |
| Producer, 1,000 journeys and a 50,000-event burst | 3,770 and 50,000 events delivered, 0 failed |
| Broker failure drill, `kafka-1` deleted during writes | 3,048 events delivered, 0 failed, 0 retries; under-replicated partitions rose to 197 and returned to 0 |
| Consumer lag, all three groups | 0 on all 36 partitions |
| Reconciliation by event id | Passes on 61,654 events at every stage, with no missing, unexpected, or duplicate records |
| Ordering | Passes on 16,262 payments (16,261 complete, 1 partial from the burst) |
| Acknowledgement latency | Median 0.94 ms, p99 6.96 ms, maximum 20.5 ms; 100% within 150 ms |
| End-to-end latency, live consumers | Fraud: median 20 ms, p99 46 ms, 99.67% within 50 ms. Notification: median 14 ms, p99 28 ms, 100% within 2 s |
| Payment warm-up, 20 fresh JVMs each | Without warm-up: median 118 ms, maximum 151 ms. With warm-up: median 12 ms, maximum 13.7 ms |
| Producer throughput | 730,685 events/s with one thread (2,669 Mb/s); 829,232 events/s with four (3,029 Mb/s); 0 failures |

Two findings from the port:

- **Partitioning.** The earlier Java producer used Kafka's default partitioner, which places customers differently from the Python producer's CRC-32 rule. That split 186 customers across partitions on the shared topic. The Java producer now uses a CRC-32 partitioner, which reproduces the Python-era placement exactly. The shared topic still contains the earlier batch, so the clean topic was used for the final test.
- **Fraud latency median.** The Java median is about 20 ms, against 11 to 13 ms for the Python consumers. I have not determined the cause. The Java consumer's per-batch forced writes are one candidate.

### 9.5 Final throughput tuning

The consumers were tuned after the Java port. Every run read the same 1,500,000-event backlog with fsync on, so the figures are durable. Runs used the 12-partition backlog unless stated otherwise.

| Change | Notification | Fraud | Audit |
|---|---|---|---|
| Java port, batch 200 (section 6.6) | 84,890 (12 instances) | 42,791 (12) | 46,890 (12) |
| Batch 200, re-measured on the refilled backlog | 82,722 | 46,945 | 48,194 |
| No fsync, batch 200 (diagnostic only, not durable) | 167,992 | 153,171 | 296,619 |
| Batch 1,000, skip empty fsyncs | 233,136 | 179,835 | 203,611 |
| Batch 2,000 | 203,860 | 196,980 | 257,202 |
| Batch 2,000, no fsync (ceiling, not durable) | 243,112 | 200,000 | 334,150 |
| Batch 2,000, 24 partitions, 12 instances | 257,909 | 212,585 | not run |
| Batch 2,000, 24 partitions, 16 instances | 217,897 | 175,090 | 274,826 |
| Batch 2,000, 24 partitions, 24 instances | 161,238 | 131,764 | 237,793 |

The target is about 274,000 events per second for each group. The no-fsync rows show that removing fsync is not enough on its own. Fraud and notification stay below the target even with no fsync. The 24-partition runs show that more instances help only up to a point. Audit reached the target in one run at 16 instances. That result was not repeated, and run-to-run variation has been about 10%, so it is not a confirmed pass.

**Outcome.** The aggregate 1000 Mb/s target for the consumers is not met on this machine. Going further would need more hardware, which was out of scope for this POC.

**Test topics.** `digitalpayments.consumertest2.v1` holds the 1.5 million-event backlog, with one-day retention. `digitalpayments.consumertest24.v1` is the 24-partition topic, also with one-day retention. Both are test topics and can be deleted.

---

## 10. Evidence index

All paths are relative to the project root.

| File | Contents |
|---|---|
| [evidence/phase2_create_topics.txt](evidence/phase2_create_topics.txt) | Topic creation output and describe output |
| [evidence/phase2_topic_configs.txt](evidence/phase2_topic_configs.txt) | Effective configuration of each topic |
| [evidence/phase7_describe_digitalpayments.payment.lifecycle.v1.txt](evidence/phase7_describe_digitalpayments.payment.lifecycle.v1.txt) | Describe output for the lifecycle topic |
| [evidence/phase4_journeys_summary.json](evidence/phase4_journeys_summary.json), [evidence/phase4_burst_summary.json](evidence/phase4_burst_summary.json) | Producer run summaries |
| [evidence/phase4_end_offsets.txt](evidence/phase4_end_offsets.txt) | Broker end offsets |
| [evidence/phase4_sample_messages.txt](evidence/phase4_sample_messages.txt) | Sample messages with partition and offset |
| [evidence/phase5_consumer_group_lag.txt](evidence/phase5_consumer_group_lag.txt) | Consumer group lag after the first consumer run |
| [evidence/phase5_fraud_scoring_summary.json](evidence/phase5_fraud_scoring_summary.json), [evidence/phase5_notification_sender_summary.json](evidence/phase5_notification_sender_summary.json), [evidence/phase5_audit_archiver_summary.json](evidence/phase5_audit_archiver_summary.json) | Consumer group run summaries |
| [evidence/phase6_ordering_report.json](evidence/phase6_ordering_report.json) | Ordering check |
| [evidence/phase6_reconciliation.json](evidence/phase6_reconciliation.json) | Reconciliation by event ID |
| [evidence/phase6_failure_drill.log](evidence/phase6_failure_drill.log) | Broker failure drill |
| [evidence/phase6_final_consumer_lag.txt](evidence/phase6_final_consumer_lag.txt) | Final lag per group |
| [evidence/phase6_latency_report.json](evidence/phase6_latency_report.json) | Latest latency run |
| [evidence/tuning_baseline_latency.json](evidence/tuning_baseline_latency.json), [evidence/tuning_after_latency.json](evidence/tuning_after_latency.json) | Latency before and after the poll-timeout change |
| [evidence/ack_latency_report.json](evidence/ack_latency_report.json) | Producer acknowledgement latency |
| [evidence/profile_fraud_before_ipv4.json](evidence/profile_fraud_before_ipv4.json) | Fraud latency breakdown before the IPv4 fix |
| [evidence/profile_fraud_after_ipv4_run1.json](evidence/profile_fraud_after_ipv4_run1.json), [evidence/profile_fraud_report.json](evidence/profile_fraud_report.json) | Fraud latency breakdown after the IPv4 fix |
| [evidence/profile_fraud_after_ipv4_raw.json](evidence/profile_fraud_after_ipv4_raw.json) | Raw per-message and per-batch timings from the last profile run |
| [evidence/fraud_ipv4_latency.json](evidence/fraud_ipv4_latency.json) | Standard fraud and notification latency run after the IPv4 fix (run 1) |
| [evidence/fraud_repeat_run1.json](evidence/fraud_repeat_run1.json) to [evidence/fraud_repeat_run4.json](evidence/fraud_repeat_run4.json) | Repeat standard latency runs (runs 2 to 5) |
| [evidence/first_send_probe.json](evidence/first_send_probe.json) | First-send timings for a fresh producer, with and without warm-up and debug logging |
| [evidence/warm_up_test.json](evidence/warm_up_test.json) | Warm-up test using a journey as the warm-up: first payment event from 20 cold and 20 warmed producer starts |
| [evidence/consumer_throughput/](evidence/consumer_throughput/) | Consumer throughput by group, at 1, 4 and 12 instances, and with all three groups running concurrently |
| [evidence/max_throughput/](evidence/max_throughput/) | Throughput sweep: one report per run (settings, events per second, payload Mb/s) |
| [evidence/warm_up_built_test.json](evidence/warm_up_built_test.json) | Built warm-up test: first payment event from 20 cold and 20 built-warm-up producer starts |
| [evidence/superseded_duplicate_ids/](evidence/superseded_duplicate_ids/) | Evidence from the run with duplicate IDs, kept for the record |

**Code.** Schemas in [schemas/](schemas/). Java code in [java/src/main/java/poc/](java/src/main/java/poc/), with the producer, shared code, consumers, verification and measurement tools. Tests in [java/src/test/java/poc/](java/src/test/java/poc/). Topic and failure-drill scripts in [scripts/](scripts/).
