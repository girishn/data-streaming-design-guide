# Quota Management — Multi-Tenant Cluster Isolation

## What Quotas Do

Kafka quotas prevent a single producer, consumer, or client application from monopolising broker resources. Without quotas, a misbehaving client — a runaway consumer replaying 7 days of data, a batch producer flushing at full network speed — can saturate broker network threads or I/O threads and degrade performance for all other clients on the cluster.

Quotas are enforced per-client at the broker. When a client exceeds its quota, the broker throttles it by muting the network channel for a calculated delay period. The broker reports the throttle time back to the client in its response. The client library (`kafka-clients`) backs off accordingly — the throttle is transparent to application code.

## Two Quota Dimensions

**Byte-rate quotas** cap network bandwidth. They apply independently to produce and fetch operations:

- `producer_byte_rate` — maximum bytes/second a producer (identified by user + client-id) can write across all topics on the broker
- `consumer_byte_rate` — maximum bytes/second a consumer can fetch

**Request-rate quotas** cap CPU. They measure the fraction of broker request handler thread time consumed by a client:

- `request_percentage` — percentage of I/O thread time (across all I/O threads on the broker) a client can consume

Request-rate quotas address a different failure mode than byte-rate quotas. A client sending many small requests (high-frequency metadata refreshes, frequent offset commits, schema ID lookups forwarded through the broker) can saturate request handler threads without moving significant data volume. See `13-Performance-Tuning/broker-tuning.md` for thread pool configuration.

## Quota Scoping and Precedence

Quotas are matched against the client's **user principal** and **client.id**. The broker applies the most specific quota that matches:

```
(user, client-id)   ← most specific
(user, *)
(*, client-id)
(*, *)              ← default quota, applies to all unmatched clients
```

Setting a default quota `(*, *)` establishes a cluster-wide floor. Specific quotas for known high-volume clients override the default upward or downward.

**Setting quotas via kafka-configs:**
```bash
# Byte-rate quota for a specific producer service account
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter --add-config 'producer_byte_rate=52428800' \
  --entity-type users --entity-name "vitals-ingestor" \
  --entity-type clients --entity-name "vitals-producer-v2"

# Consumer fetch quota for a historical replay client
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter --add-config 'consumer_byte_rate=26214400' \
  --entity-type users --entity-name "audit-replay"

# Default quota applied to all unmatched clients
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter --add-config 'producer_byte_rate=10485760,consumer_byte_rate=10485760' \
  --entity-type users --entity-name "<default>"
```

Quotas are stored in the cluster metadata (KRaft log or ZooKeeper) and take effect immediately without a broker restart.

## Throttling Mechanics

When a client exceeds its quota within a measurement window, the broker calculates a throttle delay:

```
throttle_delay_ms = (quota_violation_ratio × window_size_ms)
```

The broker **mutes the client's network channel** for `throttle_delay_ms`. During this period:
- The broker does not read new requests from the client's socket
- Inflight requests already queued are still processed
- The throttle time is included in the next response so the client can log it

**Key configuration parameters — the measurement window itself:**
```properties
# Broker config — quota measurement window
quota.window.num=11           # number of retained samples (default)
quota.window.size.seconds=1   # length of each sample, in seconds (default)
```

`window_size_ms` in the formula above is `quota.window.num × quota.window.size.seconds`, converted to milliseconds — an 11-second rolling window by default. Fewer/shorter samples make the broker react to bursts faster but tolerate less burstiness before throttling; more/longer samples smooth out short bursts at the cost of slower reaction to sustained overuse. There is no separate broker-side cap on an individual throttle delay's duration — the delay is whatever the window math computes to bring the client's rolling average back under quota, so window size is the lever for bounding worst-case throttle duration, not a standalone timeout config. Tune `request.timeout.ms` on the client relative to how long the widest quota window on the cluster can throttle for, so a legitimate throttle doesn't get misread as a broker outage.

## Inter-Broker Replication Exemption

Broker-wide quotas do not apply to inter-broker replication traffic. Follower fetch requests from replica fetcher threads are exempt — a replication-level quota would allow ISR replicas to fall behind during a busy period, which is more damaging than any individual client throttle.

The exception to this exemption is **replica reassignment**. When partitions are being moved between brokers (`kafka-reassign-partitions.sh`), the replication traffic generated is subject to a separate throttle:

```bash
# Throttle partition reassignment replication to 50 MB/s
kafka-configs.sh --bootstrap-server broker:9092 \
  --alter --add-config 'leader.replication.throttled.rate=52428800,
                         follower.replication.throttled.rate=52428800' \
  --entity-type brokers --entity-name 1
```

Without a reassignment throttle, moving a large partition set can saturate inter-broker network bandwidth and push replicas out of the ISR on unrelated partitions. Always set a reassignment throttle before running large partition moves in production.

## Monitoring Throttling

A client being throttled is not inherently an error — it means the quota is working. The concern is sustained throttling of latency-sensitive clients, or throttling that was not expected.

**JMX metrics (self-managed Confluent Platform):**
```
kafka.server:type=Produce,user=vitals-ingestor,client-id=vitals-producer-v2,name=produce-throttle-time-avg
kafka.server:type=Fetch,user=audit-replay,name=fetch-throttle-time-avg
kafka.server:type=Request,user=vitals-ingestor,name=request-throttle-time-avg
```

Alert when `produce-throttle-time-avg` or `fetch-throttle-time-avg` is non-zero for latency-sensitive clients. For batch or replay clients, sustained throttling is expected and acceptable.

**Confluent Cloud Metrics API:**
The `io.confluent.kafka.server/request_byte_rate` and `io.confluent.kafka.server/response_byte_rate` metrics expose per-principal bandwidth. Cross-reference with quotas to detect clients approaching their limits before throttling begins. See `11-Monitoring-Observability/confluent-cloud-metrics-api.md`.

## When to Set Quotas

| Scenario | Recommended quota type |
|---|---|
| Multi-tenant cluster shared by multiple teams | Default `(*, *)` byte-rate quota as a floor; per-team overrides for known high-volume services |
| Historical replay / backfill jobs | Consumer byte-rate quota to protect real-time consumers from replay saturation |
| Partition reassignment in production hours | Reassignment throttle on all brokers involved |
| High-frequency metadata clients (schema validation, admin tools) | Request-rate quota |
| Single-tenant cluster, all clients are trusted services | Quotas optional — add if a runaway client has caused an incident |

Quotas are a second-line defence. The first line is correct producer and consumer configuration (`linger.ms`, `batch.size`, `fetch.min.bytes`) that prevents clients from generating unnecessary request load in the first place. See `13-Performance-Tuning/producer-tuning.md` and `13-Performance-Tuning/consumer-tuning.md`.
