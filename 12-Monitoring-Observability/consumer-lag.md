# Consumer Lag — Measurement, Causes, and Alerting

## What Consumer Lag Is

Consumer lag is the measure of how far behind a consumer group is from the head of a partition. It is the primary signal for whether a Kafka consumer is keeping pace with production.

**Standard lag (at-least-once consumers):**
```
lag = Log End Offset (LEO) - Consumer Committed Offset
```

The LEO is the next offset the producer will write to. The committed offset is the last offset the consumer group has confirmed processing. The difference is the number of records the consumer has not yet processed.

**Transactional lag (`read_committed` consumers):**
```
lag = Last Stable Offset (LSO) - Consumer Committed Offset
```

For consumers configured with `isolation.level=read_committed`, the effective head of the partition is not the LEO but the LSO — the offset up to which all transactions are either committed or aborted. Measuring lag against the LEO for a `read_committed` consumer overstates the real lag, because records beyond the LSO are invisible to the consumer until their transactions resolve. Use LSO-based lag calculations for transactional pipelines. See `07-Advanced-Reliability/exactly-once-semantics.md`.

Lag is per-partition, not per-topic. A consumer group with 16 partitions assigned and zero lag on 15 but 500,000 records of lag on one partition is not a healthy group — that single partition is a bottleneck. Monitor at partition granularity, not topic-level aggregates.

## What Causes Lag to Accumulate

**Poison-pill records and `max.poll.interval.ms` eviction:**
A record that triggers a long-running processing path (expensive enrichment call, database deadlock, infinite retry loop) can cause the consumer to exceed `max.poll.interval.ms` between `poll()` calls. When this happens, the broker considers the consumer dead and evicts it from the group, triggering a rebalance. The partition is reassigned to another consumer, which will encounter the same record and repeat the failure. Lag accumulates on the affected partition while the consumer group cycles through eviction and rebalance.

Detection: lag growing on specific partitions while other partitions in the same group remain at zero. Resolution: route the record to a DLQ and continue, or instrument processing time per record to identify the slow path.

**Stateful recovery (RocksDB changelog rebuild):**
Kafka Streams and stateful Flink jobs maintain local state backed by changelog topics. When a consumer restarts after losing local storage (host failure, ephemeral container, volume replacement), it must rebuild state by replaying the changelog before it can resume processing the main topic. For large state stores — 300M+ keys, 10–50 GB compressed — this rebuild takes 45 minutes to 2 hours. During rebuild, lag on the main topic grows continuously.

Detection: lag accumulating immediately after a consumer restart, combined with the consumer's offset position not advancing on the main topic. Resolution: RocksDB S3 pre-seeding to reduce rebuild time. See `10-Operational-Patterns/rocksdb-s3-preseeding.md`.

**Rebalance storms:**
Eager rebalancing (the default before `CooperativeStickyAssignor`) revokes all partition assignments and halts processing across the entire consumer group during each rebalance. In unstable deployments — rolling restarts without static membership, consumer instances with inconsistent health-check timeouts — rebalances can trigger faster than the group achieves a stable state. Processing halts accumulate as lag.

Detection: `kafka.consumer:type=consumer-coordinator-metrics,name=rebalance-rate-and-time` increasing; lag growing in bursts that correlate with deployment or restart events. Resolution: `CooperativeStickyAssignor` + `group.instance.id` for StatefulSet deployments. See `04-Data-Consumption/cooperative-rebalancing.md` and `04-Data-Consumption/static-membership.md`.

**Throughput imbalance:**
Production rate exceeds consumption rate. This is the steady-state growth case — not an event-triggered spike but a chronic condition where the consumer simply cannot keep up. Causes include under-provisioned consumer instances, a topic with more partitions than consumers (parallelism ceiling hit), or a downstream dependency (database, API) that has become a bottleneck.

Detection: lag growing at a steady rate over hours or days. Resolution: scale consumer instances up to the partition count ceiling, or address the downstream bottleneck.

## Alerting Strategy

Effective lag alerting distinguishes between transient spikes (rebalances, restarts) and structural problems (chronic throughput imbalance, stuck consumers).

**Alert 1 — Lag growth rate (primary):**
Alert when lag has been growing continuously for N minutes rather than on an absolute threshold. An absolute threshold of 100,000 records fires on every deployment; a growth-rate alert of "lag has increased for 10 consecutive minutes" fires only when the consumer is falling behind for a sustained period.

```yaml
# Prometheus alerting rule example
alert: KafkaConsumerLagGrowing
expr: |
  increase(kafka_consumer_group_lag[10m]) > 0
  and
  kafka_consumer_group_lag > 1000
for: 10m
labels:
  severity: warning
```

**Alert 2 — Consumption rate drop:**
Alert when `records-consumed-rate` drops below a baseline for a consumer group that should be continuously processing. This catches consumer thread blockage — the consumer is connected and heartbeating but not calling `poll()` or not completing `poll()` calls fast enough.

**Alert 3 — Zero consumption on active topic:**
Alert when a consumer group has committed offsets (i.e., has been active) but `records-consumed-rate` has been zero for N minutes while the topic's LEO is advancing. This catches crashed consumers that have not yet triggered the broker's session timeout.

**Alert 4 — Partition-level lag outlier:**
Alert when a single partition's lag is more than 10x the median lag across all partitions in the group. This catches poison-pill stuck consumers while other partitions in the group appear healthy at the aggregate level.

## Tooling

**Kafka Lag Exporter** (open source, Lightbend): scrapes consumer group offsets and LEOs from the broker, exposes partition-level lag as Prometheus metrics. Recommended for Confluent Platform deployments. Grafana dashboards are available from the community.

**Confluent Cloud Metrics API:** exposes consumer group lag as a first-class metric without requiring direct broker access. See `12-Monitoring-Observability/confluent-cloud-metrics-api.md`.

**`kafka-consumer-groups.sh`:** built-in CLI for point-in-time lag inspection:
```bash
kafka-consumer-groups.sh \
  --bootstrap-server broker:9092 \
  --describe \
  --group my-consumer-group
```

Output includes `LOG-END-OFFSET`, `CURRENT-OFFSET`, and `LAG` per partition. Use for ad-hoc debugging; not suitable for continuous monitoring.

**`kafka.consumer` JMX MBeans:** the consumer client exposes `records-lag-max` and `records-consumed-rate` per assigned partition via JMX. Useful for fine-grained consumer-side instrumentation when Kafka Lag Exporter is not available.
