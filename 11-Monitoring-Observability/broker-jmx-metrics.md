# Broker JMX Metrics — Production Health Monitoring

## JMX in Confluent Platform

Confluent Platform brokers expose operational metrics via Java Management Extensions (JMX). Each broker exposes its own MBeans — there is no aggregated cluster-level JMX view. Monitoring stacks (Prometheus JMX Exporter, Datadog, Grafana) scrape each broker independently and aggregate at the dashboard layer.

JMX is the primary observability mechanism for self-managed Confluent Platform deployments. For Confluent Cloud, use the Metrics API instead — see `11-Monitoring-Observability/confluent-cloud-metrics-api.md`.

Enable JMX on brokers by setting `KAFKA_JMX_PORT` or `-Dcom.sun.management.jmxremote.port` in the broker startup environment. In Confluent Platform on Kubernetes (CFK), JMX is exposed via a sidecar port by default.

## Critical Metrics — Replication Health

**`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`**

Target: **0 at all times.**

Under-replicated partitions are partitions where one or more replicas have fallen out of the ISR. This means the partition is operating with fewer replicas than configured, reducing fault tolerance. If the leader fails while partitions are under-replicated, data loss is possible depending on `unclean.leader.election.enable` settings.

Common causes:
- Follower broker disk I/O saturated — cannot keep pace with leader's write rate
- Network bandwidth exhausted between brokers — follower fetch requests are delayed
- Follower broker GC pause — extended stop-the-world pauses prevent timely fetches
- Broker failure — all partitions led by the failed broker become under-replicated on remaining replicas

A sustained non-zero value is a P1 alert. A transient spike during a rolling restart is expected — partitions become temporarily under-replicated as a broker restarts and its replicas catch up.

**`kafka.controller:type=KafkaController,name=ActiveControllerCount`**

Target: **exactly 1 across the cluster.**

The active controller manages partition leader elections and broker membership. In a healthy cluster, exactly one broker holds the active controller role. Zero means no controller exists — the cluster cannot perform leader elections and is effectively read-only or degraded. More than one means a split-brain condition.

In KRaft mode, this metric verifies the health of the Raft quorum's elected leader. Monitor alongside the KRaft-specific `kafka.controller:type=RaftManager` MBeans for quorum health.

**`kafka.server:type=ReplicaManager,name=IsrShrinksPerSec`**
**`kafka.server:type=ReplicaManager,name=IsrExpandsPerSec`**

ISR shrinks indicate replicas falling out of sync; ISR expands indicate replicas catching up. A healthy cluster at steady state has both rates near zero. Sustained ISR shrinks without subsequent expands indicate a replica that cannot recover — investigate the follower broker.

Correlate with `UnderReplicatedPartitions`: if shrinks are occurring but `UnderReplicatedPartitions` remains zero, the follower is recovering quickly. If `UnderReplicatedPartitions` is rising, the follower is not catching up.

## Critical Metrics — Broker Thread Pool Saturation

**`kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent`**

Target: **above 30% idle.** Below 20% is warning; below 10% is critical.

The request handler thread pool processes all client requests: produce, fetch, metadata, offset commits. When this pool is saturated, request latency increases across all operations — not just the operation that is causing saturation.

Common saturation causes:
- High TLS encryption/decryption load: each produce and fetch request requires crypto operations. At scale, this can consume a significant fraction of handler threads. Mitigation: hardware AES-NI acceleration, or move TLS termination to a load balancer for listener-specific traffic.
- Broker-side schema ID validation: the broker must decompress and inspect each record batch to validate schema IDs, consuming handler thread time proportional to message volume. See `08-Stream-Governance/broker-side-validation.md`.
- Compaction-heavy topics: log compaction running concurrently with high produce throughput competes for I/O and CPU.

**`kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent`**

Target: **above 30% idle.**

The network processor threads handle socket I/O — reading bytes from client connections and writing responses. Saturation here indicates the broker cannot read client data fast enough, causing clients to experience slow response times and potential connection timeouts. Often caused by a very large number of active client connections or very high per-connection throughput.

## Critical Metrics — Storage and I/O

**`kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs`**

This metric tracks how frequently the broker flushes memory-mapped log segments to disk and how long each flush takes. Kafka relies on the OS page cache for buffering; explicit flushes (`log.flush.interval.messages`, `log.flush.interval.ms`) are expensive.

High flush rate or high flush duration indicates disk I/O saturation. The consequence: the High Watermark (HW) advancement is delayed because followers cannot fetch data that has not been flushed. This delays the `committed` threshold for consumers, increasing visible lag even when production is proceeding normally.

In most production deployments, `log.flush.interval.messages` and `log.flush.interval.ms` are left at defaults (rely on OS page cache, no explicit flushing). If explicit flushing is configured, this metric becomes critical.

**`kafka.log:type=Log,name=LogEndOffset`** (per topic-partition)

Not an alert metric, but useful for debugging throughput distribution. If certain partitions have LEOs that are orders of magnitude higher than others, the partitioning key distribution is skewed.

## Alerting Reference

| Metric | Target | Warning | Critical |
|---|---|---|---|
| `UnderReplicatedPartitions` | 0 | > 0 for > 60s | > 0 for > 5 min |
| `ActiveControllerCount` | 1 | ≠ 1 for any duration | — (immediate P1) |
| `IsrShrinksPerSec` | 0 | > 0 sustained | Correlate with UnderReplicated |
| `RequestHandlerAvgIdlePercent` | > 30% | < 30% | < 10% |
| `NetworkProcessorAvgIdlePercent` | > 30% | < 30% | < 10% |
| `LogFlushRateAndTimeMs` (p99) | < 1s | > 1s | > 5s |

## Scraping JMX with Prometheus

The Prometheus JMX Exporter runs as a Java agent on each broker and converts MBeans to Prometheus metrics:

```yaml
# jmx_exporter_config.yaml — key rules for Kafka broker
rules:
  - pattern: 'kafka.server<type=ReplicaManager,name=UnderReplicatedPartitions><>Value'
    name: kafka_server_replicamanager_underreplicatedpartitions
    type: GAUGE

  - pattern: 'kafka.controller<type=KafkaController,name=ActiveControllerCount><>Value'
    name: kafka_controller_kafkacontroller_activecontrollercount
    type: GAUGE

  - pattern: 'kafka.server<type=ReplicaManager,name=IsrShrinksPerSec><>Count'
    name: kafka_server_replicamanager_isrshrinks_total
    type: COUNTER

  - pattern: 'kafka.server<type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent><>Value'
    name: kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent
    type: GAUGE
```

Confluent provides a reference JMX Exporter configuration for Kafka brokers in the Confluent Platform documentation. Use it as the starting point and add alert-specific rules on top.
