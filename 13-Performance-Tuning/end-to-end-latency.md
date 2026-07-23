# End-to-End Latency Budget — Layer-by-Layer Reference

Pipeline latency is the sum of contributions from each layer. Each layer has a different tunable floor — getting the full system to low latency requires addressing all of them, not just the producer.

---

## Ingestion

| Mechanism | Typical latency | Driver |
|---|---|---|
| SDK producer, `acks=1`, `linger.ms=0` | 1–5ms | Network round trip to leader only |
| SDK producer, `acks=all`, `linger.ms=5–20ms` | 6–25ms | Linger + ISR replication (adds 1–5ms under normal conditions) |
| SDK producer, throughput-optimised (`linger.ms=50–100ms`) | 50–105ms | Linger is the floor regardless of broker speed |
| Kafka Connect (managed) | 1s – 60s | Poll-interval driven; no sub-second guarantee by default |
| CDC via Debezium | <10ms from DB commit | Reads WAL sequentially, bypasses query planner entirely |

The `linger.ms` value sets a hard floor on producer-side latency. Under high load, batches fill before `linger.ms` expires and the cost is zero — but on low-to-moderate traffic topics, `linger.ms` is paid on every batch. See [producer-tuning.md](producer-tuning.md).

---

## Transport (Broker)

| Factor | Latency |
|---|---|
| Broker log append (page cache write) | <1ms |
| ISR replication acknowledgment (`acks=all`) | +1–5ms; p99 spikes under follower disk or GC pressure |
| TLS overhead | 20–40% throughput reduction (not latency per record, but reduces effective headroom) |

ISR replication latency is the most operationally volatile component — it is stable under normal conditions but degrades sharply when a follower is under pressure. Monitor `kafka.server:type=ReplicaFetcherManager,name=MaxLag` to catch this before it shows up in end-to-end measurements. See [broker-tuning.md](broker-tuning.md).

---

## Processing

| Path | Added latency | Notes |
|---|---|---|
| Stateless (SMT, filter, route) | <1ms | Inline with connector or consumer thread; no extra network hop |
| Kafka Streams, stateful (RocksDB lookup) | 1–10ms per record | Increases as working set exceeds page cache; cold reads hit disk |
| Flink, stateful | ms-level per record; checkpoint interval sets EOS delivery floor | Default checkpoint interval is 10s–5min — this dominates pipeline latency for EOS Flink jobs |

For Kafka Streams, commit interval (`commit.interval.ms`, default 30000ms — 100ms when `processing.guarantee` is set to an exactly-once mode) sets the minimum interval between offset commits and state store flushes — it is not per-record latency, but it determines how stale consumer lag metrics appear. See [kafka-streams-vs-flink.md](../06-Stream-Processing/kafka-streams-vs-flink.md).

---

## Consumption

| Configuration | Consumer-side latency |
|---|---|
| Default (`fetch.min.bytes=1`, `fetch.max.wait.ms=500ms`) | Responds immediately when data is available; up to 500ms on idle topics |
| Low-latency tuning (`fetch.max.wait.ms=50–100ms`) | 50–100ms ceiling on idle topics |
| High-throughput (`fetch.min.bytes=1MB+`) | Zero added latency on busy topics; full `fetch.max.wait.ms` on quiet ones |

The default `fetch.max.wait.ms=500ms` is the most commonly missed latency source in new deployments. A producer sending events every 200ms will still see 500ms consumer-side delay if the topic is not busy enough to satisfy `fetch.min.bytes` before the wait expires. See [consumer-tuning.md](consumer-tuning.md).

---

## Governance (Schema Registry)

| Operation | Latency |
|---|---|
| Schema lookup, first call (uncached) | 1–5ms network round trip |
| Schema lookup, cached (same schema ID) | <1ms — client-side in-memory map |
| Schema registration (new version) | 5–20ms — HTTP POST + server-side compatibility check |

The Avro/Protobuf serializer caches schema IDs after the first lookup. Per-message overhead after the first call per schema is negligible. Schema Registry latency only matters at producer startup and on schema version changes.

---

## Operations Layer (DLQ, Observability)

| Mechanism | Latency |
|---|---|
| DLQ routing via connector error handler | <1ms — same Connect task thread, no additional hop |
| Consumer lag alert (growth-rate based) | Detection lag of 1–5 min depending on metrics scrape interval |
| End-to-end heartbeat (synthetic event) | Reflects total pipeline latency from ingestion through consumption |

The end-to-end heartbeat is the only measurement that captures the full stack. Per-layer metrics (producer send time, consumer lag) are necessary for diagnosis but insufficient for SLA reporting — a lag spike looks different from a heartbeat delay spike, and both matter. See [consumer-lag.md](../11-Monitoring-Observability/consumer-lag.md).

---

## Typical Budget Summary

| Scenario | Ingestion | Broker | Processing | Consumption | Total |
|---|---|---|---|---|---|
| Low-latency (SDK, `acks=1`, stateless) | 1–5ms | <1ms | <1ms | immediate | ~2–7ms |
| Standard production (SDK, `acks=all`, stateless) | 6–25ms | 1–5ms | <1ms | immediate | ~7–31ms |
| Stateful (Kafka Streams) | 6–25ms | 1–5ms | 1–10ms | immediate | ~8–40ms |
| Connect-based ingestion (low poll interval) | 1–5s | <1ms | <1ms | immediate | ~1–5s |
| CDC (Debezium) + stateless | <10ms | 1–5ms | <1ms | immediate | ~11–16ms |

"Immediate" consumption assumes the topic is active enough that `fetch.min.bytes` is satisfied before `fetch.max.wait.ms` expires. On low-traffic topics, add up to `fetch.max.wait.ms` (default 500ms) to the consumption column.

---

## Cross-References

- Producer configuration detail — [producer-tuning.md](producer-tuning.md)
- Consumer fetch tuning — [consumer-tuning.md](consumer-tuning.md)
- Broker thread and ISR configuration — [broker-tuning.md](broker-tuning.md)
- Consumer lag monitoring — [consumer-lag.md](../11-Monitoring-Observability/consumer-lag.md)
- Kafka Streams vs Flink processing latency tradeoffs — [kafka-streams-vs-flink.md](../06-Stream-Processing/kafka-streams-vs-flink.md)
- Layer-by-layer design walkthrough — [streaming-design-approach.md](../streaming-design-approach.md)
