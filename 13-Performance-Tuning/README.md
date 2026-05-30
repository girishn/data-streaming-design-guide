# Module 13 — Performance Tuning

Configuration guidance for producer batching, consumer fetch behavior, and broker thread pools — with concrete tradeoff analysis for throughput vs latency decisions.

## Files

- [producer-tuning.md](producer-tuning.md) — Batching (`linger.ms`, `batch.size`), compression codec selection, `acks` durability tradeoffs, and TLS throughput impact.
- [consumer-tuning.md](consumer-tuning.md) — Fetch tuning (`fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`), `max.poll.records` and its relationship to `max.poll.interval.ms`, and memory implications.
- [broker-tuning.md](broker-tuning.md) — Thread pool sizing (`num.network.threads`, `num.io.threads`, `num.replica.fetchers`), log flush configuration, and why durability through ISR replication is preferred over explicit fsync.
- [quota-management.md](quota-management.md) — Producer and consumer byte-rate quotas, request-rate quotas, quota scoping by user principal and client-id, throttling mechanics, reassignment throttles, and monitoring.
- [end-to-end-latency.md](end-to-end-latency.md) — Per-layer latency budgets (ingestion, broker, processing, consumption, Schema Registry) and a typical total budget table across common deployment scenarios.
