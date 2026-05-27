# Module 11 — Monitoring and Observability

Metrics, alerting strategies, and tooling for Kafka consumer health, broker infrastructure, and Confluent Cloud managed services.

## Files

- [consumer-lag.md](consumer-lag.md) — How consumer lag is measured (LEO vs committed offset, LSO for transactional consumers), what causes it, and how to alert on it effectively.
- [broker-jmx-metrics.md](broker-jmx-metrics.md) — Key JMX MBeans for production broker health: under-replicated partitions, controller count, thread pool saturation, ISR shrink rate, and log flush timing.
- [confluent-cloud-metrics-api.md](confluent-cloud-metrics-api.md) — REST-based metrics for Confluent Cloud clusters, connectors, and Flink; authentication, query patterns, and how it differs from JMX for self-managed clusters.
