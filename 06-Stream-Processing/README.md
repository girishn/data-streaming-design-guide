# Module 06 — Stream Processing

Framework selection, state durability, and windowing semantics for production stream processing workloads.

For a layered decision path covering stateless vs stateful, framework selection, state size, time semantics, and fault tolerance — see the root-level [Stream Processing Framework](../stream-processing-framework.md).

## Files

- [kafka-streams-vs-flink.md](kafka-streams-vs-flink.md) — Architectural differences, decision criteria, state rebuild comparison, and exactly-once semantics in each framework.
- [ksqldb.md](ksqldb.md) — SQL-based streaming on Kafka: persistent, push, and pull queries, decision framework vs Kafka Streams and Flink, schema strategy constraint, and operational monitoring.
- [state-management.md](state-management.md) — RocksDB tuning in Kafka Streams, Flink state backends, checkpoint configuration, and the DR rebuild problem.
- [windowing.md](windowing.md) — Tumbling, hopping, sliding, and session window definitions, event-time vs processing-time semantics, watermarks, and late event handling.
