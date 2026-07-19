# Module 05 — Enterprise Connect

Kafka Connect in production: deployment model trade-offs, field-level transforms, and error handling patterns.

## Files

- [managed-connectors.md](managed-connectors.md) — Worker architecture, managed vs self-managed operational differences, source/sink delivery guarantees, and connector lifecycle.
- [single-message-transforms.md](single-message-transforms.md) — SMT execution position, common built-in transforms, chaining performance, and when to use stream processing instead.
- [error-handling-dlq.md](error-handling-dlq.md) — Connector failure modes, DLQ configuration, debugging with header context, and connector health monitoring.

See [../connect-vs-flink-framework.md](../connect-vs-flink-framework.md) for when a requirement
crosses from Connect's territory (integration) into Flink's (processing) — CDC, latency, exactly-once,
schema evolution, and multi-team pipelines.
