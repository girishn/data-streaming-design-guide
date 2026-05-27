# Module 03 — Data Production

Producer reliability: the mechanics behind duplicate prevention and atomic multi-partition writes.

## Files

- [idempotent-producers.md](idempotent-producers.md) — The duplicate problem, PID/sequence-number deduplication, configuration, and what idempotence does not guarantee.
- [transactional-producers.md](transactional-producers.md) — Cross-partition atomicity, the transactions API, Transaction Coordinator internals, zombie fencing, consumer read isolation, and performance cost.
