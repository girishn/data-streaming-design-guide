# Module 02 — Broker Infrastructure

Broker internals for production engineers: KRaft mode as the replacement for ZooKeeper, partition and replication factor design, log compaction decisions, and tiered storage for long retention.

## Files

- [kraft-mode.md](kraft-mode.md) — KRaft architecture, ZooKeeper removal, controller quorum, failover mechanics, and migration requirements.
- [partitioning-strategies.md](partitioning-strategies.md) — Partition count sizing, replication factor decisions, log compaction vs retention, and partitioning strategy trade-offs.
- [tiered-storage.md](tiered-storage.md) — Hot/cold tier architecture, hotset configuration, object storage backends, OSS Kafka KIP-405 vs Confluent Platform, and when to use tiered storage for regulatory retention requirements.
