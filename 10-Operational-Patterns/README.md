# Module 10 — Operational Patterns

Patterns for reliable data movement between databases and Kafka, change data capture, and fast state recovery for stateful stream processors.

## Files

- [transactional-outbox.md](transactional-outbox.md) — The dual-write problem, outbox table structure, CDC-based relay, at-least-once delivery, and when to skip the pattern.
- [cdc-debezium.md](cdc-debezium.md) — Debezium architecture (WAL/binlog reading), snapshot vs streaming modes, schema change handling, and production operational concerns.
- [rocksdb-s3-preseeding.md](rocksdb-s3-preseeding.md) — RocksDB snapshot creation via reflection, S3 archival, delta catch-up on cold start, and when the pattern justifies its complexity.
- [gitops-terraform.md](gitops-terraform.md) — GitOps model for Kafka topic management: Confluent Terraform provider, two-pipeline model (infra vs cluster), topic naming convention as ACL driver, schema registration via CI/CD, CFK self-managed Connect, and CSI Secrets Provider for credential injection.
- [consumer-onboarding.md](consumer-onboarding.md) — Six-gate checklist for onboarding new consumer teams to production topics: schema contract validation, consumer group isolation and quotas, DLQ configuration, offset commit strategy, lag monitoring, and data handling obligations.
