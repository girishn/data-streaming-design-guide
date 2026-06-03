# Module 10 — Operational Patterns

Patterns for reliable data movement between databases and Kafka, change data capture, and fast state recovery for stateful stream processors.

## Files

- [transactional-outbox.md](transactional-outbox.md) — The dual-write problem, outbox table structure, CDC-based relay, at-least-once delivery, and when to skip the pattern.
- [cdc-debezium.md](cdc-debezium.md) — Debezium architecture (WAL/binlog reading), snapshot vs streaming modes, schema change handling, and production operational concerns.
- [rocksdb-s3-preseeding.md](rocksdb-s3-preseeding.md) — RocksDB snapshot creation via reflection, S3 archival, delta catch-up on cold start, and when the pattern justifies its complexity.
- [gitops-terraform.md](gitops-terraform.md) — GitOps model for Kafka topic management: Confluent Terraform provider, two-pipeline model (infra vs cluster), topic naming convention as ACL driver, schema registration via CI/CD, CFK self-managed Connect, and CSI Secrets Provider for credential injection.
- [consumer-onboarding.md](consumer-onboarding.md) — Six-gate checklist for onboarding new consumer teams to production topics: schema contract validation, consumer group isolation and quotas, DLQ configuration, offset commit strategy, lag monitoring, and data handling obligations.
- [producer-onboarding.md](producer-onboarding.md) — Six-gate checklist for onboarding producer teams: topic design review, schema registration and contract authorship, partition key and ordering contract, producer configuration, ACL provisioning, and staging validation.
- [connector-onboarding.md](connector-onboarding.md) — Gates for source and sink connector deployment: hosting decision (managed vs self-managed), SMT chain review, mandatory DLQ, credential injection via SSM, throughput declaration, and connector availability alerting.
- [platform-automation.md](platform-automation.md) — Four-layer automation pyramid: fully automated (zero-touch topic/ACL/schema provisioning), policy-automated, PR review gate, and platform-team-only decisions. Includes pipeline structure, tool choices, and what automation does not solve.
- [blue-green-topic-migration.md](blue-green-topic-migration.md) — Safe repartitioning via blue-green topic swap: migration state machine, dual-write vs Cluster Linking vs MM2, offset translation, platform team responsibilities, rollback gates, and decision table.
- [opa-policy-enforcement.md](opa-policy-enforcement.md) — OPA policy enforcement for Kafka platforms: conftest Rego policies against Terraform plans in CI, OPA Gatekeeper ConstraintTemplates on CFK KafkaTopic CRDs, and broker-side CreateTopicPolicy for self-managed deployments. Includes partition count limits, naming convention validation, and a policy coverage matrix.
- [oss-to-confluent-cloud-migration.md](oss-to-confluent-cloud-migration.md) — Ordered migration sequence from self-managed OSS Kafka to Confluent Cloud: pre-migration assessment (CKU sizing, feature gap), Schema Linking for continuous SR sync, Cluster Linking with the `promote` command for zero-downtime data cutover, auth transition, Connect migration decision, and decommission sequence.
