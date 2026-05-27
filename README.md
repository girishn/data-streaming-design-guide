# Data Streaming Design Guide — Confluent Kafka

Enterprise-grade reference covering Apache Kafka and Confluent Platform architecture decisions, reliability patterns, stream processing tradeoffs, governance and security models, and operational design. Written for engineers who are past the tutorial stage and need precise technical grounding when making production decisions — partition strategies, exactly-once guarantees, schema evolution rules, CDC wiring, and security authentication flows.

## How to Use This Guide

Each module is self-contained. Navigate directly to the area matching your immediate problem: if you are debugging consumer lag, start in `04-Data-Consumption`; if you are wiring a CDC pipeline, go to `10-Operational-Patterns/cdc-debezium.md`. Cross-references are explicit within files. No prescribed reading order — this is a reference, not a course.

---

## Table of Contents

### [01 — Core Concepts](01-Core-Concepts/README.md)
Foundational positioning: OSS Kafka vs Confluent Cloud capabilities, the data streaming platform model, and the log as a first-class data structure.

- [kafka-vs-confluent.md](01-Core-Concepts/kafka-vs-confluent.md)
- [data-streaming-platform.md](01-Core-Concepts/data-streaming-platform.md)

---

### [02 — Broker Infrastructure](02-Broker-Infrastructure/README.md)
Broker internals: KRaft mode replacing ZooKeeper, partition strategy design, replication factor and compaction decisions.

- [kraft-mode.md](02-Broker-Infrastructure/kraft-mode.md)
- [partitioning-strategies.md](02-Broker-Infrastructure/partitioning-strategies.md)

---

### [03 — Data Production](03-Data-Production/README.md)
Producer reliability: idempotent producers and the duplicate problem, transactional producers for cross-partition atomicity.

- [idempotent-producers.md](03-Data-Production/idempotent-producers.md)
- [transactional-producers.md](03-Data-Production/transactional-producers.md)

---

### [04 — Data Consumption](04-Data-Consumption/README.md)
Consumer group mechanics, partition assignment strategies, cooperative rebalancing, and static membership for stable Kubernetes deployments.

- [consumer-groups.md](04-Data-Consumption/consumer-groups.md)
- [cooperative-rebalancing.md](04-Data-Consumption/cooperative-rebalancing.md)
- [static-membership.md](04-Data-Consumption/static-membership.md)

---

### [05 — Enterprise Connect](05-Enterprise-Connect/README.md)
Kafka Connect in production: managed vs self-operated connectors, SMT chains for field-level transforms, and dead letter queue patterns.

- [managed-connectors.md](05-Enterprise-Connect/managed-connectors.md)
- [single-message-transforms.md](05-Enterprise-Connect/single-message-transforms.md)
- [error-handling-dlq.md](05-Enterprise-Connect/error-handling-dlq.md)

---

### [06 — Stream Processing](06-Stream-Processing/README.md)
Kafka Streams vs Flink decision criteria, state management internals, RocksDB tuning, and fault tolerance models.

- [kafka-streams-vs-flink.md](06-Stream-Processing/kafka-streams-vs-flink.md)
- [state-management.md](06-Stream-Processing/state-management.md)

---

### [07 — Advanced Reliability](07-Advanced-Reliability/README.md)
ISR mechanics and MISR configuration, full exactly-once semantic protocol from idempotence through two-phase commit.

- [replication-isr.md](07-Advanced-Reliability/replication-isr.md)
- [exactly-once-semantics.md](07-Advanced-Reliability/exactly-once-semantics.md)

---

### [08 — Stream Governance](08-Stream-Governance/README.md)
Schema Registry compatibility modes, broker-side schema validation, PII tagging and the crypto-shredding pattern for right-to-erasure.

- [schema-evolution.md](08-Stream-Governance/schema-evolution.md)
- [broker-side-validation.md](08-Stream-Governance/broker-side-validation.md)
- [pii-tracking.md](08-Stream-Governance/pii-tracking.md)

---

### [09 — Security Architecture](09-Security-Architecture/README.md)
mTLS and OAuth/OIDC authentication flows, Confluent Cloud Identity Pools with CEL expressions, multi-protocol listener migration.

- [mtls-oauth.md](09-Security-Architecture/mtls-oauth.md)
- [cel-identity-pools.md](09-Security-Architecture/cel-identity-pools.md)
- [multi-protocol-auth.md](09-Security-Architecture/multi-protocol-auth.md)

---

### [10 — Operational Patterns](10-Operational-Patterns/README.md)
Transactional outbox for dual-write safety, Debezium CDC configuration, and RocksDB state pre-seeding from S3 for fast cold starts.

- [transactional-outbox.md](10-Operational-Patterns/transactional-outbox.md)
- [cdc-debezium.md](10-Operational-Patterns/cdc-debezium.md)
- [rocksdb-s3-preseeding.md](10-Operational-Patterns/rocksdb-s3-preseeding.md)

---

### [11 — Case Studies](11-Case-Studies/README.md)
Two production architectures: B2B logistics CX transformation with Salesforce CDC and Flink SQL, and financial e-commerce exactly-once payment ledger with hot SKU handling.

- [logistics-cx-transformation.md](11-Case-Studies/logistics-cx-transformation.md)
- [financial-ecommerce-eos.md](11-Case-Studies/financial-ecommerce-eos.md)
