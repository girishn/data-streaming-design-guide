# Data Streaming Design Guide — Confluent Kafka

Enterprise-grade reference covering Apache Kafka and Confluent Platform architecture decisions, reliability patterns, stream processing tradeoffs, governance and security models, and operational design. Written for engineers who are past the tutorial stage and need precise technical grounding when making production decisions — partition strategies, exactly-once guarantees, schema evolution rules, CDC wiring, and security authentication flows.

## How to Use This Guide

Each module is self-contained. Navigate directly to the area matching your immediate problem: if you are debugging consumer lag, start in `11-Monitoring-Observability/consumer-lag.md`; if you are wiring a CDC pipeline, go to `10-Operational-Patterns/cdc-debezium.md`. Cross-references are explicit within files. No prescribed reading order — this is a reference, not a course.

**Designing a new system from scratch?** Start with the [Decision Framework](decision-framework.md) — a phase-by-phase elimination flow that maps your requirements to forced choices and ruled-out options before you touch any module detail.

**Facing an ambiguous problem or interview question?** See the [Streaming Design Approach](streaming-design-approach.md) — the meta-skill for getting from an unclear problem to a set of requirements worth running through the framework: discovery questions, elimination thinking, layer-by-layer structure, and how to handle open decisions.

**Designing the topic topology for a new system?** See the [Topic Design Framework](topic-design-framework.md) — a layered decision path covering ordering constraints, tenant isolation, retention and state topology, partition count sizing, schema governance, and an anti-pattern checklist.

**Choosing a stream processing framework or designing a stateful pipeline?** See the [Stream Processing Framework](stream-processing-framework.md) — a layered decision path from stateless vs stateful through framework selection, state size, time semantics, windowing, and fault tolerance configuration.

**Designing an end-to-end pipeline that touches an external source or sink?** See the [Connect vs Flink Framework](connect-vs-flink-framework.md) — where Kafka Connect (data integration) hands off to Flink (processing), and how CDC, latency SLAs, exactly-once, schema evolution, and multi-team sharing each shift that boundary. Use this before the Stream Processing Framework when a database, API, or external system is involved.

**Building a real-time AI or GenAI application on Kafka?** See [Module 15 — AI and GenAI Patterns](15-AI-GenAI-Patterns/README.md) — streaming RAG architecture, Flink `ML_PREDICT()` for inline model inference, and the complete airline chatbot case study.

**Preparing for an interview or design session?** See the [Interview Cheat Sheet](interview-cheat-sheet.md) — a condensed one-pager: five discovery questions, the two structural gates, the key elimination table, and the anti-pattern list. Five minutes to reload the mental model.

---

## Designing a System — Decision Sequence

When starting a new data streaming design, work through these questions in order. Each links to the relevant module.

**1. What is the data contract?**
Define the event schema before any infrastructure decision. What fields, what types, what nullable guarantees? → [08 — Stream Governance](08-Stream-Governance/schema-evolution.md)

**2. What is the topic structure?**
How many partitions? What is the partition key? What retention policy? → [02 — Broker Infrastructure](02-Broker-Infrastructure/partitioning-strategies.md)

**3. What delivery guarantee does the producer need?**
At-least-once (idempotent) or exactly-once (transactional)? → [03 — Data Production](03-Data-Production/idempotent-producers.md)

**4. What is the consumer topology?**
How many consumer groups? How do groups share partitions? What happens on rebalance? → [04 — Data Consumption](04-Data-Consumption/consumer-groups.md)

**5. Is stream processing required?**
Stateless transformations, stateful aggregations, or joins? Which framework — Kafka Streams, Flink, or ksqlDB? → [06 — Stream Processing](06-Stream-Processing/kafka-streams-vs-flink.md), [ksqldb.md](06-Stream-Processing/ksqldb.md)

**6. What are the governance requirements?**
Schema compatibility enforcement, PII handling, audit lineage? → [08 — Stream Governance](08-Stream-Governance/README.md)

**7. What is the security model?**
mTLS or OAuth/OIDC? Fine-grained access via Identity Pools? → [09 — Security Architecture](09-Security-Architecture/mtls-oauth.md)

**8. How does data enter the system?**
Direct producer, Kafka Connect, or CDC from a database? If an external system is on either edge,
work through the boundary decision first → [Connect vs Flink Framework](connect-vs-flink-framework.md),
[05 — Enterprise Connect](05-Enterprise-Connect/README.md), [10 — Operational Patterns](10-Operational-Patterns/cdc-debezium.md)

**9. How will the system be observed?**
What metrics matter? What are the alert thresholds? → [11 — Monitoring and Observability](11-Monitoring-Observability/README.md)

**10. What is the DR strategy?**
Active-passive or active-active? RPO/RTO requirements? → [12 — Multi-Region DR](12-Multi-Region-DR/README.md)

---

## Production Readiness Checklist

Before promoting a Kafka-based system to production, verify:

**Reliability**
- [ ] Replication factor ≥ 3 for all production topics
- [ ] `min.insync.replicas` = RF − 1 on all production topics
- [ ] `acks=all` configured on all producers
- [ ] Idempotent producers enabled (`enable.idempotence=true`)
- [ ] Transactional producers used where cross-partition atomicity is required

**Consumer Stability**
- [ ] Consumer groups use `CooperativeStickyAssignor`
- [ ] Static membership (`group.instance.id`) configured for StatefulSet deployments
- [ ] `max.poll.interval.ms` tuned to actual processing time — not the default
- [ ] Dead letter queue configured for all Kafka Connect connectors

**Governance**
- [ ] All topics registered in Schema Registry with an explicit compatibility mode
- [ ] `BACKWARD` or `FULL_TRANSITIVE` compatibility enforced (not `NONE`)
- [ ] PII fields tagged; crypto-shredding key rotation plan in place if applicable
- [ ] Broker-side schema validation enabled if forward compatibility is a hard requirement

**Security**
- [ ] All client connections use mTLS or OAuth/OIDC — no PLAINTEXT in production
- [ ] ACLs scoped to minimum required topics and operations per service identity
- [ ] TLS certificates have a rotation process documented

**Observability**
- [ ] `UnderReplicatedPartitions` alert set at threshold > 0 for > 60 seconds
- [ ] `ActiveControllerCount` alert set at threshold ≠ 1 (immediate P1)
- [ ] Consumer lag growth-rate alert (not absolute threshold) per consumer group
- [ ] `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` dashboards active

**Disaster Recovery**
- [ ] DR strategy chosen and documented (Cluster Linking or MirrorMaker 2)
- [ ] Consumer offset sync enabled on DR link
- [ ] Failover procedure tested in a non-production environment
- [ ] RPO and RTO targets defined and verified against replication lag measurements

---

## Table of Contents

### [01 — Core Concepts](01-Core-Concepts/README.md)
Foundational positioning: OSS Kafka vs Confluent Cloud capabilities, the data streaming platform model, and the log as a first-class data structure.

- [kafka-vs-confluent.md](01-Core-Concepts/kafka-vs-confluent.md)
- [data-streaming-platform.md](01-Core-Concepts/data-streaming-platform.md)

---

### [02 — Broker Infrastructure](02-Broker-Infrastructure/README.md)
Broker internals: KRaft mode replacing ZooKeeper, partition strategy design, replication factor and compaction decisions, and tiered storage for long retention.

- [kraft-mode.md](02-Broker-Infrastructure/kraft-mode.md)
- [partitioning-strategies.md](02-Broker-Infrastructure/partitioning-strategies.md)
- [tiered-storage.md](02-Broker-Infrastructure/tiered-storage.md)

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
Kafka Streams vs Flink decision criteria, ksqlDB for SQL-based streaming, state management internals, RocksDB tuning, windowing semantics, and fault tolerance models.

- [kafka-streams-vs-flink.md](06-Stream-Processing/kafka-streams-vs-flink.md)
- [ksqldb.md](06-Stream-Processing/ksqldb.md)
- [state-management.md](06-Stream-Processing/state-management.md)
- [windowing.md](06-Stream-Processing/windowing.md)

---

### [07 — Advanced Reliability](07-Advanced-Reliability/README.md)
ISR mechanics and MISR configuration, full exactly-once semantic protocol from idempotence through two-phase commit.

- [replication-isr.md](07-Advanced-Reliability/replication-isr.md)
- [exactly-once-semantics.md](07-Advanced-Reliability/exactly-once-semantics.md)

---

### [08 — Stream Governance](08-Stream-Governance/README.md)
Schema Registry compatibility modes, broker-side schema validation, PII tagging and the crypto-shredding pattern for right-to-erasure, stream catalog, stream lineage, CSFLE, and data contracts.

- [schema-evolution.md](08-Stream-Governance/schema-evolution.md)
- [broker-side-validation.md](08-Stream-Governance/broker-side-validation.md)
- [pii-tracking.md](08-Stream-Governance/pii-tracking.md)
- [stream-catalog.md](08-Stream-Governance/stream-catalog.md)
- [stream-lineage.md](08-Stream-Governance/stream-lineage.md)
- [csfle.md](08-Stream-Governance/csfle.md)
- [data-contracts.md](08-Stream-Governance/data-contracts.md)

---

### [09 — Security Architecture](09-Security-Architecture/README.md)
mTLS and OAuth/OIDC authentication flows, Confluent Cloud Identity Pools with CEL expressions, multi-protocol listener migration, RBAC role taxonomy and bindings, and private networking for Dedicated clusters.

- [mtls-oauth.md](09-Security-Architecture/mtls-oauth.md)
- [cel-identity-pools.md](09-Security-Architecture/cel-identity-pools.md)
- [multi-protocol-auth.md](09-Security-Architecture/multi-protocol-auth.md)
- [rbac.md](09-Security-Architecture/rbac.md)
- [private-networking.md](09-Security-Architecture/private-networking.md)
- [cloud-idp-integration.md](09-Security-Architecture/cloud-idp-integration.md)

---

### [10 — Operational Patterns](10-Operational-Patterns/README.md)
Transactional outbox for dual-write safety, Debezium CDC configuration, RocksDB state pre-seeding from S3 for fast cold starts, GitOps/Terraform self-service topic management, OPA policy enforcement for platform governance, and OSS Kafka to Confluent Cloud migration.

- [transactional-outbox.md](10-Operational-Patterns/transactional-outbox.md)
- [cdc-debezium.md](10-Operational-Patterns/cdc-debezium.md)
- [rocksdb-s3-preseeding.md](10-Operational-Patterns/rocksdb-s3-preseeding.md)
- [gitops-terraform.md](10-Operational-Patterns/gitops-terraform.md)
- [blue-green-topic-migration.md](10-Operational-Patterns/blue-green-topic-migration.md)
- [consumer-onboarding.md](10-Operational-Patterns/consumer-onboarding.md)
- [producer-onboarding.md](10-Operational-Patterns/producer-onboarding.md)
- [connector-onboarding.md](10-Operational-Patterns/connector-onboarding.md)
- [platform-automation.md](10-Operational-Patterns/platform-automation.md)
- [opa-policy-enforcement.md](10-Operational-Patterns/opa-policy-enforcement.md)
- [oss-to-confluent-cloud-migration.md](10-Operational-Patterns/oss-to-confluent-cloud-migration.md)

---

### [11 — Monitoring and Observability](11-Monitoring-Observability/README.md)
Consumer lag measurement and alerting strategy, broker JMX metrics and thread pool health, Confluent Cloud Metrics API for managed clusters.

- [consumer-lag.md](11-Monitoring-Observability/consumer-lag.md)
- [broker-jmx-metrics.md](11-Monitoring-Observability/broker-jmx-metrics.md)
- [confluent-cloud-metrics-api.md](11-Monitoring-Observability/confluent-cloud-metrics-api.md)
- [datadog-integration.md](11-Monitoring-Observability/datadog-integration.md)

---

### [12 — Multi-Region DR](12-Multi-Region-DR/README.md)
Cluster Linking for byte-exact offset-preserving replication, MirrorMaker 2 for OSS-compatible cross-cluster replication, and active-active topology tradeoffs.

- [cluster-linking.md](12-Multi-Region-DR/cluster-linking.md)
- [mirrormaker2.md](12-Multi-Region-DR/mirrormaker2.md)
- [active-active-dr.md](12-Multi-Region-DR/active-active-dr.md)

---

### [13 — Performance Tuning](13-Performance-Tuning/README.md)
Producer batching and compression tuning, consumer fetch configuration, broker thread pool sizing, log flush strategy, and quota management for multi-tenant clusters.

- [producer-tuning.md](13-Performance-Tuning/producer-tuning.md)
- [consumer-tuning.md](13-Performance-Tuning/consumer-tuning.md)
- [broker-tuning.md](13-Performance-Tuning/broker-tuning.md)
- [quota-management.md](13-Performance-Tuning/quota-management.md)
- [end-to-end-latency.md](13-Performance-Tuning/end-to-end-latency.md)

---

### [14 — Case Studies](14-Case-Studies/README.md)
Production architecture walkthroughs: B2B logistics CX transformation, financial e-commerce exactly-once payment ledger, enterprise banking platform design via the decision framework, enterprise retail platform validation, real-time fraud detection, B2B logistics SaaS topic design, and airline customer service streaming RAG pipeline.

- [real-time-shipment-tracking.md](14-Case-Studies/real-time-shipment-tracking.md)
- [financial-ecommerce-eos.md](14-Case-Studies/financial-ecommerce-eos.md)
- [banking-platform-design.md](14-Case-Studies/banking-platform-design.md)
- [retail-platform-design.md](14-Case-Studies/retail-platform-design.md)
- [fintech-fraud-detection.md](14-Case-Studies/fintech-fraud-detection.md)
- [logistics-order-tracking.md](14-Case-Studies/logistics-order-tracking.md)
- [streaming-rag-airline.md](14-Case-Studies/streaming-rag-airline.md)

---

### [15 — AI and GenAI Patterns](15-AI-GenAI-Patterns/README.md)
Kafka and Confluent Platform as the backbone for real-time AI applications: streaming RAG lifecycle (data augmentation, inference, workflow routing, post-processing), Flink `ML_PREDICT()` for inline model inference without a separate embedding service, and operational patterns for PII masking, audit observability, and LLM response caching.

- [streaming-rag.md](15-AI-GenAI-Patterns/streaming-rag.md)
- [flink-ml-predict.md](15-AI-GenAI-Patterns/flink-ml-predict.md)

---

### [16 — Hands-On Labs](16-Hands-On-Labs/README.md)
Practical exercises for a staff-level platform engineer: Flink stream-processing labs (framework selection, Confluent Cloud Flink SQL, event-time windowing, EOS/checkpoint tuning) and security labs (mTLS vs OAuth design, CFK local mTLS + OAuth, RBAC/Identity Pools via Terraform, zero-downtime SCRAM-to-OAuth migration). Each lab is tagged Design, Hands-on (Local), or Hands-on (Cloud) and cites the module file it's grounded in.

- [flink-lab-01-framework-selection-design.md](16-Hands-On-Labs/flink-lab-01-framework-selection-design.md)
- [flink-lab-02-confluent-cloud-flink-sql.md](16-Hands-On-Labs/flink-lab-02-confluent-cloud-flink-sql.md)
- [flink-lab-03-event-time-windowing.md](16-Hands-On-Labs/flink-lab-03-event-time-windowing.md)
- [flink-lab-04-eos-checkpoint-tuning-design.md](16-Hands-On-Labs/flink-lab-04-eos-checkpoint-tuning-design.md)
- [flink-lab-05-connect-vs-flink-boundary-design.md](16-Hands-On-Labs/flink-lab-05-connect-vs-flink-boundary-design.md)
- [security-lab-01-mtls-oauth-design.md](16-Hands-On-Labs/security-lab-01-mtls-oauth-design.md)
- [security-lab-02-cfk-local-mtls-oauth.md](16-Hands-On-Labs/security-lab-02-cfk-local-mtls-oauth.md)
- [security-lab-03-rbac-identity-pools-terraform.md](16-Hands-On-Labs/security-lab-03-rbac-identity-pools-terraform.md)
- [security-lab-04-scram-to-oauth-migration.md](16-Hands-On-Labs/security-lab-04-scram-to-oauth-migration.md)
