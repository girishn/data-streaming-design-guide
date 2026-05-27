# Kafka vs Confluent — Capability Map and Decision Framework

## OSS Kafka: What You Operate

Apache Kafka OSS provides the core broker (log storage, partition replication, consumer group coordination), the producer/consumer client libraries, Kafka Connect framework (workers + connector plugin interface), Kafka Streams library, MirrorMaker 2 for cross-cluster replication, and command-line tooling (`kafka-topics.sh`, `kafka-consumer-groups.sh`, `kafka-configs.sh`).

What OSS Kafka does NOT provide: Schema Registry, a managed connector marketplace, ksqlDB, stream governance (data catalog, PII tagging, data lineage), Tableflow (topic-to-Iceberg materialization), a managed Flink runtime, centralized RBAC, audit logging, or a metrics/alerting UI. Every one of these capabilities exists either in Confluent Platform (self-managed enterprise distribution) or Confluent Cloud (fully managed SaaS), but not in the upstream Apache project.

## Confluent-Specific Components

**Confluent Schema Registry** — A standalone service (included in both Confluent Platform and Confluent Cloud) that stores and versions Avro, Protobuf, and JSON Schema definitions. Exposes a REST API. Clients embed a 4-byte schema ID in every message; deserializers fetch the schema from the Registry to decode. Not part of OSS Kafka — must be self-hosted if using OSS.

**Kafka Connect Managed (Confluent Cloud)** — Confluent hosts and operates Connect workers. You configure connectors via API or UI; Confluent handles JVM tuning, plugin upgrades, and worker failure recovery. Self-managed Connect requires you to operate a Connect cluster (JVM sizing, plugin classpath management, scaling, failure handling).

**ksqlDB** — A streaming SQL engine built on Kafka Streams. Exposes a SQL interface for filtering, aggregating, joining, and windowing Kafka topics without writing JVM code. Available as a managed service on Confluent Cloud. In OSS Kafka there is no equivalent — you write Kafka Streams code or use a separate Flink deployment.

**Stream Governance** — Confluent Cloud's data catalog layer: tagging schemas and fields with business/compliance labels (PII, GDPR, CCPA), data lineage visualization across topics and connectors, data quality rules. No OSS equivalent.

**Tableflow** — Automated materialization of Kafka topics into Apache Iceberg tables on S3/GCS, queryable via Iceberg-compatible engines (Trino, Spark, Athena). This eliminates a custom Flink or Spark job for the common pattern of "stream data into a lakehouse."

**Confluent Managed Flink** — Flink jobs deployed and operated by Confluent, integrated with Schema Registry for schema-aware SQL. The OSS path is self-operating a Flink cluster (Job Manager + Task Managers) with all associated upgrade and availability burden.

**Cluster Linking** — Native, broker-level topic mirroring across clusters, regions, and clouds without requiring a Kafka Connect deployment. The primary tool for cloud migration, active-active multi-region, and disaster recovery setups. Unlike MirrorMaker 2, it preserves offsets across clusters and requires no additional Connect infrastructure to operate.

**WarpStream** — A Kafka-compatible streaming platform that runs inside the customer's own VPC (Bring Your Own Cloud). The data plane lives entirely within your cloud environment; Confluent manages the streaming engine as a service. Wire-compatible with Kafka — existing clients connect without code changes. Sits between Confluent Cloud (fully managed, data leaves your network) and Confluent Platform (self-managed, full operational burden). The target is organisations with data residency mandates that rule out standard SaaS but that cannot absorb the operational cost of self-managing a Kafka cluster.

## Confluent Cloud Cluster Types

Confluent Cloud is not a single deployment model. Cluster type determines which features are available, what networking options apply, and what the performance envelope is.

| Cluster Type | Characteristics | Key Constraints |
|---|---|---|
| **Basic** | Serverless, elastic, multi-tenant | No OAuth support; no private networking; no broker-side schema validation |
| **Standard** | Serverless, elastic, multi-tenant | OAuth supported; no private networking |
| **Dedicated** | Single-tenant, fixed CKU sizing | Required for PrivateLink/VPC Peering and broker-side schema ID validation |
| **Freight** | Optimized for high-throughput, latency-insensitive workloads | Not suitable for interactive/low-latency consumers |
| **Enterprise** | High-scale, enhanced security and support tiers | — |

The Dedicated tier is the threshold that unlocks the most operationally significant features. If your compliance posture requires private networking, or your governance model requires broker-side schema enforcement, Basic/Standard are non-starters regardless of cost.

## Self-Managed vs Confluent Cloud: Decision Factors

### Networking and Data Sovereignty

Confluent Cloud offers a tiered networking model: **Public** (default, traffic over internet) → **VPC/VNet Peering** → **AWS Transit Gateway / Azure VNet** → **PrivateLink** (inbound and outbound). Dedicated clusters add a Private Network Interface, ensuring broker traffic never traverses the public internet. All private networking options require the Dedicated tier or above.

Self-managed Confluent Platform gives you complete network control — topology, firewall rules, data residency boundaries. This is required for organizations with strict on-premises data residency mandates, air-gapped environments, or edge-to-core hybrid architectures that cannot route data through a cloud provider's network at all.

### Multi-Tenancy Model

Confluent Cloud enforces multi-tenancy at the platform level via the **Kora engine**: logical isolation through Organizations and Environments, client throughput quotas to prevent noisy-neighbor effects, and no tenant-visible access to underlying compute. This is transparent to the engineer — you configure topics and ACLs, not JVM heap sizes.

Confluent Platform multi-tenancy is manual: RBAC and ACLs for access control, JVM heap tuning per broker to prevent one tenant's partition load from exhausting resources for others. At high partition counts or mixed-workload clusters, this requires active capacity management that Confluent Cloud abstracts away.

### SLA and Availability

Confluent Cloud provides a contractual **99.99% uptime SLA** for Dedicated clusters deployed across multiple availability zones. For Basic/Standard clusters the SLA is lower. This is a committed service guarantee backed by Confluent's operations team.

With self-managed Confluent Platform, availability is an operational responsibility. There is no SLA — your uptime target is determined by your own replication configuration, broker placement, and incident response capability. The distinction matters for regulated industries where a contractual SLA is a compliance requirement, not just a preference.

### Cost Crossover

Confluent Cloud pricing is consumption-based: CKU (Confluent Kafka Unit) hourly rate + storage + egress. For dedicated clusters, a Dedicated cluster starts at ~$0.65/hour per CKU. For small workloads (<500 MB/s sustained throughput, <5TB storage), Confluent Cloud is typically cheaper than the engineering cost of operating OSS Kafka (EC2 instances, ZooKeeper or KRaft quorum management, monitoring, upgrades).

The crossover occurs roughly when:
- Sustained throughput exceeds 1–2 GB/s (requires multiple CKUs or a large Dedicated cluster)
- Storage retention exceeds 10TB (Confluent Cloud storage pricing becomes significant)
- You need strict network isolation beyond what Confluent's private networking options provide

At large scale (10+ GB/s throughput, 100TB+ storage), self-managed on EC2 or bare metal with i3.4xlarge or i3en instances often wins on pure infrastructure cost, but requires a dedicated Kafka operations team.

### Compliance Constraints

Confluent Cloud is SOC 2 Type II, ISO 27001, PCI DSS Level 1, and HIPAA eligible. However, some regulated industries require on-premises data residency or disallow multi-tenant managed services for certain data classifications. In these cases, Confluent Platform (self-managed) on your own infrastructure satisfies residency requirements while retaining Schema Registry, RBAC, and audit logging.

### Staffing Requirements

| Deployment Model | Required Expertise |
|---|---|
| Confluent Cloud (Basic/Standard) | Kafka developer knowledge; no cluster ops |
| Confluent Cloud (Dedicated) | Developer + connector/schema expertise; limited ops |
| WarpStream (BYOC) | Developer + cloud infrastructure (VPC, networking, IAM); no Kafka ops |
| Confluent Platform (self-managed) | Full Kafka admin: ZK/KRaft ops, JVM tuning, disk management, upgrade procedures |
| OSS Kafka | As above + custom tooling for Schema Registry, RBAC, monitoring |

The hidden cost of OSS Kafka is operational burden: version upgrades (Kafka 3.x → 4.x), rolling restarts, broker rebalancing, disk capacity management, and incident response. Confluent Platform reduces this via controlled distribution and support; Confluent Cloud eliminates it.

## The Gold-Layer Broker Model

In traditional data architectures, databases are the system of record: the database is the truth, and downstream systems query or replicate from it. In a data streaming platform, the **broker is the system of record** — the topic log is the authoritative, immutable record of what happened and when.

Implications:
- The database (Postgres, MySQL, MongoDB) becomes a materialized view of the topic log, rebuilt by a consumer on demand
- Retention policy on the topic determines how far back you can replay — this is the equivalent of backup/restore for a database
- Schema changes are governed at the topic level (Schema Registry), not at the database level — database schema follows topic schema, not the reverse
- Downstream systems consume independently from the same topic; there is no fan-out coordination between producer and each consumer

This inversion matters for architecture: when you design a new data flow, the question is "what is the topic structure?" first, and "what database projection do we need?" second. The topic is permanent; the database is ephemeral relative to it.
