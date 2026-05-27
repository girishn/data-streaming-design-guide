# Kafka Streams vs Apache Flink — Decision Criteria

## Architectural Differences

**Kafka Streams** is an embedded client library — it runs inside your application's JVM process alongside your business logic. There is no separate cluster to provision or operate. Scaling is achieved by starting additional instances of the application; Kafka's consumer group protocol distributes partitions across them automatically. The operational surface is identical to any other Java microservice.

**Apache Flink** is a distributed execution framework with a dedicated cluster runtime. A **JobManager** orchestrates the job — task scheduling, checkpoint coordination, and failure recovery. **TaskManagers** execute the actual processing. Scaling requires adding TaskManagers or, on managed Flink (Confluent Cloud), adjusting the parallelism configuration. Confluent's managed Flink abstracts cluster management entirely: checkpointing, upgrades, and scaling are handled by the platform.

| Dimension | Kafka Streams | Apache Flink |
|---|---|---|
| Deployment model | Embedded library, app JVM | Dedicated cluster (JobManager + TaskManagers) |
| Operational footprint | Same as a Java microservice | Separate infrastructure to operate or manage |
| Scaling unit | Application instances | TaskManagers / parallelism config |
| Language support | JVM (Java, Kotlin, Scala) | JVM + Python (PyFlink) + SQL |
| State scale | 10s of GB per instance | TB to PB (RocksDB backend) |
| Managed option | No dedicated managed offering | Confluent Cloud managed Flink |

## When to Choose Kafka Streams

Kafka Streams is the right choice when:

- The pipeline is **Kafka-to-Kafka** — consuming from topics, transforming or aggregating, producing back to topics — with no external cluster dependency
- The team operates **Java microservices** and wants stream processing to live in the same codebase, CI/CD pipeline, and monitoring stack
- **State requirements are moderate** — tens of GB per instance fits on the local disk of the application node
- The use case is **event-driven microservices** — materialising a view of a topic into a local store queryable via interactive queries, or enriching events with local state
- **Operational simplicity** outweighs processing power — no cluster to manage means no JobManager failures, no TaskManager restarts, no checkpoint store to configure

Kafka Streams is a poor fit when state grows to hundreds of GB, when the pipeline requires complex multi-stream joins across heterogeneous sources, or when CEP (Complex Event Processing) patterns are needed.

## When to Choose Apache Flink

Flink is the right choice when:

- **State scale exceeds what fits on a single application node** — Flink's `EmbeddedRocksDBStateBackend` with incremental checkpoints to S3 handles TB-scale state that would be impractical for Kafka Streams
- The use case requires **complex analytical windowing** — multi-stream joins, session windows across heterogeneous event sources, or time-travel queries
- **CEP patterns** are needed — detecting sequences of events across time with the Flink CEP library
- The team prefers **SQL-based stream processing** — Flink SQL provides a declarative interface that Kafka Streams does not offer
- **Confluent Cloud managed Flink** is available — it eliminates cluster operations and integrates natively with Schema Registry for schema-aware SQL

## State Rebuild: The Deciding Factor at Scale

The state rebuild behaviour on failure or restart is often the most operationally significant difference between the two frameworks.

**Kafka Streams** uses changelog-based recovery. Every update to a local RocksDB state store is mirrored as a record to a compacted changelog topic in Kafka. On restart, the state is rebuilt by replaying this changelog over the network. For state stores with millions or hundreds of millions of keys, this replay can take **45 minutes to over 2 hours** — during which the instance is not processing new records and consumer lag accumulates. See `06-Stream-Processing/state-management.md` for the S3 pre-seeding pattern that reduces this to seconds.

**Flink** uses checkpoint-based recovery (Chandy-Lamport algorithm). Periodic snapshots of the entire job state are written asynchronously to durable distributed storage (S3, GCS). On restart, TaskManagers download the most recent checkpoint directly — no Kafka replay needed. With incremental checkpoints, only the delta since the last snapshot is uploaded and downloaded. Recovery time is proportional to checkpoint size and storage throughput, not to the total amount of data ever processed.

For state stores exceeding ~20GB, Flink's checkpoint recovery is consistently faster than Kafka Streams changelog replay. Below that threshold, the operational simplicity of Kafka Streams often wins.

## Exactly-Once Semantics

**Kafka Streams EOS:** uses Kafka's native transactions to atomically commit produced output records, state store changelog updates, and consumer offset commits as a single transaction. Enable with `processing.guarantee=exactly_once_v2` (Kafka 2.6+). See `03-Data-Production/transactional-producers.md` for the underlying mechanism.

**Flink EOS:** coordinates checkpoints with two-phase commit sinks. Output records are held in a pre-commit buffer; they are flushed to the sink only after the global checkpoint succeeds. If the job fails before checkpoint completion, the output is not committed and the job restarts from the previous checkpoint — no duplicates. This requires the sink to support 2PC (Kafka sink does natively; JDBC and other sinks require careful implementation).
