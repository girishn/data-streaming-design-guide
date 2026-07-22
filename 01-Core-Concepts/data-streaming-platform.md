# The Data Streaming Platform Model

This is the third evolution beyond the two architectures most teams try first — a batch pipeline patched with a bolted-on streaming layer (Lambda), then a single unified streaming engine (Kappa). See [lambda-vs-kappa-vs-streaming-platform.md](lambda-vs-kappa-vs-streaming-platform.md) for that comparison and why this model is where Lambda and Kappa both end up converging. What follows here is the detailed mechanics of the model itself.

## Event-Driven Architecture Primitives

An **event** in Kafka is a record with four fields: key, value, timestamp, and optional headers. The broker stores all of these as untyped byte arrays — it is format-agnostic. Schema enforcement happens at the client layer (via Schema Registry), not the broker. This distinction matters: without Schema Registry enforcement, a misconfigured producer can write garbage bytes to a topic and the broker will accept them.

A **topic** is a logical channel. The **partition** is the unit that actually matters for system design — it is the fundamental unit of scalability, parallelism, fault tolerance, and ordering. Topics scale horizontally by distributing partitions across brokers.

**Offsets** are 64-bit integers assigned sequentially within a partition. They are the consumer's primary cursor and the mechanism Kafka uses for group state management. Offsets are meaningless across partitions — only within one.

**Consumer group constraint:** each partition is assigned to exactly one consumer thread within a group at a time. If a topic has N partitions and a group has N+1 consumers, one consumer is permanently idle. This is the hard ceiling on parallelism for a given topic — you cannot scale past the partition count by adding consumers. See `04-Data-Consumption/consumer-groups.md` for rebalancing mechanics.

**Default partitioning:** producers use `murmur2(key) mod N` to select a partition. This is deterministic — all records with the same key land in the same partition, which is how Kafka preserves strict key-level ordering. Null-key records are round-robined (or sticky-batched in newer clients). Changing partition count after topic creation breaks this mapping for existing keys.

**Partition limits:** do not exceed 4,000 partitions per broker or 200,000 partitions per cluster. Beyond these thresholds, metadata management and rebalance latency degrade significantly.

## The Immutable Append-Only Log

Each partition is an append-only, immutable log on disk. Records are written only at the tail; existing records are never modified. This constraint is not accidental — it drives several critical system properties:

**Sequential I/O:** append-only writes exploit sequential disk access patterns, which are orders of magnitude faster than random I/O at scale, whether on spinning disks or when managing high-density SSDs.

**Zero-copy transfer:** because the data on disk does not change, Kafka brokers can use the Linux `sendfile()` system call to transfer segment files directly from the OS page cache to the network interface card, bypassing userspace entirely. This is a primary reason Kafka achieves high consumer throughput without proportional CPU cost.

**Independent replay:** multiple consumer groups consume the same partition independently, each maintaining its own offset. Any group can seek backward and replay from an arbitrary offset without affecting others. This enables backfilling new downstream systems, recovering from application-level processing bugs, and bootstrapping analytics pipelines against existing data.

**Retention as recovery boundary:** topic retention policy (time or size) determines how far back replay is possible. Treat this like a recovery point objective — shorter retention means a narrower recovery window. Log compaction (key-based, not time-based) extends effective retention for stateful topics by keeping only the latest value per key.

**KRaft metadata log:** in modern Kafka (KRaft mode), the cluster metadata — topics, partition assignments, broker memberships — is itself stored as a specialized internal Kafka topic log. Controllers track state changes via log offsets rather than via external ZooKeeper coordination. This enables near-instant controller failover because a standby controller only needs to catch up on the log delta, not rebuild state from an external system. See `02-Broker-Infrastructure/kraft-mode.md`.

## Data-in-Motion as the Primary Substrate

Traditional architectures treat databases as the system of record and streams as a secondary transport. A data streaming platform inverts this: **the topic log is the primary substrate**; everything else is derived from it.

**Design consequence:** when designing a new data flow, the first question is "what is the topic structure and schema?" — not "what table do we need?" Databases become **materialized views**: sinks that project the topic log into a query-optimized shape for a specific application. A database can be dropped and rebuilt by replaying the topic from offset 0, subject to retention. This makes the database ephemeral relative to the topic. Confluent's own pattern catalog names this "Database Write Through" — the event stream is written first and is authoritative; the database is derived from it, not written to directly.

**Producer/consumer decoupling:** producers publish to a topic without any knowledge of downstream consumers. Kafka acts as a durable buffer between them. If a consumer group crashes or is mid-rebalance, records accumulate in the topic — the producer is not blocked and does not experience backpressure. This failure isolation means consumer bugs or slowness do not propagate upstream. The tradeoff: consumer lag becomes the primary operational concern rather than producer throughput.

**Dual-write problem:** a common failure mode is writing to a database and publishing to a topic in separate operations — if one succeeds and the other fails, state diverges. The solution is the **Transactional Outbox Pattern**: write the business record and the event to an outbox table in a single ACID database transaction, then use CDC (Debezium) to stream the outbox log into Kafka. See `10-Operational-Patterns/transactional-outbox.md` and `10-Operational-Patterns/cdc-debezium.md`.

**Stateful processing:** treating the stream as primary enables real-time stateful computation — windowed aggregations, joins across topics, running totals — without batch ETL. Frameworks like Kafka Streams and Flink maintain local state stores (RocksDB) that are continuously updated from the data-in-motion. See `06-Stream-Processing/state-management.md` for the trade-offs between these two approaches.
