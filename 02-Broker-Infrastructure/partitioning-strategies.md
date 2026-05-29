# Partitioning Strategies — Sizing, Replication, and Compaction

## Sizing Partition Count

The partition is the unit of parallelism. Every design decision about partition count is ultimately about two things: throughput capacity and consumer concurrency ceiling.

**Sizing formula:**

```
partitions = max(target_throughput / per_partition_throughput, expected_max_consumers)
```

`per_partition_throughput` is empirically determined for your hardware and message size — a reasonable starting estimate is 10 MB/s for writes on standard broker hardware.

**Practical bands:**

| Sustained throughput | Partition range |
|---|---|
| < 10 MB/s | 3–6 |
| 10–100 MB/s | 6–30 |
| 100–500 MB/s | 30–100 |

**Consumer parallelism ceiling:** a consumer group can have at most one active consumer thread per partition. Adding consumers beyond the partition count produces idle instances. If you expect to scale a consumer group to 20 threads, the topic needs at least 20 partitions. This is the constraint that most often drives partition count higher than throughput alone would suggest.

**Broker limits:** keep to 4,000 partitions per broker; KRaft supports up to 2 million partitions per cluster — see `02-Broker-Infrastructure/kraft-mode.md`. The per-broker limit is driven by JVM heap (roughly 1–2 MB per partition replica) and OS file handle exhaustion.

**Partition count is append-only:** you can increase partitions on an existing topic but cannot decrease them. Increasing partitions after the topic has data breaks the `murmur2(key) mod N` mapping — keys that previously landed on partition X will now land on a different partition. If strict key-based ordering across the full topic history matters, changing partition count is a breaking change that requires topic recreation and consumer replay.

## Replication Factor and ISR

**Baseline:** RF=3 for all production topics. This tolerates a single broker failure without data loss and keeps a leader available when one follower is catching up.

**Durability configuration that must accompany RF=3:**

```
min.insync.replicas = 2      # at the topic or broker level
acks = all                   # at the producer level
```

With this combination, a write is only acknowledged after at least 2 replicas have persisted it. If only 1 replica is in-sync (e.g., 2 brokers are down), producers block — the cluster refuses the write rather than accepting data that could be lost. This is the correct trade-off for production; if you need availability over durability, lower `min.insync.replicas` explicitly and document why.

**Unclean leader election:** set `unclean.leader.election.enable=false`. If all in-sync replicas are unavailable and an out-of-sync replica becomes leader, it will have already-committed messages missing from its log. Those writes are permanently lost from the consumer's perspective. Keeping unclean election disabled means the partition stays offline until an in-sync replica recovers — availability drops but consistency holds.

For deeper ISR mechanics — MISR configuration, leader epoch tracking, replica lag tuning — see `07-Advanced-Reliability/replication-isr.md`.

## Log Compaction vs Retention

These are mutually exclusive cleanup policies per topic. Choose based on what downstream consumers need.

**Retention** (`cleanup.policy=delete`) removes entire log segments based on a time threshold (`retention.ms`) or size threshold (`retention.bytes`). Use this for event streams where historical records beyond a window have no value — application logs, metrics, audit trails. Retention is simple and predictable.

**Compaction** (`cleanup.policy=compact`) keeps only the most recent record for each key, indefinitely. Use this for topics that represent current state rather than a sequence of events — user profile updates, configuration state, CDC change events where only the latest row value matters. Compaction is what makes a Kafka topic usable as a durable key-value store.

**Tombstones:** to delete a key entirely from a compacted topic, produce a record with that key and a `null` value. The compaction cleaner will eventually remove all records for that key including the tombstone itself (after `delete.retention.ms`). Until the cleaner runs, the tombstone is visible to consumers — they must handle null values.

**Compaction lag:** compaction runs asynchronously in a background cleaner thread. A consumer replaying a compacted topic from offset 0 may encounter multiple versions of the same key before the cleaner has processed recent segments. Do not assume a single pass through a compacted topic yields one record per key — it guarantees only that the *last* record for each key is preserved after compaction completes.

**Mixed policy:** `cleanup.policy=compact,delete` is valid — it compacts by key while also enforcing a time/size retention floor. Useful for CDC topics where you want current state but not indefinite history.

## Partitioning Strategy: Key-Based vs Round-Robin

**Key-based (default):** the producer uses `murmur2(key) mod N` to select a partition. All records with the same key land on the same partition in the same order they were produced. This is the only way to guarantee strict ordering for an entity (e.g., all events for a given `user_id` or `order_id` arrive at a consumer in sequence).

The risk is hot partitions: if your key space is unevenly distributed — a small number of keys generate disproportionate traffic — a subset of partitions become throughput bottlenecks while others sit idle. Diagnose with consumer lag per partition; a single overloaded partition cannot be relieved by adding consumers.

**Round-robin / Sticky Partitioner:** used when the producer sends records with a null key. The **Sticky Partitioner** (default since Kafka 2.4) batches records to one partition until the batch is full or `linger.ms` elapses, then switches. This maximises batch efficiency and distributes load evenly across partitions. It provides no ordering guarantees — records for the same logical entity may land on different partitions.

**Choosing:** if downstream consumers need to process all events for an entity in order (joins, state machines, aggregations by key), use key-based partitioning and design keys to be as uniformly distributed as possible. If ordering within a key is irrelevant and throughput matters more, use null keys and let the Sticky Partitioner distribute load.

Custom partitioners are supported but rarely necessary — the common alternative to the default is routing by a compound key (e.g., `tenant_id + entity_id`) to improve distribution while preserving per-entity ordering.

## Concurrent Producer Ordering

Key-based partitioning guarantees that all records for a given key land on the same partition. It does not guarantee ordering when multiple producer instances produce to that partition concurrently.

The problem: two application instances both produce events for `customer_id=12345`. Both use the same key, so both target the same partition. Ordering within the partition is determined by arrival time at the leader broker — network latency, thread scheduling, and GC pauses on either instance can cause their messages to arrive out of sequence regardless of the logical event order.

**Solution 1 — Kafka Streams (preferred for stateful processing)**
Kafka Streams assigns exactly one stream task per partition. Only one thread ever produces to a given output partition for a given key. Concurrent producer interference is structurally eliminated. See `06-Stream-Processing/kafka-streams-vs-flink.md`.

**Solution 2 — Application-level routing (consistent hashing)**
Route processing for a given key to a designated instance before producing. All loyalty engine pods for `customer_id=12345` are routed to the same pod via a consistent hash ring at the load balancer or application router layer. That pod is the sole producer for that key's output partition.

**Solution 3 — Optimistic concurrency in the sink**
Include a sequence number or version field in each event. The consumer's sink (e.g., database) rejects writes where the incoming version is older than the stored version. Ordering at the Kafka layer becomes irrelevant because the sink enforces correct final state. Requires the producer to maintain a monotonic sequence per key.

**Solution 4 — Idempotent producer with sequence numbers**
`enable.idempotence=true` assigns a Producer ID (PID) and incrementing sequence number to every batch. The leader broker rejects a batch whose sequence number is not exactly one greater than the last written — preventing reordering caused by producer retries. This protects against retry-induced reordering from a single producer instance but does not protect against concurrent writes from two distinct instances (each has its own PID and sequence space).

**Decision:** if per-key ordering across all events is a hard requirement (state machines, ledgers, loyalty calculations), use Kafka Streams or application-level routing to ensure a single logical producer per key. Idempotent producers are a durability guard, not a concurrency ordering guard.

## Live Repartitioning — Blue-Green Migration

Increasing partition count on a live topic is irreversible and breaks per-key ordering. The `murmur2(key) mod N` mapping changes — a customer that hashed to partition 7 with 12 partitions hashes to a different partition with 48. Events for the same key exist across two partitions with no ordering relationship between them.

The safe pattern is blue-green topic migration: create a new topic (green) with the target partition count, mirror or dual-write data from the original (blue) into it, drain consumers from blue to zero lag, translate offsets, cut consumers over to green, then decommission blue.

For the full operational procedure — state machine (`CREATED → DUAL_WRITE → DRAINING → CUTOVER → DECOMMISSIONED`), Cluster Linking vs MirrorMaker 2 vs dual-write comparison, offset translation, rollback gates, and platform team responsibilities — see `10-Operational-Patterns/blue-green-topic-migration.md`.
