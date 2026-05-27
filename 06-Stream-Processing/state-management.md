# State Management — RocksDB, Checkpoints, and DR Recovery

## RocksDB in Kafka Streams

Kafka Streams uses **RocksDB** as its default persistent state store. RocksDB is an embedded key-value store that writes to local disk using a Log-Structured Merge (LSM) tree. It is not an in-memory store — data that does not fit in the configured cache spills to disk, making it practical for state that exceeds available JVM heap.

**How it works in Kafka Streams:**
- Each state store is backed by a RocksDB instance in a local directory (`state.dir`, default `/tmp/kafka-streams`)
- Writes go to an in-memory write buffer first, then flush to sorted disk files (SSTables) when the buffer fills
- Reads check the block cache first (in-memory), then SSTable files on disk
- A compacted changelog topic in Kafka mirrors every state store write — this is the recovery mechanism

**Sequential I/O:** RocksDB's LSM design turns random writes into sequential disk writes, which is critical for throughput on spinning disks and efficient even on SSDs. LZ4 compression is applied by default to reduce disk footprint.

**Tuning block cache and write buffers:**

```java
RocksDBConfigSetter implementation:

options.setBlockBasedTableFormatConfig(
    new BlockBasedTableConfig()
        .setBlockCache(new LRUCache(256 * 1024 * 1024L))  // 256MB read cache
        .setBlockSize(16 * 1024)                           // 16KB blocks
);
options.setWriteBufferSize(64 * 1024 * 1024L);   // 64MB per write buffer
options.setMaxWriteBufferNumber(3);               // 3 write buffers in memory
options.setLevel0FileNumCompactionTrigger(4);
```

For workloads with 300M+ keys: start with a 256MB block cache and 3 write buffers of 64MB each. Monitor `rocksdb.block-cache-miss-count` via Kafka Streams metrics — a high miss rate indicates the block cache is undersized for the working set.

**Write buffer sizing:** each write buffer is flushed to an L0 SSTable when full. If the flush rate cannot keep up with write rate, RocksDB stalls writes. `max.write.buffer.number` controls how many buffers can accumulate before stalling — increasing it gives more buffer headroom at the cost of more memory.

## Kafka Streams Changelog and the DR Rebuild Problem

Every write to a Kafka Streams state store produces a record to a corresponding changelog topic (e.g., `my-app-my-store-changelog`). This topic is compacted — only the latest value per key is retained. On restart or failover, a new instance replays the changelog to rebuild its local RocksDB state.

**The problem:** changelog replay is bounded by network throughput, not local disk speed. For a state store with 100GB of compacted data, rebuilding over a 1Gbps network takes at minimum several minutes and in practice often 45 minutes to 2 hours when the changelog is heavily compacted or the broker is under load. During this rebuild, the instance is not processing new records and consumer lag accumulates on all partitions assigned to it.

The S3 pre-seeding pattern solves this. Details and implementation are in `10-Operational-Patterns/rocksdb-s3-preseeding.md`.

## Flink State Backends

Flink offers two state backend options, chosen at the job level:

**`HashMapStateBackend`**
- Stores state as Java objects in JVM heap memory
- Fastest access — no serialisation overhead for reads and writes
- State size is bounded by heap; large state causes GC pressure and OOM
- Checkpoints serialise heap objects to the checkpoint storage — checkpoint write time is proportional to state size
- Use for: small to medium state (< a few GB), latency-sensitive jobs where serialisation overhead matters

**`EmbeddedRocksDBStateBackend`**
- Stores state in RocksDB on local disk of each TaskManager
- State can exceed JVM memory — practical for very large state (tens to hundreds of GB per TaskManager)
- Reads and writes go through RocksDB serialisation — adds latency vs heap storage
- Enables **incremental checkpoints** — only the RocksDB SSTable files changed since the last checkpoint are uploaded, dramatically reducing checkpoint write time for large state
- Use for: large state, TB-scale jobs, or any job where state must survive TaskManager restarts reliably

Configure in the job:
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // true = incremental
```

## Flink Checkpointing

Checkpoints are periodic, asynchronous snapshots of all operator state in a running Flink job. They implement the Chandy-Lamport distributed snapshot algorithm — a checkpoint barrier flows through the job graph, and each operator snapshots its state when the barrier passes through.

**Checkpoint storage:** snapshots are written to a durable distributed store:
```java
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/flink-checkpoints/");
```

S3, GCS, HDFS, and Azure Blob Storage are supported. The checkpoint directory must be accessible by all TaskManagers.

**Checkpoint interval:** the trade-off between recovery time objective (RTO) and checkpoint overhead:

| Interval | RTO on failure | Overhead |
|---|---|---|
| 10s | ~10s of data reprocessed | High — continuous I/O |
| 30s | ~30s of data reprocessed | Moderate |
| 60–120s | 1–2 min of data reprocessed | Low |

For most production jobs, **30–60 seconds** balances RTO against overhead. Jobs with very large state and incremental checkpoints can use longer intervals because each checkpoint upload is small.

**Incremental checkpoints** (RocksDB backend only): only upload RocksDB SSTable files that changed since the last successful checkpoint. For TB-scale state, this reduces checkpoint time from hours to seconds per interval — the full state is rebuilt on recovery by downloading the base snapshot plus all incremental deltas.

```java
// Minimum checkpoint configuration for production
env.enableCheckpointing(30_000); // 30s interval
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000); // prevent overlap
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // tolerate transient failures
```

**Savepoints** are manually triggered snapshots used for planned upgrades, job migrations, and A/B deployments. Unlike checkpoints (which are internal and may be garbage collected), savepoints are retained until explicitly deleted and are portable across job versions.
