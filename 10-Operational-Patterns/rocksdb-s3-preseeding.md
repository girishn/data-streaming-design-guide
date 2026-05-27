# RocksDB S3 Pre-Seeding — Fast State Recovery for Kafka Streams

## The Cold-Start Problem

Kafka Streams maintains local state stores backed by RocksDB. The changelog topic is the source of truth — when local storage is lost (host failure, ephemeral container restart, EBS volume replacement), the application rebuilds its state by replaying the compacted changelog from the beginning.

For small state stores (tens of MB, millions of keys), changelog replay takes seconds. For large state stores — item mapping services, recommendation engines, enrichment caches — rebuild time scales with state size, not wall-clock time:

| State size | Approximate rebuild time |
|---|---|
| < 1 GB | Seconds to minutes |
| 10 GB | 10–20 minutes |
| 50 GB | 45 minutes – 1.5 hours |
| 300M+ keys | 1.5 – 2+ hours |

During rebuild, the application either serves stale state or is completely unavailable. For services with sub-minute RTO requirements, changelog replay is not a viable recovery mechanism at scale.

## The Pre-Seeding Pattern

S3 pre-seeding treats object storage as a warm backup of the local RocksDB state. Recovery becomes: download the backup → extract it → replay only the small delta of changes since the backup was taken. This reduces recovery time from hours to minutes or seconds depending on backup recency.

### Step 1 — Access the RocksDB Handle

Kafka Streams does not expose a public API for RocksDB snapshotting. Access requires Java reflection to reach the underlying RocksDB instance:

```java
// Access the RocksDB instance handle from a Kafka Streams state store
// The 'db' field is stable across Kafka Streams 3.x
Field dbField = RocksDBStore.class.getDeclaredField("db");
dbField.setAccessible(true);
RocksDB rocksDb = (RocksDB) dbField.get(stateStore);
```

This is the established workaround for this limitation. The reflection target (`RocksDBStore.db`) has been stable across the 3.x release line, but Java reflection against private fields breaks silently on upgrades — the field may be renamed or inlined without notice. Wrap this access in a startup smoke-test that validates the field is reachable before the application starts processing, so a Streams version upgrade that breaks the reflection fails fast at deploy time rather than at recovery time:

```java
// Smoke-test at application startup — call before streams.start()
static void validateRocksDbReflection() {
    try {
        Field dbField = RocksDBStore.class.getDeclaredField("db");
        dbField.setAccessible(true);
    } catch (NoSuchFieldException e) {
        throw new IllegalStateException(
            "RocksDBStore.db field not found — Kafka Streams upgrade may have changed " +
            "internal field names. Update the pre-seeding reflection access before running.", e);
    }
}
```

### Step 2 — Create a Point-in-Time Snapshot

RocksDB's native `Checkpoint` API creates a consistent snapshot of all SST files at a given point in time, without pausing writes:

```java
import org.rocksdb.Checkpoint;

Path snapshotDir = Paths.get("/tmp/rocksdb-snapshot-" + System.currentTimeMillis());
try (Checkpoint checkpoint = Checkpoint.create(rocksDb)) {
    checkpoint.createCheckpoint(snapshotDir.toString());
}
// snapshotDir now contains a consistent set of SST files + MANIFEST
```

The checkpoint is a hard-link tree — fast to create, does not copy data on disk.

### Step 3 — Upload to S3

Package the snapshot and upload with the current changelog offset so recovery knows where to resume:

```java
// Write the last processed offset alongside the snapshot
Path offsetFile = snapshotDir.resolve(".checkpoint");
Files.writeString(offsetFile, String.valueOf(lastProcessedOffset));

// Tar and upload
String tarPath = snapshotDir + ".tar.gz";
new ProcessBuilder("tar", "-czf", tarPath, "-C", snapshotDir.toString(), ".")
    .start().waitFor();

s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-stream-state-backups")
    .key("store/" + storeName + "/latest.tar.gz")
    .build(),
    Path.of(tarPath));
```

Run this from a background thread every few minutes during normal processing, and as a shutdown hook to capture the final state before the process exits.

### Step 4 — Pre-Seeded Startup

On cold start, before Kafka Streams initializes, a pre-initialization script downloads and extracts the backup:

```bash
#!/bin/bash
STORE_DIR=/var/kafka-streams/state/my-app/my-store
mkdir -p $STORE_DIR

# Download latest backup
aws s3 cp s3://my-stream-state-backups/store/my-store/latest.tar.gz /tmp/latest.tar.gz

if [ $? -eq 0 ]; then
    tar -xzf /tmp/latest.tar.gz -C $STORE_DIR
    echo "Pre-seeding complete. Starting Kafka Streams."
else
    echo "No backup found. Kafka Streams will rebuild from changelog."
fi
```

When Kafka Streams initializes, it finds existing RocksDB files in the state directory. It reads the `.checkpoint` file to determine the last committed offset and replays only the changelog records written after that offset — the **delta** — rather than the full history.

## When to Use This Pattern

| Condition | Recommendation |
|---|---|
| State < 1 GB | Skip — changelog replay is fast enough |
| State 1–10 GB, persistent volumes | Skip — EBS/PVC survival makes cold starts rare |
| State > 10 GB or 100M+ keys | Consider pre-seeding |
| Ephemeral compute (ECS Fargate, Cloud Run, spot instances) | Pre-seeding is essential — every restart is a cold start |
| RTO requirement < 5 minutes | Pre-seeding required at large state sizes |
| Already committed to Kafka Streams and cannot migrate to Flink | Pre-seeding is the primary mitigation for slow recovery |

## Flink vs Kafka Streams for Large State

Flink handles this problem natively. `EmbeddedRocksDBStateBackend` with incremental checkpoints to S3 or GCS is built into the framework — no reflection, no custom backup scripts, no application-layer offset tracking:

```java
env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // true = incremental
env.getCheckpointConfig().setCheckpointStorage("s3://my-flink-checkpoints/");
env.enableCheckpointing(30_000); // every 30 seconds
```

Flink workers recover by downloading incremental checkpoint files directly from object storage. For state measured in tens or hundreds of GB, Flink's native recovery is faster and operationally simpler than the Kafka Streams pre-seeding workaround.

If your service has a large state footprint and you are still in the framework selection phase, this recovery characteristic is a meaningful argument for Flink over Kafka Streams. If you are committed to Kafka Streams, pre-seeding is the established pattern — the operational complexity (reflection access, S3 lifecycle management, startup script integration) is the cost of the framework's architecture. See `06-Stream-Processing/kafka-streams-vs-flink.md` for the full comparison.

## Operational Notes

**S3 lifecycle policies:** Backups accumulate. Set a lifecycle policy to expire objects older than N days, keeping only the most recent N snapshots per store.

**Backup frequency vs delta size:** A backup taken every 5 minutes means recovery replays at most 5 minutes of changelog. A backup taken every hour means recovery may replay up to 1 hour of changelog. Trade off backup cost (S3 PUT + transfer) against recovery time (changelog replay window).

**Graceful shutdown integration:** The most valuable backup is the one taken on clean shutdown — it captures the exact committed state and minimizes the delta. Integrate the upload into the JVM shutdown hook:

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    streams.close(Duration.ofSeconds(30));
    uploadSnapshotToS3(stateStore, lastOffset);
}));
```

**Multi-partition deployments:** Each Kafka Streams task owns specific partitions and has its own RocksDB instance. Backup and pre-seed per task, using the partition assignment as the S3 key prefix, so tasks only restore state relevant to their assigned partitions.
