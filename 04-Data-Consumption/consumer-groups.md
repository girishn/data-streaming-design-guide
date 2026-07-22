# Consumer Groups — Coordination, Assignment, and Offset Management

## Group Coordinator

Every consumer group is managed by a **Group Coordinator** — a designated broker determined by hashing the group ID against the `__consumer_offsets` partition count:

```
coordinator = hash(group.id) % __consumer_offsets.partition.count
```

The coordinator tracks two datasets: the active membership list and the group's committed offsets. When membership changes — a consumer joins, leaves, or misses a heartbeat — the coordinator initiates a rebalance. The coordinator does not compute assignments itself; assignment logic is delegated to the client-side group leader (the first member to join). This makes the assignment strategy pluggable without broker changes.

## Group State Machine

A consumer group moves through five states:

| State | Meaning |
|---|---|
| **Empty** | No active members; committed offsets may still exist |
| **PreparingRebalance** | Coordinator has signalled all members to rejoin; processing stops |
| **CompletingRebalance** | Group leader is computing the new partition assignment |
| **Stable** | All members have received their assignments and are processing |
| **Dead** | Group is being decommissioned; coordinator will expire its offsets |

A group re-enters PreparingRebalance on any membership change: a new consumer joining, an existing consumer leaving cleanly, or a consumer missing its heartbeat deadline.

## Partition Assignment Strategies

The `partition.assignment.strategy` config controls how partitions are distributed across group members. The choice has meaningful operational consequences.

**RangeAssignor** assigns contiguous ranges of partitions per topic in alphabetical consumer order. For a single topic it distributes evenly, but across multiple topics the same consumers always receive the leading ranges — a group consuming five topics can end up with one consumer holding five leading partitions while another holds five trailing ones.

**RoundRobinAssignor** distributes partitions from all subscribed topics evenly across consumers in rotation. Distribution is uniform, but any membership change triggers a full reassignment — every consumer may receive different partitions than it held before, which means local state stores must be rebuilt from scratch.

**StickyAssignor** optimises for stability: it preserves as many existing partition-to-consumer mappings as possible during rebalances and only moves partitions that must change hands. For consumers with local RocksDB state stores, this is the difference between a rebalance that takes seconds and one that takes minutes. See `06-Stream-Processing/state-management.md` for RocksDB rebuild costs.

For Kafka Streams and Flink, the framework controls the assignor internally — do not override it manually.

## Offset Management

Consumer offsets are persisted as records in the internal **`__consumer_offsets`** topic. This topic is compacted — only the latest committed offset per `(group, topic, partition)` is retained. On consumer restart or rebalance, the new owner reads its starting position from this topic.

**Auto-commit** (`enable.auto.commit=true`): the client commits the current offset periodically based on `auto.commit.interval.ms` (default 5s). The commit happens regardless of whether the in-flight records have been processed. If the consumer crashes between the commit and finishing processing, those records are not reprocessed — data loss. Auto-commit is appropriate only for workloads where at-most-once delivery is acceptable.

**Manual sync commit** (`commitSync()`): blocks until the broker acknowledges the commit. Safe but adds per-batch latency. Use when you need the guarantee that an offset is durably recorded before processing the next batch.

**Manual async commit** (`commitAsync()`): non-blocking; issues the commit and continues. If the commit fails, there is no automatic retry — a subsequent successful commit of a higher offset implicitly covers it, but a crash between an async commit failure and the next successful commit can cause reprocessing. Use for high-throughput paths where the occasional duplicate on crash is acceptable.

**The `onPartitionsRevoked` callback** is the correct place to commit offsets before a rebalance transfers partition ownership. If offsets are not committed here, the new owner starts from the last committed position, potentially reprocessing records the previous owner already handled.

```java
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets); // commit before handoff
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // initialise state for newly assigned partitions
    }
});
```

## Application-Level DLQ Pattern

Connect and Kafka Streams have framework-managed DLQ mechanisms (`05-Enterprise-Connect/error-handling-dlq.md`, `06-Stream-Processing/windowing.md`). A hand-written consumer has no equivalent built in — the application must implement the catch-produce-commit sequence itself:

```java
try {
    process(record);
    consumer.commitSync(offsetFor(record));
} catch (RetriableException e) {
    if (attempt < maxRetries) {
        retryWithBackoff(record, attempt + 1); // e.g. 3 attempts before DLQ
    } else {
        producer.send(new ProducerRecord<>("dlq.my-topic", record.key(), record.value()));
        consumer.commitSync(offsetFor(record)); // commit only after the DLQ write succeeds
    }
} catch (NonRetriableException e) {
    producer.send(new ProducerRecord<>("dlq.my-topic", record.key(), record.value()));
    consumer.commitSync(offsetFor(record));
}
```

**Commit only after the DLQ write succeeds**, not before — committing first and then failing to produce to the DLQ loses the record entirely, with no trace it ever failed.

**Retry before DLQ, not instead of it.** A fixed number of retries with backoff (e.g. 3 attempts) absorbs transient failures (a downstream dependency blip) without discarding good records; only route to DLQ once retries are exhausted, so the DLQ reflects genuine failures rather than routine transient noise.

This is the third of three distinct DLQ mechanisms in this guide, none of which are interchangeable: Connect's framework-managed DLQ (config-driven, automatic error headers — `05-Enterprise-Connect/error-handling-dlq.md`), Kafka Streams/Flink's side-output DLQ for late or malformed events during stream processing (`06-Stream-Processing/windowing.md`), and this application-level pattern for hand-written consumers. Confirming which mechanism applies at each hop in a multi-stage pipeline — rather than assuming "DLQ" means one thing — is the actual design decision.

If failures are frequent enough that in-process backoff sleeps start costing main-topic throughput, escalate to `retry-topic-pattern.md` instead of retrying in place.

## Consumer Group Protocol Flow

Every rebalance follows this sequence:

**JoinGroup:** each member sends its topic subscriptions and assignor metadata to the Group Coordinator. The coordinator waits for all members to check in (bounded by `rebalance.timeout.ms`), then elects the first member to join as the group leader and returns the full membership list to it.

**SyncGroup:** the leader runs the assignment algorithm locally and sends the full partition-to-member map back to the coordinator. The coordinator distributes each member's individual assignment. Members that do not receive a SyncGroup response within the timeout re-enter JoinGroup.

**Heartbeat:** after receiving its assignment, each member sends periodic heartbeat RPCs to the coordinator within `session.timeout.ms` (default 45s, minimum 6s). A missed heartbeat causes the coordinator to mark the member as failed and initiate a new rebalance. `heartbeat.interval.ms` controls how frequently heartbeats are sent — set it to roughly one-third of `session.timeout.ms`.

**Poll loop interaction:** the Kafka client sends heartbeats only from the thread calling `poll()`. If `poll()` is not called frequently enough — because processing a batch takes too long — the heartbeat thread starves and the coordinator evicts the consumer. Set `max.poll.interval.ms` to a value that covers your worst-case processing time per batch, and size `max.poll.records` to keep each batch completable within that window.
