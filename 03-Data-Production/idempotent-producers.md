# Idempotent Producers — Single-Partition Duplicate Prevention

## The Duplicate Problem

Standard Kafka producers operate under at-least-once semantics: when a send fails or times out, the producer retries. The failure case that matters is a broker that **successfully persists a record but loses the ACK** before it reaches the producer — a network partition, a broker GC pause, or a timeout between write and response. The producer cannot distinguish this from a failed write, so it retries. The broker appends a second copy. The consumer sees a duplicate.

This is not a bug — it is the correct behaviour for at-least-once delivery. Idempotent producers make exactly-once delivery possible within a single session and a single partition without changing application code.

## The Idempotent Mechanism

Kafka implements producer idempotency using a tracking scheme analogous to TCP sequence numbers.

**Producer ID (PID):** during `initProducer()`, the broker assigns a unique 64-bit PID and an epoch number to the producer instance. This pair identifies the producer session to the broker.

**Sequence numbers:** the producer attaches a monotonically increasing sequence number to every record batch, scoped per `(PID, topic, partition)`. Sequence numbers start at 0 and increment with each batch.

**Broker deduplication:** the broker maintains an in-memory map of `(PID, topic, partition) → sequence highwater mark`. On receiving a batch:
- If `batch.sequence ≤ highwater` → duplicate; broker discards the batch and returns a success ACK to the producer
- If `batch.sequence == highwater + 1` → expected; broker appends and advances the highwater
- If `batch.sequence > highwater + 1` → gap detected; broker throws `OutOfOrderSequenceException`

**Durability of dedup state:** the broker persists the PID-to-sequence mapping in `.snapshot` files on log segment rollover. This means deduplication survives leader reassignments — a new leader reads the snapshot and continues tracking correctly.

## Configuration

```properties
enable.idempotence=true
```

This single setting implicitly enforces three constraints:

| Implicit constraint | Value | Reason |
|---|---|---|
| `acks` | `all` | Durability required for dedup to be meaningful |
| `retries` | `Integer.MAX_VALUE` | Retries must be unlimited for idempotence to hold |
| `max.in.flight.requests.per.connection` | `≤ 5` | Bounds in-flight batches to preserve ordering |

In Kafka 3.0+ clients, `enable.idempotence=true` is the default. Explicitly setting `acks=0` or `acks=1` while also setting `enable.idempotence=true` will throw a configuration exception — the settings are contradictory.

## What Idempotence Does Not Guarantee

**Session scope only:** the PID is allocated at producer startup and lives for the duration of that process. If the producer restarts without a `transactional.id` configured, the broker assigns a new PID and resets sequence numbers from 0. There is no mechanism to link the new session to the old one — duplicates written just before the crash cannot be deduplicated after restart.

**Per-partition only:** deduplication is scoped to `(PID, topic, partition)`. If a producer writes to two partitions, each is tracked independently. There is no cross-partition coordination.

**No cross-partition atomicity:** if a producer writes to partition A and partition B and the process crashes between the two sends, there is no way to roll back the first write. Partition A has the record; partition B does not. Downstream consumers see an inconsistent state. This is the problem transactional producers solve — see `03-Data-Production/transactional-producers.md`.
