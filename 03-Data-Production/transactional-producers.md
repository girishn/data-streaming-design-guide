# Transactional Producers — Cross-Partition Atomicity

## The Problem Idempotence Does Not Solve

Idempotent producers prevent duplicates within a single partition, but they cannot guarantee that writes to multiple partitions succeed or fail together. The canonical failure case is **consume-transform-produce**: a consumer reads from an input topic, transforms the record, writes output to one topic, and commits the offset to another. If the process crashes between the output write and the offset commit, the output record is in Kafka but the offset is not advanced. On restart, the consumer reprocesses the input and produces a duplicate output.

Transactional producers solve this by wrapping multiple sends and offset commits into a single atomic operation — all writes become visible to read-committed consumers together, or none do.

## The Transactions API

```java
producer.initTransactions();                          // register transactional.id, fetch epoch

producer.beginTransaction();
producer.send(new ProducerRecord<>(outputTopic, ...));
producer.sendOffsetsToTransaction(offsets, groupMetadata); // atomic offset commit
producer.commitTransaction();                         // or abortTransaction() on error
```

**`initTransactions()`** registers the producer's `transactional.id` with the Transaction Coordinator and retrieves the current epoch. This must be called once before any transaction begins. It blocks until the coordinator acknowledges.

**`beginTransaction()`** marks the start of an atomic boundary in the client. No broker RPC is issued at this point.

**`send()`** within a transaction stages records to the involved partitions. The Transaction Coordinator is notified of each new partition the transaction touches.

**`commitTransaction()` / `abortTransaction()`** triggers a two-phase commit. The coordinator first writes a `PREPARE_COMMIT` marker to `__transaction_state`, then writes `COMMIT` (or `ABORT`) markers to each involved partition. Only after the partition markers are written do read-committed consumers see the records.

## Transaction Coordinator and `__transaction_state`

The **Transaction Coordinator** is a broker-side component responsible for the transaction state machine. Every `transactional.id` is assigned to a specific coordinator via:

```
partition = hash(transactional.id) % __transaction_state.partition.count
```

The coordinator that owns that partition is the coordinator for all transactions using that `transactional.id`.

State transitions — `ONGOING`, `PREPARE_COMMIT`, `COMMITTED`, `PREPARE_ABORT`, `ABORTED` — are logged as records to the `__transaction_state` internal topic. This log is the source of truth for transaction recovery: if a coordinator fails mid-commit, its replacement reads the log, determines which transactions were in `PREPARE_COMMIT`, and completes them.

## Zombie Fencing

In distributed systems, a producer instance may be presumed dead (by a scheduler or watchdog) and replaced with a new instance before the old one actually terminates. The old "zombie" instance may still be running and attempting to write — its writes would create inconsistencies.

Kafka prevents this using the `transactional.id` and an **epoch counter**:

1. When a new producer instance calls `initTransactions()` with a given `transactional.id`, the coordinator **increments the epoch** for that ID
2. Any subsequent request from the old instance — which still holds the previous epoch — is rejected with a `ProducerFencedException`
3. The old instance must shut down; it cannot complete its in-flight transaction

The `transactional.id` must be static and unique per logical producer (e.g., one per application instance or partition assignment). Reusing the same `transactional.id` across unrelated producers defeats the fencing mechanism.

## Consumer Read Isolation

Transactions are only useful if consumers respect the commit boundary. Two isolation levels:

**`isolation.level=read_uncommitted`** (default): consumers see all records as they are appended, including records from in-flight and later-aborted transactions. Aborted records are never removed from the log — they remain as physical records with an abort marker. This is the default and appropriate for consumers that do not need transactional guarantees.

**`isolation.level=read_committed`**: consumers skip records belonging to aborted transactions and do not advance past the **Last Stable Offset (LSO)**. The LSO is the lowest starting offset of any currently open transaction. A slow or stuck transaction on one partition can stall all read-committed consumers on that partition at the LSO, even if later partitions are fully committed.

Implication: a long-running transaction or a zombie producer that never commits creates LSO lag that looks like consumer lag but is not resolvable by adding consumer capacity — the root cause is the open transaction.

## Performance Cost and When Not to Use Transactions

**Latency:** each `commitTransaction()` requires multiple sequential RPCs — coordinator write, partition marker writes, coordinator finalization. Added latency is typically **2–5ms** under normal conditions, up to **50ms at p99** under load.

**Throughput:** the two-phase commit overhead and the LSO mechanism reduce sustainable throughput by **10–20%** compared to non-transactional production.

**Do not use transactions when:**

- The workload is stateless telemetry or metrics where duplicates are acceptable and speed is the priority
- The consumer sink is already idempotent — a database upsert keyed on the record ID makes duplicate records harmless; adding transaction overhead buys nothing
- Writing to external systems (S3, JDBC, HTTP) that do not participate in Kafka's two-phase commit — the Kafka transaction commits atomically but the external write does not; you have not achieved end-to-end exactly-once
- The topic has `acks=1` or `acks=0` — transactions require `acks=all` on the brokers involved

For end-to-end exactly-once from Kafka to Kafka (consume-transform-produce), transactions are the correct tool. For end-to-end exactly-once to external sinks, see `10-Operational-Patterns/transactional-outbox.md`.
