# Exactly-Once Semantics — The Full Protocol

## The Three Delivery Guarantees

**At-most-once:** the producer does not retry on failure. If the broker does not ACK, the record is lost. The consumer commits offsets before processing — if it crashes mid-processing, records are skipped on restart. Use when speed matters more than completeness: operational metrics, high-frequency telemetry where occasional gaps are acceptable.

**At-least-once:** the producer retries until it receives an ACK. The consumer commits offsets only after processing completes. Records may be processed more than once — a crash between processing and offset commit causes reprocessing on restart. This is the default for most Kafka applications and is correct when the downstream system is idempotent.

**Exactly-once:** every record is processed precisely once, even across producer retries and consumer failures. Requires coordinated protocol support across producers, brokers, and consumers. Not a configuration toggle — it is a holistic guarantee that requires all three layers to be correctly configured.

## The Full EOS Stack

Exactly-once in Kafka requires three components working together. Omitting any one breaks the guarantee:

**1. Idempotent producers** prevent duplicates caused by producer retries within a session. The broker deduplicates using `(PID, partition, sequence_number)`. This alone is not sufficient — it does not coordinate across partitions or survive producer restarts. See `03-Data-Production/idempotent-producers.md`.

**2. Transactions** provide atomic multi-partition writes. Records sent within a transaction are either all visible to consumers or none are — there is no partial commit. The `transactional.id` links sessions so that a restarted producer continues the same logical transaction identity, enabling zombie fencing. See `03-Data-Production/transactional-producers.md`.

**3. Read-committed consumers** (`isolation.level=read_committed`) see only records from committed transactions. In-flight records from ongoing transactions and records from aborted transactions are hidden. Without this, a consumer reads records that may later be aborted — the consumer's processing is based on data that is retracted.

```
EOS guarantee =
  idempotent producer (no retry duplicates)
  + transactions (atomic multi-partition)
  + read_committed consumer (no aborted data)
```

## Transaction Coordinator Two-Phase Commit

The Transaction Coordinator is a broker-side component that owns the lifecycle of every transaction touching its assigned `transactional.id`. State is durably logged to `__transaction_state`.

**State transitions:**

```
initTransactions()
    → [coordinator] register transactional.id, increment epoch
    → state: EMPTY

beginTransaction() + send() calls
    → [coordinator] record partitions involved
    → state: ONGOING

commitTransaction()
    → [coordinator] write PREPARE_COMMIT to __transaction_state   ← 2PC phase 1
    → [coordinator] write COMMIT markers to all involved partitions ← 2PC phase 2
    → [coordinator] write COMMITTED to __transaction_state
    → state: COMPLETE

abortTransaction()
    → same flow with PREPARE_ABORT / ABORT markers
```

**Crash recovery:** if the coordinator crashes between PREPARE_COMMIT and writing partition markers, the successor coordinator reads `__transaction_state`, finds the transaction in PREPARE_COMMIT, and completes the commit by writing the missing partition markers. Transactions in ONGOING state at coordinator crash are eventually aborted after the `transaction.timeout.ms` expires.

**The COMMIT marker:** a special record written to each partition involved in the transaction at the offset immediately after the last transactional record. Read-committed consumers advance past transactional records only after seeing this marker — it is the signal that the transaction is complete and records are safe to consume.

## Zombie Fencing with Producer Epoch

A zombie producer is an application instance that was presumed dead — by a Kubernetes scheduler, a watchdog, or a session timeout — and replaced, but is still running and attempting to write.

Without fencing, the zombie and the replacement both hold the same `transactional.id` and can produce records concurrently. The transaction coordinator would have two producers racing to commit — one will succeed and one will leave orphaned records.

**Fencing mechanism:**

1. Each `transactional.id` has an associated epoch stored at the coordinator
2. When a new producer calls `initTransactions()`, the coordinator increments the epoch
3. All subsequent requests from the new producer include the new epoch
4. Any request from the zombie carrying the old epoch is rejected with `ProducerFencedException`
5. The zombie must shut down — it cannot complete any pending transaction

The epoch is also used at the broker level via the leader epoch mechanism (see `07-Advanced-Reliability/replication-isr.md`) to prevent writes from a zombie that bypassed the coordinator.

## Consume-Transform-Produce with EOS

The canonical exactly-once pattern in Kafka: read from an input topic, transform, write to an output topic, and advance the consumer offset — all as a single atomic unit.

```java
consumer.subscribe(inputTopics);
producer.initTransactions();

while (true) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));

    producer.beginTransaction();
    try {
        for (ConsumerRecord<K, V> record : records) {
            ProducerRecord<K, V> output = transform(record);
            producer.send(output);
        }
        // Atomically commit offsets AND output records
        producer.sendOffsetsToTransaction(
            currentOffsets(records),
            consumer.groupMetadata()   // includes generation ID for fencing
        );
        producer.commitTransaction();
    } catch (Exception e) {
        producer.abortTransaction();
    }
}
```

**`sendOffsetsToTransaction`** writes the consumer offsets to `__consumer_offsets` as part of the same transaction as the output records. If the producer crashes before `commitTransaction()`:
- The output records are not visible (transaction aborted by coordinator after timeout)
- The consumer offsets are not advanced (they were part of the aborted transaction)
- On restart, the consumer re-reads the same input records and re-produces the same output

This is exactly-once: no output is emitted more than once, and no input is silently skipped.

## EOS Boundaries and Limitations

**External sinks:** Kafka's exactly-once protocol is a Kafka-to-Kafka guarantee. Writing to a JDBC database, an HTTP endpoint, or an S3 bucket is outside the transaction boundary. The Kafka transaction commits, but the external write is a separate operation — if it fails, the Kafka offset is advanced but the external write did not happen.

For exactly-once *effects* at external sinks:
- **Idempotent sinks:** design the external write to be a safe no-op on replay — SQL UPSERT with a primary key derived from the Kafka offset, S3 object key based on partition + offset
- **Transactional Outbox:** use a database transaction to atomically write business data and an outbox event, then stream the outbox via CDC — see `10-Operational-Patterns/transactional-outbox.md`

**Read-uncommitted consumers break the guarantee:** if any consumer in the pipeline uses `isolation.level=read_uncommitted` (the default), it reads records from in-flight transactions that may later be aborted. The end-to-end exactly-once guarantee only holds when every consumer between producer and final sink uses `read_committed`.

**Exactly-once is not free:** transactions add 2–5ms latency per commit, reduce throughput by 10–20%, and require all involved brokers to support the transactions API (Kafka 0.11+). For workloads where at-least-once with idempotent sinks achieves the same effective guarantee with lower overhead, prefer that approach.
