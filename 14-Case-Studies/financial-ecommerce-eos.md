# Financial E-Commerce Exactly-Once Payment Ledger

## The Problem

An e-commerce payment platform processes millions of transactions per day. The initial architecture has an application that writes a payment record to PostgreSQL and then publishes a `PaymentInitiated` event to Kafka. Under normal conditions this works. Under failure conditions it produces two distinct classes of corruption:

**Silent divergence:** The PostgreSQL write commits. The application crashes before publishing to Kafka. The payment exists in the database but no downstream service — the ledger updater, the fraud scorer, the notification service — ever learns of it. The customer's payment is processed but their order is never fulfilled.

**Phantom events:** The application publishes to Kafka successfully. The PostgreSQL write fails (deadlock, constraint violation). The payment does not exist in the database, but downstream services have already processed the event. The customer is charged for a transaction that was never committed.

Both failure modes are silent — no exception propagates to the customer, no alert fires. They surface hours or days later during reconciliation.

## Entry Point: Transactional Outbox

The fix for the entry point is not a more reliable Kafka producer. It is eliminating the dual-write entirely. The application writes the payment record and a structured event into a **single ACID transaction** against PostgreSQL. Either both commit or neither does:

```sql
BEGIN;

INSERT INTO payments (payment_id, amount, currency, customer_id, status, created_at)
VALUES ('pay_abc123', 9999, 'USD', 'cust_xyz', 'INITIATED', NOW());

INSERT INTO outbox (aggregate_id, aggregate_type, type, version, partition_key, payload)
VALUES (
  'pay_abc123',
  'Payment',
  'PaymentInitiated',
  1,
  'pay_abc123',
  '\xAvroSerializedBytes...'  -- serialized to Avro inside the application
);

COMMIT;
```

Debezium reads the outbox table via the PostgreSQL WAL and relays events to Kafka using the `OutboxEventRouter` SMT. The relay is at-least-once — a Debezium restart after publishing but before committing its offset re-publishes the event. Every downstream consumer must be idempotent. See `10-Operational-Patterns/transactional-outbox.md` and `10-Operational-Patterns/cdc-debezium.md`.

## Payment Lifecycle Events

The payment lifecycle produces four event types on the `payments` topic, all keyed on `payment_id`:

```
PaymentInitiated  → payment received from checkout, pending authorization
PaymentAuthorized → issuing bank approved the charge
PaymentSettled    → funds transferred; ledger entry final
PaymentFailed     → authorization declined or settlement error
```

Each event is an Avro record registered in Schema Registry under `TopicRecordNameStrategy`. Broker-side schema validation is enabled on the `payments` topic — a record missing a required field (e.g., `currency`) is rejected before entering the log. See `08-Stream-Governance/broker-side-validation.md`.

All four event types share `payment_id` as the Kafka message key, so all events for a given payment land on the same partition in commit order. A consumer reading that partition sees the state machine transitions for any payment in strict sequence.

## Transactional Producer for Ledger Updates

The ledger processor consumes from the `payments` topic and writes confirmed entries to the ledger database. It uses the consume-transform-produce pattern with Kafka transactions to make this atomic:

```java
producer.initTransactions();

while (true) {
    ConsumerRecords<String, Payment> records = consumer.poll(Duration.ofMillis(100));

    producer.beginTransaction();
    try {
        for (ConsumerRecord<String, Payment> record : records) {
            if (record.value().getType().equals("PaymentSettled")) {
                // Write to ledger DB with idempotent upsert on payment_id
                ledgerDb.upsert(record.value());

                // Publish LedgerUpdated event
                producer.send(new ProducerRecord<>("ledger-events",
                    record.key(), buildLedgerEvent(record.value())));
            }
        }
        // Commit consumed offsets and produced records atomically
        producer.sendOffsetsToTransaction(
            currentOffsets(records),
            consumer.groupMetadata()
        );
        producer.commitTransaction();

    } catch (Exception e) {
        producer.abortTransaction();
        // Re-seek consumer to last committed offset
    }
}
```

Producer configuration:
```properties
transactional.id=payment-ledger-processor-1   # unique, persistent per instance
enable.idempotence=true
acks=all
max.in.flight.requests.per.connection=5
```

The `transactional.id` enables zombie fencing. If a ledger processor instance hangs and a replacement starts with the same `transactional.id`, the Transaction Coordinator increments the epoch and fences the stale instance — its subsequent produce attempts are rejected with `ProducerFencedException`. See `03-Data-Production/transactional-producers.md`.

## Read-Committed Isolation for Ledger Consumers

The `ledger-events` topic must never expose partial or aborted transaction state. All consumers of this topic — the customer balance service, the accounting export, the fraud detection pipeline — are configured with:

```properties
isolation.level=read_committed
```

Under `read_committed`, the consumer only reads messages belonging to committed transactions. Messages from aborted transactions are filtered out by the client library. The consumer cannot read past the **Last Stable Offset (LSO)** — the lowest offset of any open, uncommitted transaction — which prevents a consumer from reading the first half of a transaction before the commit marker arrives.

In practice: if the ledger processor begins a transaction, writes a `LedgerUpdated` event, then crashes before committing, `read_committed` consumers see nothing. When the replacement processor re-processes the `PaymentSettled` event and commits its transaction, consumers see the `LedgerUpdated` event exactly once.

See `07-Advanced-Reliability/exactly-once-semantics.md` for the full EOS stack.

## Cross-System Idempotency

Kafka's exactly-once guarantee is bounded by the Kafka ecosystem. Once a `LedgerUpdated` event is consumed and written to an external system — a PostgreSQL ledger database, a bank settlement API — Kafka's transaction protocol no longer applies. Two strategies close this gap.

**Tracking table for database sinks:**

```sql
CREATE TABLE processed_payments (
  payment_id   VARCHAR(64) PRIMARY KEY,
  event_type   VARCHAR(64) NOT NULL,
  processed_at TIMESTAMPTZ DEFAULT NOW()
);
```

The sink consumer inserts into `processed_payments` in the same database transaction as the ledger update. A duplicate event produces a primary key conflict — the consumer catches it and discards the duplicate without applying the ledger change.

**Optimistic versioning for balance updates:**

Balance updates must apply in the correct order even if events arrive out of sequence due to consumer restarts. A conditional upsert enforces sequencing:

```sql
UPDATE account_balances
SET balance = balance + :amount,
    version = version + 1,
    updated_at = NOW()
WHERE account_id = :account_id
  AND version = :expected_version;
```

If the update affects zero rows (because `version` does not match), the event is a duplicate or out-of-order arrival — the consumer routes it to a DLQ for manual reconciliation rather than silently applying it.

**External bank settlement API:** The settlement service generates a stable idempotency key from `payment_id` + `event_sequence_number` and passes it as a request header on every API call to the bank. Banks that support idempotency keys (Stripe, Adyen, most modern payment processors) return the original response for duplicate requests with the same key.

## Error Handling and DLQ

Events that fail business rule validation — negative payment amounts, unsupported currencies, missing required fields that passed schema validation but failed domain checks — are routed to a `payments-dlq` topic with full context headers:

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=payments-dlq
errors.deadletterqueue.context.headers.enable=true
```

The DLQ headers include the original topic, partition, offset, exception class, and stack trace. Financial operations teams review DLQ entries before each business day close. DLQ entries are not discarded — they are manual reconciliation items that must be resolved before the day's books close. See `05-Enterprise-Connect/error-handling-dlq.md`.

## Regulatory Audit Trail

**Stream Lineage** maps the full data path: outbox table → `payments` topic → ledger processor → `ledger-events` topic → PostgreSQL balance table → Snowflake financial reporting. Data stewards can trace any specific `payment_id` through every transformation and confirm where the data was consumed and by what service. This satisfies PCI DSS data flow documentation requirements. See `08-Stream-Governance/stream-lineage.md`.

**Audit Logs** capture every administrative action on the Kafka cluster: topic creation, ACL changes, schema registration, consumer group resets. These logs are written to an immutable S3 bucket with object lock enabled. Access patterns are reviewed quarterly for SOC 2 Type II compliance.

**Broker-side schema validation** ensures that no malformed payment record — missing currency code, null amount, invalid enum value — enters the `payments` topic log. This provides a hard guarantee that the event log is internally consistent, which is required for the log to serve as a legally admissible audit record.

## Architecture Summary

```
Checkout Service
  → PostgreSQL (payment + outbox, single ACID transaction)
  → Debezium CDC (WAL reader)
  → payments topic (Avro, broker-validated, keyed on payment_id)

Ledger Processor (transactional consumer-producer)
  → read_committed consumer on payments topic
  → atomic: sendOffsetsToTransaction + commitTransaction
  → ledger-events topic
  → PostgreSQL ledger DB (upsert on payment_id)

Balance Service
  → read_committed consumer on ledger-events
  → conditional UPDATE WHERE version = expected_version
  → PostgreSQL account_balances

Settlement Service
  → read_committed consumer on ledger-events (PaymentSettled only)
  → bank settlement API (idempotency key = payment_id + seq)

Audit and Compliance
  → Stream Lineage: full data provenance graph
  → Audit Logs: all platform admin actions → S3 (object lock)
  → Schema Registry: all event schemas versioned and immutable

Error Path
  → payments-dlq (failed validation, domain rule violations)
  → financial ops review before day-close
```

The broker is the system of record. The PostgreSQL ledger, the account balances, the Snowflake financial reports are all projections of the event log. A balance discrepancy is debugged by replaying the `payments` topic for the affected `payment_id` — the sequence of events is the ground truth, not any derived database state.
