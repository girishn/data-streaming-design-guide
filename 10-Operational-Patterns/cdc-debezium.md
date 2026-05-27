# Debezium CDC — Change Data Capture Architecture

## How Debezium Reads the Transaction Log

Debezium is a log-based Change Data Capture engine deployed as a Kafka Connect source connector. Rather than polling the database with periodic queries, it acts as a **logical replication client** that reads the database's internal transaction log directly.

**PostgreSQL:** Debezium uses the `pgoutput` logical decoding plugin, which requires `wal_level = logical` in `postgresql.conf`. It registers as a replication slot, which causes PostgreSQL to retain WAL segments until Debezium confirms it has read them.

**MySQL:** Debezium reads the binary log (binlog) in row format (`binlog_format = ROW`). The connector position is stored as a binlog filename and offset.

**Oracle/SQL Server/MongoDB:** Similar log-based mechanisms — Oracle LogMiner, SQL Server CDC tables, MongoDB oplog.

The key property of log-based CDC is that Debezium reads sequential writes to log files, bypassing the query planner and indexes entirely. This imposes near-zero additional load on the database compared to polling. Events are emitted within milliseconds of commit.

## Snapshot vs Streaming Modes

A connector deployment transitions through two phases:

**Initial snapshot:** When a connector first connects to a database with existing data, it must establish a baseline state before streaming live changes. A naive full-table scan with `SELECT *` is problematic on large tables — it holds locks, strains I/O, and produces a massive burst of events.

**Incremental snapshot** (production default since Debezium 1.6): The connector chunks the table by primary key range and snapshots one chunk at a time while simultaneously streaming live WAL changes. Changes to already-snapshotted rows that occur during the snapshot are captured from the WAL and merged. This avoids lock contention and spreads I/O across hours rather than minutes.

**Streaming mode:** Once the snapshot completes, Debezium transitions to pure streaming. It reads the WAL or binlog continuously, emitting Kafka messages for each committed insert, update, and delete. The connector stores its log position (LSN for PostgreSQL, binlog offset for MySQL) in Kafka Connect's offset storage — typically `__consumer_offsets` or a dedicated offset topic.

## Connector Configuration

Debezium runs as a Kafka Connect source connector. A minimal PostgreSQL configuration:

```json
{
  "name": "postgres-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres.internal",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${file:/opt/kafka/secrets/db-credentials.properties:password}",
    "database.dbname": "orders",
    "database.server.name": "orders-db",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication",
    "table.include.list": "public.outbox",
    "snapshot.mode": "initial",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.payload": "payload",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "https://schema-registry.internal",
    "value.converter.schema.registry.url": "https://schema-registry.internal"
  }
}
```

## Schema Change Handling

Schema changes to the source database are one of the highest-risk operational events in a Debezium deployment.

**Application-side serialization (recommended):** The application serializes the event to Avro or Protobuf _before_ inserting it into the outbox table. Debezium is configured to treat the `payload` column as opaque binary (`BYTEA`) and pass it through without parsing. Schema validation occurs in the application, inside the local ACID transaction. A schema violation fails the business transaction before it reaches the outbox; it cannot produce an invalid record.

**Debezium-side serialization (problematic):** If the application inserts raw JSON and lets the Debezium connector convert it using `JsonConverter`, any malformed record stalls the connector task. Debezium cannot skip past an invalid WAL entry — it must process records in commit order. The connector enters a failed state and stops processing until the bad record is manually resolved or the offsets are reset.

**Subject naming:** Use `RecordNameStrategy` (not the default `TopicNameStrategy`) and disable `auto.register.schemas=false` on the Confluent Schema Registry. See `08-Stream-Governance/schema-evolution.md` for the full compatibility and naming strategy model.

## Delivery Guarantee and Ordering

**At-least-once delivery:** Debezium guarantees that every committed database change is published to Kafka at least once. If the connector crashes after publishing a batch of events but before committing its WAL offset, it re-reads from the last committed offset and re-publishes those events on restart. Consumers must be idempotent. See `10-Operational-Patterns/transactional-outbox.md` for the tracking table pattern.

**Strict ordering:** Because Debezium reads the transaction log sequentially, it preserves the commit order of the source system. All changes to a given row appear in Kafka in the exact order they were committed. When combined with the Outbox Event Router routing by `aggregate_id`, per-entity ordering is preserved on the Kafka topic.

## Production Operational Concerns

**Replication slot accumulation:** PostgreSQL retains WAL segments for every replication slot until the slot consumer confirms it has read them. If the Debezium connector is paused, stopped, or falls behind, the slot causes WAL to accumulate on disk. Unchecked, this fills the disk and takes down the database. Set `max_slot_wal_keep_size` (PostgreSQL 13+) to bound WAL accumulation, and alert on replication slot lag.

**Outbox table growth:** Outbox rows are no longer needed after publication. Without cleanup, the table grows unboundedly. See `10-Operational-Patterns/transactional-outbox.md` for cleanup strategies (Delete SMT, partition truncation, scheduled DELETE).

**Monitoring:**

| Metric | Source | Alert threshold |
|---|---|---|
| Connector task status | Kafka Connect REST API (`/connectors/{name}/status`) | Any task in FAILED state |
| Consumer lag on outbox topic | Kafka consumer group metrics | Sustained lag > operational SLA |
| Replication slot WAL retention | PostgreSQL `pg_replication_slots.wal_status` | `lost` or disk > 80% |
| Snapshot progress | Debezium JMX `snapshot.remaining.scanning.tables` | Snapshot stalled |

```bash
# Check connector status
curl -s http://connect:8083/connectors/postgres-outbox-connector/status | jq .

# Check replication slot health
psql -c "SELECT slot_name, wal_status, safe_wal_size FROM pg_replication_slots;"
```

**Incremental snapshot for large tables:** Do not use `snapshot.mode=initial` on tables with hundreds of millions of rows without incremental snapshot support. Configure `snapshot.mode=initial` with `incremental.snapshot.chunk.size` set to a value that keeps each chunk under a few seconds of query time.
