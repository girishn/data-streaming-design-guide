# ksqlDB — Streaming SQL on Kafka

## What It Is

ksqlDB is a streaming database built on Apache Kafka that exposes a SQL interface for real-time event processing. Unlike Kafka Streams (an embedded JVM library) or Flink (a separate cluster), ksqlDB runs as a server-side cluster where processing logic is submitted as SQL statements. The runtime manages state, scaling, and fault tolerance — the engineer writes SQL, not topology code.

ksqlDB uses RocksDB for local state storage and backs all state to Kafka changelog topics, giving it the same durability model as Kafka Streams: state survives node failures by replaying changelogs on restart.

## Query Types

**Persistent queries** — continuous, long-running queries that process every record arriving in a topic and either materialise results into a table or emit records to an output topic. These are the primary operational unit in ksqlDB.

```sql
-- Persistent query: continuously aggregate vitals per patient
CREATE TABLE patient_news2_score AS
  SELECT patient_id,
         AVG(heart_rate) AS avg_hr,
         AVG(spo2)       AS avg_spo2,
         COUNT(*)        AS reading_count
  FROM vitals_stream
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY patient_id
  EMIT CHANGES;
```

**Push queries** — subscribe to a stream or table and receive a continuous feed of changes as they occur. Used for client-side real-time dashboards. Not available in all deployment modes.

```sql
-- Push query: stream new results to a client as they arrive
SELECT patient_id, avg_hr FROM patient_news2_score EMIT CHANGES;
```

**Pull queries** — point-in-time lookup of the current state of a materialised table. Behaves like a standard relational SELECT — the client sends a request and receives the current state. This is ksqlDB's primary mechanism for replacing a polling database.

```sql
-- Pull query: current state for a specific patient
SELECT * FROM patient_news2_score WHERE patient_id = 'P-10042';
```

Pull queries require a materialised table (created via a persistent query). They cannot be run directly against a stream.

## Decision Framework: ksqlDB vs Kafka Streams vs Flink

| Dimension | ksqlDB | Kafka Streams | Apache Flink |
|---|---|---|---|
| Interface | SQL | Java/Scala API | Java/Scala/Python API (SQL available) |
| Deployment | Separate server cluster | Embedded in application | Separate cluster |
| State scale | Local disk (RocksDB), moderate | Local disk (RocksDB), moderate | RocksDB or heap; handles TB+ state |
| Windowing | Tumbling, hopping, session | Tumbling, hopping, session, sliding | All types + event-time watermarks |
| Joins | Stream-stream, stream-table | Stream-stream, stream-table, table-table | Full join semantics including interval joins |
| Schema strategy | TopicNameStrategy only | Any | TopicNameStrategy recommended |
| Team skill | SQL-first, no JVM required | JVM, Kafka familiarity | JVM/Python, distributed systems |

**Choose ksqlDB when:**
- The processing logic is expressible in SQL (filtering, enrichment, simple aggregations, joins)
- The team wants to avoid writing and deploying JVM application code
- Real-time materialised views with pull-query access are a primary requirement
- State size is moderate — local RocksDB on ksqlDB nodes is bounded by disk capacity

**Choose Kafka Streams when:**
- Stream processing must be embedded directly into an existing JVM microservice
- The team already operates a Java CI/CD pipeline and wants stream logic version-controlled with application code
- Custom partitioners, interceptors, or Processor API-level control are required

**Choose Flink when:**
- State exceeds what local disk can reasonably hold (TB+ keyed state)
- Complex windowing with event-time and watermark semantics is required (e.g., NEWS2 scoring over 6 async vital streams)
- The team needs join types beyond what Kafka Streams and ksqlDB support (e.g., interval joins, temporal table joins)
- Python or SQL-first data engineering teams are building analytical pipelines

## Schema Strategy Constraint

ksqlDB is incompatible with `RecordNameStrategy` and `TopicRecordNameStrategy`. These strategies allow an unbounded number of record types per topic; ksqlDB cannot resolve which schema applies to a given record at runtime under this model.

**ksqlDB requires `TopicNameStrategy`** — one subject per topic, one schema per subject.

If a topic must carry multiple event types and ksqlDB is in the processing stack, use a **union type** (Avro union, Protobuf oneof) to define a bounded, explicit set of event types under a single `TopicNameStrategy` subject:

```json
// Avro union: single subject, multiple event shapes
[
  "com.hospital.vitals.HeartRate",
  "com.hospital.vitals.BloodPressure",
  "com.hospital.vitals.SpO2"
]
```

This constraint also applies when the downstream of a ksqlDB pipeline is Apache Flink — record-based strategies complicate Flink's schema resolution in a similar way. See `08-Stream-Governance/schema-evolution.md` for the full subject naming strategy decision.

## Operational Monitoring

In production, ksqlDB exposes metrics via JMX and the REST API. Key signals:

**Streaming Units (SUs):** Confluent Cloud's resource unit for ksqlDB. Monitor utilisation to detect query saturation — a query consistently at 100% SU is a scaling signal, not a stability one.

**`ksql.streams.metrics` JMX namespace:** for self-managed ksqlDB, exposes per-query consumer lag, processing rate, and error counts. A persistent query with growing lag is falling behind production rate.

**Query health check:**
```bash
# List all persistent queries and their status
curl -s http://ksqldb-server:8088/queries | jq '.queries[] | {id, state, queryString}'
```

A query in `ERROR` state has halted processing. ksqlDB does not automatically restart failed queries — this must be caught by alerting.

Consumer group rebalancing: ksqlDB uses cooperative rebalancing internally. Scaling a ksqlDB cluster (adding nodes) triggers a rebalance; cooperative rebalancing minimises the pause compared to eager reassignment. See `04-Data-Consumption/cooperative-rebalancing.md`.

## DLQ Integration

ksqlDB does not have native DLQ support equivalent to Kafka Connect. Processing errors (deserialisation failures, null pointer in a UDF) cause a query to enter `ERROR` state by default. To handle bad records gracefully, configure the underlying Streams client:

```properties
ksql.streams.processing.guarantee=exactly_once_v2
ksql.streams.default.deserialization.exception.handler=org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
```

`LogAndContinueExceptionHandler` logs the bad record and continues rather than halting. Route deserialisation failures to a separate error topic via a UDF or a second persistent query for operational visibility.
