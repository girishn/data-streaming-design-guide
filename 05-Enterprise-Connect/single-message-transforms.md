# Single Message Transforms — Field-Level Pipeline Transforms

## What SMTs Are and Where They Execute

Single Message Transforms are stateless, record-level operations applied within the Connect worker — no additional infrastructure required. They operate on one record at a time and have no visibility into other records, prior state, or time windows.

Execution position differs by connector direction:

**Source connector pipeline:**
```
External system → [read] → SMT chain → [serialise] → Kafka topic
```
SMTs run after the connector reads the record from the source but before it is serialised and written to Kafka. This means SMTs operate on the connector's internal `SourceRecord` representation, before schema enforcement.

**Sink connector pipeline:**
```
Kafka topic → [deserialise] → SMT chain → [write] → External system
```
SMTs run after deserialisation from Kafka but before the record reaches the sink. Field names and types must match the destination system's expectations after the SMT chain completes.

## Common Built-in SMTs

**Field manipulation:**

| SMT | Use case |
|---|---|
| `ReplaceField` | Rename or drop fields; use `include`/`exclude` lists to whitelist or blacklist columns |
| `MaskField` | Replace field values with a type-appropriate null/zero — PII redaction without removing the field from the schema |
| `InsertField` | Inject metadata into the record: `$timestamp`, `$offset`, `$partition`, `$topic`, or a static value |
| `Cast` | Convert field types (e.g., string → int) for schema compatibility with the destination |

**Structure and routing:**

| SMT | Use case |
|---|---|
| `Flatten` | Collapse nested structs into dot-notation flat fields — required for JDBC sinks that cannot handle nested schemas |
| `RegexRouter` | Rewrite the destination topic name using a regex replacement — route records dynamically based on field values |
| `TimestampConverter` | Convert between timestamp representations (Unix epoch, ISO 8601, database datetime) |

**Re-keying:**

| SMT | Use case |
|---|---|
| `ValueToKey` | Extract one or more fields from the value and set them as the record key — establishes partition routing by entity ID |
| `ExtractField` | Pull a single field out of a struct and use it as the entire key or value |

## Chaining SMTs

SMTs are applied in the order listed in the connector config:

```json
{
  "transforms": "dropPii,addTimestamp,reroute",
  "transforms.dropPii.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.dropPii.fields": "ssn,credit_card",
  "transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addTimestamp.timestamp.field": "ingested_at",
  "transforms.reroute.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.reroute.regex": "(.*)raw(.*)",
  "transforms.reroute.replacement": "$1processed$2"
}
```

Each SMT in the chain executes synchronously in the connector task's hot path — the record cannot be written to Kafka until every transform completes. A slow SMT (e.g., one making an external call or performing heavy string manipulation) directly reduces task throughput.

Monitor chain performance via the JMX metric `kafka.connect:type=connector-task-metrics,connector=<name>,task=<id>` → `transformation-time-ms-avg`. If this metric is high relative to `batch-size-avg` and `offset-commit-avg-time-ms`, the SMT chain is the bottleneck.

## When Not to Use SMTs

SMTs are the right tool for simple, stateless, per-record operations. They are the wrong tool when:

**You need to aggregate:** SMTs cannot compute sums, counts, or any value derived from multiple records. There is no state between invocations.

**You need to join:** SMTs operate on a single record with no access to other topics or streams. Enriching a record with data from a lookup table requires a stream processor.

**You need windowed calculations:** tumbling, hopping, or session windows are not available in SMTs. Any time-based grouping requires Flink or Kafka Streams.

**The transform is conditional on prior records:** SMTs have no memory. Deduplication, rate limiting, and sequence detection all require state that persists across records.

For any of these requirements, route data to an intermediate Kafka topic via a source connector with minimal SMTs, then process with Flink or Kafka Streams before writing to the destination. See `06-Stream-Processing/kafka-streams-vs-flink.md` for the framework selection decision.
