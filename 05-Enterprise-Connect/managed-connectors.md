# Managed Connectors — Architecture and Deployment Trade-offs

## Worker Architecture

Kafka Connect separates the logical definition of a job from its execution:

**Connector:** defines what to move and how — source system, topic mapping, serialisation format, and configuration. A connector does not itself move data.

**Task:** the atomic unit of work that executes the actual data movement. One connector spawns one or more tasks. Tasks run inside Connect worker JVMs and are the unit of parallelism.

**Worker cluster:** in distributed mode, multiple workers share the same `group.id`. The Group Coordinator (a Kafka broker) manages task rebalancing across workers the same way it manages consumer partition assignment — if a worker fails, its tasks are redistributed to surviving workers. Worker state (connector configs, task assignments, offsets) is stored in internal Kafka topics (`connect-configs`, `connect-offsets`, `connect-status`).

`tasks.max` sets the upper bound on task parallelism per connector. The connector implementation determines the actual task count — a JDBC source connector for a single table cannot split across more tasks than the table has logical partitions, regardless of `tasks.max`.

## Managed vs Self-Managed

**Confluent Cloud managed connectors:**
- Infrastructure, JVM sizing, scaling, and plugin upgrades are handled by Confluent
- 80+ fully managed connectors available via UI, API, or Terraform
- Billing is consumption-based: task count × throughput
- No plugin classpath management — connectors are versioned and updated by Confluent
- Connector failures trigger automatic task restart with backoff

**Self-managed Confluent Platform / OSS Connect:**
- You provision and size the Connect worker VMs or containers
- JVM heap must be tuned per workload — memory-intensive connectors (large batch sizes, schema caching) require larger heaps
- Plugins are installed manually into `plugin.path`; version conflicts between plugins must be resolved at the classloader level
- OS-level limits apply: `ulimit -n` must be high enough to support the number of open file handles (Kafka producer/consumer clients + connector file handles)
- Plugin upgrades require a rolling worker restart

The operational cost of self-managed Connect is frequently underestimated. Each connector plugin is effectively a third-party dependency that the team owns — bug fixes, version upgrades, and compatibility validation become ongoing work.

## Source vs Sink Delivery Guarantees

**Source connectors** read from an external system and produce to Kafka. The source tracks its read position using offsets stored in the `connect-offsets` internal topic. On restart, the connector resumes from the last committed offset in that topic.

Exactly-once for source connectors is achievable when the connector uses an idempotent producer and transactional writes to atomically commit the source offset and the produced records together. Not all source connectors implement this — check the connector documentation. Without EOS, a worker crash between producing records and committing the source offset results in duplicate records in Kafka on restart.

**Sink connectors** read from Kafka and write to an external system. Kafka offsets for sink connectors are committed to `connect-offsets` after the external write succeeds. This is at-least-once: if the worker crashes after writing to the external system but before committing the Kafka offset, the record is re-read and re-written on restart.

Exactly-once *effects* at the sink require the destination system to support idempotent writes:
- JDBC sink: `insert.mode=upsert` with a primary key — a duplicate row update is a safe no-op
- Elasticsearch: document ID-based upserts
- S3/GCS/ADLS: object keys derived from offset ensure overwrites are idempotent

For sinks that do not support idempotent writes (e.g., HTTP endpoints, stored procedures with side effects), at-least-once is the practical ceiling.

## Connector Lifecycle and Sizing

**`tasks.max`:** size this to the parallelism of the external source, not the number of Kafka partitions. A JDBC source for a table with no natural partition key cannot parallelize beyond one task. An S3 source connector can parallelize to the number of files being ingested. Setting `tasks.max` higher than the external system supports wastes resources without improving throughput.

**Pause and resume:** connectors support PAUSED state for temporary halting without losing configuration or offset state. Use pause during downstream maintenance windows rather than deleting and recreating connectors.

**STOPPED state (Kafka 3.6+):** a stopped connector can have its offsets modified via the REST API — useful for replaying data from an earlier offset or skipping past a poison-pill record without deleting and recreating the connector configuration.

```bash
# Check connector status
curl http://connect-host:8083/connectors/my-connector/status

# Pause a connector
curl -X PUT http://connect-host:8083/connectors/my-connector/pause

# Modify offsets (STOPPED state required)
curl -X PATCH http://connect-host:8083/connectors/my-connector/offsets \
  -H "Content-Type: application/json" \
  -d '{"offsets": [{"partition": {...}, "offset": {...}}]}'
```
