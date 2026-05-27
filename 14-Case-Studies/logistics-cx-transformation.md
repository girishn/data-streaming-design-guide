# B2B Logistics CX Transformation — Real-Time Shipment Tracking

## The Before State

A B2B logistics platform operates through a web of batch ETL jobs. Shipment status is polled from the TMS every four hours and loaded into a warehouse. Customer portals query the warehouse. A B2B client reporting a missing shipment is looking at data that is up to four hours old. The operations team is reactive — they learn about stuck shipments when clients call.

The architecture that replaces this treats the **broker as the system of record**. Events are the primary substrate; databases are materialized views rebuilt from the event log. The portal's staleness ceiling drops from four hours to seconds.

## Domain Event Design

Three event types drive the shipment lifecycle:

**`ShipmentCreated`** — emitted when a B2B order is initiated. Captured from Salesforce using the Salesforce CDC Source Connector. Contains customer contract metadata, origin/destination, SLA class, and a stable `shipment_id` that serves as the partition key throughout the pipeline. All downstream events for this shipment key on `shipment_id` to preserve per-shipment ordering.

**`LocationUpdated`** — high-frequency events from IoT telematics devices on vehicles. GPS coordinates, timestamp, vehicle ID. Volume is high and variable — a single truck generates one event per 30 seconds; a fleet of 10,000 trucks generates ~20,000 events/minute. These events are not individually significant; they are significant in aggregate and in their absence.

**`DeliveryConfirmed`** — terminal state. Triggers downstream workflows: automated billing, SLA calculation, and closure of the associated Salesforce Case via a sink connector. This event is the one that must never be processed twice.

**Schema governance:** All three event types are registered in Schema Registry under `TopicRecordNameStrategy`. Broker-side schema ID validation is enabled on the ingest topics — a `LocationUpdated` record missing `shipment_id` is rejected at the broker before entering the log. See `08-Stream-Governance/broker-side-validation.md`.

## Real-Time Processing with Flink

Raw events are not what the portal needs. The portal needs enriched, coherent state per shipment. Managed Flink on Confluent Cloud performs two processing roles.

**Stream enrichment (join):** `LocationUpdated` events arrive with a vehicle ID but no customer contract context — that context lives on the `ShipmentCreated` event. Flink maintains a keyed state store per `shipment_id` populated from `ShipmentCreated`. Incoming `LocationUpdated` events are joined against this state to produce enriched tracking records: GPS position + customer contract + SLA class + estimated delivery window. The enriched records are written to a `shipment-tracking-enriched` topic and sunk to PostgreSQL for portal queries using `insert.mode=upsert` on the `shipment_id` key.

**Complex Event Processing — stuck shipment detection:** Flink registers a CEP pattern: if a `shipment_id` has received a `ShipmentCreated` but no `LocationUpdated` in the last 12 hours and no `DeliveryConfirmed`, it is classified as "stuck." The CEP job emits a `ShipmentStuck` alert event to a dedicated alert topic. A sink connector forwards this to the customer's notification endpoint and creates a Salesforce Case automatically. Operations teams are notified before the customer calls.

```
Pattern: ShipmentCreated → [no LocationUpdated for 12h] → ShipmentStuck alert
```

This inverts the operational model: instead of reactive response to client complaints, the platform proactively detects and surfaces anomalies.

## Materialized Views for the Customer Portal

The portal does not query Kafka directly. It queries a PostgreSQL database that is a materialized view of the event log:

```
shipment-tracking-enriched topic
  → Kafka Connect JDBC Sink (upsert on shipment_id)
  → PostgreSQL shipment_status table
  → Portal API reads PostgreSQL
```

The sink uses `insert.mode=upsert` with `pk.mode=record_key` — idempotent writes ensure that connector restarts do not create duplicate rows. If the connector replays a batch of enriched records after a failure, the upsert silently overwrites with the same values.

A second materialized view flows to Snowflake for analytical queries: SLA compliance rates, carrier performance, regional delay patterns. The Snowflake Kafka Connector handles schema evolution automatically against the registered Avro schemas.

## Cross-Cutting Architectural Decisions

Each of the following decisions addresses a specific failure mode that would otherwise surface at scale.

**CooperativeStickyAssignor for portal consumer groups:** During peak shipping season, the portal consumer groups scale out from 8 to 32 instances. With eager rebalancing, every scale-out event triggers a full partition revocation across all consumers — a stop-the-world pause that can last tens of seconds. With `partition.assignment.strategy=CooperativeStickyAssignor`, only the partitions that need to move are revoked; the remaining consumers continue processing without interruption. See `04-Data-Consumption/cooperative-rebalancing.md`.

**Exactly-once semantics for `DeliveryConfirmed`:** A duplicate `DeliveryConfirmed` event would trigger duplicate billing and duplicate SLA credit calculations. The processor that consumes `DeliveryConfirmed` and writes to the billing system uses a transactional producer with `sendOffsetsToTransaction()` — the consumed offset and the billing event are committed atomically. The billing sink uses a tracking table keyed on `shipment_id` to detect and discard any duplicate that reaches it despite the EOS guarantee. See `07-Advanced-Reliability/exactly-once-semantics.md`.

**RocksDB S3 pre-seeding for the enrichment join state:** The Flink enrichment job maintains a keyed state store of active shipments. For a large logistics operator, this is 300M+ active shipment records — 10–15 GB of compressed state. If the Flink job restarts cold, rebuilding this state from the changelog takes 45 minutes to 2 hours. The platform uses S3 pre-seeding (periodic RocksDB snapshot upload) to reduce cold-start recovery to under 5 minutes. See `10-Operational-Patterns/rocksdb-s3-preseeding.md`.

**CEL Identity Pools for microservice authentication:** The logistics platform comprises hundreds of microservices — tracking ingestors, portal APIs, billing processors, connector workers. Managing one service account and API key pair per service is not viable at this scale. Instead, identity pools map JWT claims from Okta directly to Confluent RBAC roles. A new microservice deployment gets Kafka access by acquiring an Okta token with the appropriate group claim — no human creates a Confluent service account, no API key is distributed. See `09-Security-Architecture/cel-identity-pools.md`.

**Static membership for tracking consumer pods:** The `LocationUpdated` consumer group runs as a Kubernetes StatefulSet. Each pod is assigned `group.instance.id=${POD_NAME}`. Rolling restarts do not trigger rebalances — the group coordinator recognises the returning pod by its instance ID and reassigns its previous partitions directly. During a 10-pod rolling restart, zero rebalances occur. See `04-Data-Consumption/static-membership.md`.

## Architecture Summary

```
Salesforce CDC Connector
  → shipment-created topic (Avro, broker-validated)
  → Flink: populate shipment keyed state

IoT Telematics Gateway
  → location-updated topic (Avro, broker-validated)
  → Flink: enrich with shipment state
  → shipment-tracking-enriched topic
  → JDBC Sink Connector (upsert) → PostgreSQL (portal)
  → Snowflake Connector → Snowflake (analytics)

Flink CEP: detect 12h no-update
  → shipment-stuck-alerts topic
  → Notification Sink → customer email/webhook
  → Salesforce Sink → Case creation

delivery-confirmed topic (Avro, broker-validated)
  → EOS transactional consumer
  → Billing system (idempotent sink)
  → Salesforce Sink → Case closure
```

The broker is the system of record. Every state in every downstream system — the portal database, the Snowflake warehouse, the Salesforce Cases — is a projection of the event log. A portal database failure is recovered by re-sinking from the topic. A Snowflake table rebuild replays the topic. The events are the source of truth; the databases are caches.
