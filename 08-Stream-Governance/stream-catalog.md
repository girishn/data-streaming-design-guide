# Stream Catalog — Data Discovery and Business Metadata

## What the Stream Catalog Is

The Stream Catalog is Confluent's data discovery layer — a searchable, organisation-wide index of data assets: topics, schemas, connectors, and the relationships between them. It answers the question "what data do we have and where does it come from?" without requiring engineers to read source code or query broker metadata directly.

The catalog exposes a global search interface and filter facets based on business metadata: data owner, description, business domain, data classification tag, and creation date. Non-technical stakeholders — data analysts, compliance officers, product managers — can discover streams without knowing topic names or schema structures.

## Stream Catalog vs Schema Registry

These two systems are complementary, not redundant:

| | Schema Registry | Stream Catalog |
|---|---|---|
| **Focus** | Technical schema structure and evolution | Business context and discoverability |
| **Primary user** | Engineers writing producers and consumers | Engineers, data analysts, governance teams |
| **Stores** | Avro/Protobuf/JSON Schema definitions, compatibility rules | Tags, descriptions, owners, data quality rules, lineage pointers |
| **Enforces** | Schema compatibility on registration | Tag-based policies and data quality rules |
| **API** | REST (schema CRUD, compatibility checks) | REST + GraphQL (metadata queries, audit) |

Schema Registry is a technical contract layer. Stream Catalog is the governance and discoverability layer built on top of it. Both are required for a complete governance posture — Schema Registry prevents schema breakage; Stream Catalog ensures data assets are understood, owned, and classified.

## Classification Tags and Policy Propagation

Tags in the Stream Catalog are the foundation for downstream governance automation. Tags applied to a schema or topic can trigger:

**Data Quality Rules:** define rules at the tag level that apply to all topics carrying that tag. For example, a `HIGH_SENSITIVITY` tag can require that any topic so classified must have broker-side schema validation enabled and encryption at rest configured.

**Security policies:** tags integrate with Confluent's RBAC system. A `RESTRICTED` tag can trigger access control policies that limit which service accounts can consume from tagged topics.

**Lineage propagation:** when a tagged topic is consumed by a connector or stream processor that writes to a downstream topic, the tag propagates through the lineage graph. Governance teams can see at a glance which downstream applications are consuming PII-tagged data without manually tracing connector configurations.

## Practical Governance Workflows

**Data contract enforcement:** production best practice is to require every topic to have an entry in the Stream Catalog with an assigned owner, description, and data classification before deployment. The catalog's REST and GraphQL APIs make this auditable programmatically — a CI/CD gate can reject deployments for topics without catalog metadata.

**Abandoned stream identification:** topics that have not received messages in a configurable window and have no associated owner are surfaced as candidates for decommissioning. This prevents topic sprawl and the associated storage and metadata management costs.

**Compliance audit preparation:** when a regulator requests evidence of how PII data flows through the organisation, the Stream Catalog (combined with Stream Lineage) provides a queryable record of which topics are tagged as containing PII, who owns them, and where the data moves. Manual documentation becomes a fallback rather than the primary source of truth.

**API access:**
```bash
# List all topics tagged as PII
curl -X GET "https://confluent.cloud/catalog/v1/search/basic?types=kafka_topic&tag=PII"

# Get full metadata for a specific topic
curl -X GET "https://confluent.cloud/catalog/v1/entity/type/kafka_topic/name/{topic-name}"
```

The GraphQL API supports complex queries — "find all topics owned by team X that have no data quality rule configured" — that the REST API cannot express in a single call.
