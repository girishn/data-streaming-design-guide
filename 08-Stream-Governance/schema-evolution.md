# Schema Evolution — Compatibility Modes and Registry Practices

## The Wire Format

Every record produced through a Confluent Schema Registry client carries a **5-byte header** prepended to the serialised payload:

```
Byte 0:    0x00  (magic byte — signals Confluent wire format)
Bytes 1-4: schema ID (big-endian 32-bit integer)
Bytes 5+:  serialised payload (Avro, Protobuf, or JSON Schema)
```

The broker treats everything as untyped byte arrays — it has no awareness of the magic byte or schema ID unless broker-side validation is explicitly enabled (see `08-Stream-Governance/broker-side-validation.md`). Schema enforcement is a client-side contract enforced by the serialiser/deserialiser pair. This is why a misconfigured producer can write raw JSON to an Avro topic and the broker will accept it — the protection layer sits at the client, not the broker, by default.

Consumers fetch the schema ID from the header, look up the schema in the Registry (cached locally after first fetch), and use it to deserialise the payload. This means consumers can deserialise records written by any producer version as long as the schemas are compatible.

## Compatibility Modes

The Schema Registry enforces compatibility between schema versions on registration. The mode is set per subject (not globally, though a global default applies).

| Mode | What it guarantees | Upgrade order |
|---|---|---|
| **BACKWARD** | New schema can read data written with the immediate prior version | Consumers first |
| **FORWARD** | Data written with the new schema can be read by consumers using the immediate prior version | Producers first |
| **FULL** | Both BACKWARD and FORWARD simultaneously | Either order |
| **BACKWARD_TRANSITIVE** | New schema can read data from all previously registered versions | Consumers first |
| **FORWARD_TRANSITIVE** | New schema data readable by all previously registered consumer versions | Producers first |
| **FULL_TRANSITIVE** | BACKWARD_TRANSITIVE + FORWARD_TRANSITIVE | Either order |
| **NONE** | No compatibility check — any schema accepted | — |

**TRANSITIVE vs non-transitive:** non-transitive modes validate only against the most recent version. If a consumer is two versions behind, BACKWARD does not guarantee it can read the latest. BACKWARD_TRANSITIVE validates against every registered version — a consumer running any historical version can read the newest data. Use TRANSITIVE modes for long-lived systems where consumers may lag significantly behind producers.

**NONE** disables checks entirely. Use only for topics in active development or where the team explicitly owns all producers and consumers. Never use in production for shared topics.

## Safe vs Breaking Changes

**Avro:**

| Change | BACKWARD safe | FORWARD safe |
|---|---|---|
| Add optional field with default | Yes | No (old consumers ignore it; old producers won't write it) |
| Delete optional field with default | No (old data has it; new schema lacks it) | Yes |
| Type widening (`int → long`) | Yes (via union types or schema aliases) | Depends |
| Rename field (without alias) | No | No |
| Rename field (with alias) | Yes | Yes |
| Add required field (no default) | No | No — breaking |
| Change field type | No | No — breaking |

In Avro, the `aliases` field is the escape hatch for renames. Add the old name as an alias in the new schema — the deserialiser maps the old field name to the new one during reads.

**Protobuf:**

Protobuf compatibility is governed by **field tags**, not field names. Field names can change freely; field tags cannot be reused.

| Change | Safe |
|---|---|
| Add new field (new unique tag) | Yes |
| Delete field, mark tag as `reserved` | Yes |
| Rename field | Yes (tags are the identity, not names) |
| Reuse a deleted field's tag for a different type | No — breaking, silently corrupts data |
| Change field type | No — breaking |

Always use the `reserved` keyword for deleted field tags and names to prevent accidental reuse:
```protobuf
message Order {
  reserved 3, 4;
  reserved "old_field_name";
  string order_id = 1;
}
```

## Upgrade Sequencing

The compatibility mode determines which side of the pipeline must be updated first:

**BACKWARD — upgrade consumers first:** the new schema can read old data, but old schemas cannot read new data. Consumers must be on the new schema before producers start writing it — otherwise consumers reading new-format records with old schemas will fail to deserialise.

**FORWARD — upgrade producers first:** old schemas can read new data. Producers can start writing the new format before consumers upgrade — consumers on the old schema will still be able to read it.

**FULL — either order:** both sides can be upgraded independently. Preferred for teams with independent release cycles.

## Subject Naming Strategies

The naming strategy determines how producers and consumers identify the Schema Registry subject for a given topic.

**`TopicNameStrategy` (default):** subject name = `{topic-name}-value` (or `-key`). One schema type per topic. Enforces that all producers to a topic write the same record type. Use for topics with a single, well-defined event type.

**`RecordNameStrategy`:** subject name = fully qualified record class name (e.g., `com.company.events.OrderPlaced`). Multiple record types can share a single topic, each with its own independent evolution history. Use for event bus topics that carry heterogeneous event types. Consumers must handle type dispatch.

**`TopicRecordNameStrategy`:** subject name = `{topic-name}-{record-class-name}`. Scopes the record schema to a specific topic — the same record class used in two different topics has independent evolution histories per topic. Use when the same DTO is used across domains but must evolve independently.

## Registry Best Practices

**Disable auto-registration in production:**
```properties
auto.register.schemas=false
use.latest.version=false
```
Auto-registration allows any producer to push a new schema version on first use. In production, unreviewed schemas reaching the broker can cause immediate consumer failures. Register schemas explicitly through a CI/CD gate.

**Pre-register via CI/CD:** use the Maven or Gradle Schema Registry plugin to validate and register schemas as part of the build pipeline. A failed compatibility check in CI is far cheaper than a failed deployment.

**Enable schema normalisation:** schemas that are semantically identical but differ in whitespace, field ordering, or formatting are treated as distinct versions without normalisation. Enable it at the registry level to prevent version proliferation from formatting-only changes.

**Set compatibility mode per subject, not just globally:** the global default is a fallback. High-stakes topics (payment events, audit logs) should have FULL_TRANSITIVE set explicitly so the constraint is not accidentally overridden by a global config change.

**Prefer BACKWARD as the default:** most distributed microservice evolution follows the pattern of adding capabilities while maintaining existing consumers. BACKWARD supports this naturally and the upgrade order (consumers first) is operationally safer than FORWARD (producers first — producing data consumers cannot yet read).
