# Data Contracts in Stream Governance

A Data Contract is a formal, versioned agreement between producers and consumers about the structure, semantics, and quality of data on a topic. It extends schema compatibility enforcement with field-level quality rules, schema migration logic, encryption mandates, and ownership metadata — all declared in the Schema Registry and enforced at serialisation and deserialisation time.

Schema compatibility (BACKWARD, FULL_TRANSITIVE) answers: *can this schema version be read by the previous version?* Data contracts answer: *does this record's content meet the agreed quality, security, and structural rules?*

## Components of a Data Contract

### 1. Schema + Compatibility Mode

The foundation is still a schema registered in Schema Registry with a compatibility mode. The contract builds on top — it does not replace compatibility enforcement.

### 2. Data Quality Rules (CEL expressions)

Declarative integrity constraints on field values, evaluated at produce time. Written in **Google Common Expression Language (CEL)**.

```json
{
  "ruleSet": {
    "domainRules": [
      {
        "name": "validateTotalPrice",
        "kind": "CONDITION",
        "type": "CEL",
        "mode": "WRITE",
        "expr": "message.totalPrice > 0",
        "onFailure": "DLQ",
        "doc": "Payment amount must be a positive value"
      },
      {
        "name": "validateCurrency",
        "kind": "CONDITION",
        "type": "CEL",
        "mode": "WRITE",
        "expr": "message.currency in ['AUD', 'USD', 'EUR', 'GBP']",
        "onFailure": "DLQ"
      },
      {
        "name": "validatePaymentId",
        "kind": "CONDITION",
        "type": "CEL",
        "mode": "WRITE",
        "expr": "message.paymentId.matches('^pay_[a-z0-9]{10}$')",
        "onFailure": "ERROR"
      }
    ]
  }
}
```

**Rule modes:**
- `WRITE` — enforced at produce time (most common for quality gates)
- `READ` — enforced at consume time
- `WRITEREAD` — enforced at both

**On-failure actions:**
- `DLQ` — route the record to the configured Dead Letter Queue topic; produce succeeds
- `ERROR` — fail the produce call with an exception; record is not written to the topic
- `NONE` — log the violation; continue

DLQ is preferred for quality rules on production systems — it avoids blocking the producer while preserving the problematic record for investigation. ERROR is appropriate for hard invariants (e.g., a missing required identifier).

### 3. Schema Migration Rules (JSONata)

Declarative field transformation logic for schema evolution. Allows independent upgrade of producers and consumers even across changes that would otherwise be structurally breaking.

```json
{
  "ruleSet": {
    "migrationRules": [
      {
        "name": "renameCustomerIdField",
        "kind": "TRANSFORM",
        "type": "JSONATA",
        "mode": "UPGRADE",
        "expr": "$merge([$, {'customer_id': $.cust_id}])",
        "doc": "cust_id renamed to customer_id in v3"
      },
      {
        "name": "downgradeNewFields",
        "kind": "TRANSFORM",
        "type": "JSONATA",
        "mode": "DOWNGRADE",
        "expr": "$merge([$, {'cust_id': $.customer_id}])"
      }
    ]
  }
}
```

Migration rules run during deserialisation when the consumer is on a different schema version than the producer wrote:
- `UPGRADE` — applied when the consumer schema is newer than the record's schema (reading old data with a new schema)
- `DOWNGRADE` — applied when the consumer schema is older (reading new data with an old schema)

This enables rolling upgrades without coordination. A consumer on v2 can read records written by a v3 producer as long as the DOWNGRADE rule handles the structural difference. Avro field aliases handle simple renames; JSONata migration rules handle transformations Avro aliases cannot express.

### 4. Encryption Mandate (CSFLE via Rules)

Data contracts can declare that specific fields must be encrypted, making CSFLE enforcement part of the contract rather than a producer convention. See `08-Stream-Governance/csfle.md` for full CSFLE detail.

```json
{
  "ruleSet": {
    "domainRules": [
      {
        "name": "encryptCardNumber",
        "kind": "TRANSFORM",
        "type": "ENCRYPT",
        "mode": "WRITEREAD",
        "tags": ["PCI"],
        "params": {
          "encrypt.kek.name": "payments-pci-kek"
        },
        "onFailure": "DLQ"
      }
    ]
  }
}
```

Any field tagged `PCI` in the Avro schema is automatically encrypted at produce and decrypted at consume for authorised consumers. A producer that fails to have KMS access triggers the `onFailure` action — the record is routed to DLQ rather than written with plaintext PCI data.

### 5. DLQ Routing

Contracts declare the DLQ topic per rule. Records that fail quality checks or encryption are automatically routed without the producer needing to implement DLQ logic.

The platform DLQ pattern: the failed record is written to `{topic-name}.dlq` with headers containing the failure reason, rule name, and original topic. Consumers can inspect the DLQ, fix the record, and re-produce to the original topic.

### 6. Ownership and Documentation Metadata

Embedded in the contract — not in the schema itself:

```json
{
  "metadata": {
    "properties": {
      "owner": "payments-platform-team",
      "team-email": "payments@example.com",
      "doc": "Payment initiated event — emitted on every new payment submission",
      "sensitivity": "PCI,PII",
      "sla": "sub-500ms p99 from produce to consumer",
      "data-domain": "payments"
    },
    "tags": {
      "card_number": ["PCI"],
      "customer_name": ["PII"]
    }
  }
}
```

This metadata is surfaced in Stream Catalog for data discovery — teams can search for topics by owner, sensitivity tag, or domain. See `08-Stream-Governance/stream-catalog.md`.

## How the Rules Engine Evaluates

The Rules Engine runs inside the Schema Registry client library — both the serialiser (producer side) and deserialiser (consumer side). It is not a broker-side component.

```
Produce path:
  Producer calls serialise()
    → Rules Engine evaluates WRITE rules in declaration order
    → CONDITION rules: evaluate CEL, route to DLQ or throw on failure
    → TRANSFORM/ENCRYPT rules: transform field values (encrypt PCI fields)
    → Serialise to Avro/Protobuf/JSON bytes
    → Produce to broker

Consume path:
  Consumer calls deserialise()
    → Deserialise bytes
    → Rules Engine evaluates READ rules
    → TRANSFORM/ENCRYPT rules: decrypt fields (if consumer has KMS access)
    → CONDITION rules: evaluate CEL on consumed record
    → Return record to application
```

Because enforcement is client-side, a producer without the Schema Registry client library (or with `auto.register.schemas=true`) bypasses rule evaluation. Broker-side schema validation (`08-Stream-Governance/broker-side-validation.md`) blocks records with an unregistered schema ID but cannot evaluate CEL rules or encrypt fields — those remain client-side responsibilities.

## Data Contracts vs Schema Compatibility — Decision

| Concern | Schema compatibility | Data contracts |
|---|---|---|
| Structural evolution (field add/remove) | Yes — BACKWARD/FULL_TRANSITIVE | Yes — plus migration rules for breaking changes |
| Field value constraints | No | Yes — CEL quality rules |
| Encryption enforcement | No | Yes — CSFLE mandate in rule set |
| DLQ routing for violations | No | Yes — declared per rule |
| Breaking change handling | No (rejected by compatibility check) | Yes — JSONata migration rules |
| Ownership metadata | No | Yes — metadata.properties |
| Stream Catalog discoverability | Schema only | Full contract including metadata and tags |

Use schema compatibility for structural guarantees. Add data contracts when you need field-level quality enforcement, encryption mandates, or migration logic across breaking changes.

## Registering a Contract

```bash
# Register schema with contract via Schema Registry REST API
curl -X POST https://<schema-registry-url>/subjects/<topic>-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "AVRO",
    "schema": "<avro-schema-json>",
    "metadata": {
      "properties": {
        "owner": "payments-platform-team"
      },
      "tags": {
        "card_number": ["PCI"],
        "customer_name": ["PII"]
      }
    },
    "ruleSet": {
      "domainRules": [
        {
          "name": "validateAmount",
          "kind": "CONDITION",
          "type": "CEL",
          "mode": "WRITE",
          "expr": "message.amount > 0",
          "onFailure": "DLQ"
        }
      ]
    }
  }'
```

In a GitOps model, contracts are registered via the Terraform Confluent provider as part of the cluster pipeline — alongside the topic and ACL definitions. See `10-Operational-Patterns/gitops-terraform.md`.

## Cross-References

- CSFLE implementation details — [08-Stream-Governance/csfle.md](csfle.md)
- Crypto-shredding for right-to-erasure — [08-Stream-Governance/pii-tracking.md](pii-tracking.md)
- DLQ for connector errors — [05-Enterprise-Connect/error-handling-dlq.md](../05-Enterprise-Connect/error-handling-dlq.md)
- Stream Catalog and data discovery — [08-Stream-Governance/stream-catalog.md](stream-catalog.md)
- Broker-side schema validation — [08-Stream-Governance/broker-side-validation.md](broker-side-validation.md)
- GitOps and Terraform — [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)
