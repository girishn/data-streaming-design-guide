# Client-Side Field Level Encryption (CSFLE)

CSFLE encrypts individual sensitive fields within a Kafka record at the producer before serialisation. The ciphertext travels through the broker and is decrypted only by authorised consumers with KMS access. The broker sees encrypted bytes — it cannot read the plaintext values even with administrative access.

This is distinct from full-payload encryption (which makes the entire record opaque) and from transport-layer TLS (which protects in transit but not at rest on the broker). CSFLE protects data at rest, in transit, and in the broker log simultaneously, at field granularity.

## How It Works

CSFLE is implemented through the **Schema Registry Rules Engine**, not as a standalone connector or broker feature. The enforcement is declared in a **Data Contract** on the schema (see `08-Stream-Governance/data-contracts.md`).

```
Producer                Schema Registry            KMS
  │                          │                      │
  │── serialise record ──────►│                      │
  │   Rules Engine evaluates  │                      │
  │   schema's encryption     │                      │
  │   rules                   │── fetch DEK ────────►│
  │                          │◄─ return DEK ─────────│
  │◄── encrypted bytes ───────│                      │
  │                          │                      │
  │── produce to broker ──────────────────────────►  │
```

The **EncryptionExecutor** is the Rules Engine component that acts on encryption rules at serialisation time. It:

1. Inspects the schema's declared encryption rules
2. Identifies fields tagged for encryption
3. Retrieves the Data Encryption Key (DEK) from KMS
4. Encrypts the field values using the DEK
5. Serialises the record — encrypted fields contain ciphertext, not plaintext

On the consumer side, the process reverses. The consumer's deserialiser detects the encrypted fields, retrieves the DEK from KMS (requires IAM permission), decrypts, and hands plaintext to the application.

## Declaring an Encryption Rule in a Schema

Encryption rules are declared as part of the schema metadata in Schema Registry:

```json
{
  "type": "record",
  "name": "Payment",
  "fields": [
    { "name": "payment_id", "type": "string" },
    { "name": "amount",     "type": "double" },
    {
      "name": "card_number",
      "type": "string",
      "confluent:tags": ["PCI"]
    },
    {
      "name": "customer_name",
      "type": "string",
      "confluent:tags": ["PII"]
    }
  ],
  "metadata": {
    "properties": {
      "owner": "payments-platform-team"
    }
  },
  "ruleSet": {
    "domainRules": [
      {
        "name": "encryptPCI",
        "kind": "TRANSFORM",
        "type": "ENCRYPT",
        "mode": "WRITEREAD",
        "tags": ["PCI"],
        "params": {
          "encrypt.kek.name": "payments-pci-kek"
        },
        "onFailure": "DLQ"
      },
      {
        "name": "encryptPII",
        "kind": "TRANSFORM",
        "type": "ENCRYPT",
        "mode": "WRITEREAD",
        "tags": ["PII"],
        "params": {
          "encrypt.kek.name": "payments-pii-kek"
        },
        "onFailure": "DLQ"
      }
    ]
  }
}
```

Fields tagged `PCI` or `PII` are automatically encrypted at produce time and decrypted at consume time for authorised consumers. Consumers without KMS access receive the ciphertext.

Rule modes:
- `WRITE` — encrypt on produce only
- `READ` — decrypt on consume only (useful when records were pre-encrypted)
- `WRITEREAD` — encrypt on produce, decrypt on consume (most common)

## KMS Integration

CSFLE uses a **Key Encryption Key (KEK)** stored in KMS. The KEK wraps a **Data Encryption Key (DEK)** which is what actually encrypts the field values. This is envelope encryption.

```
KMS holds:   KEK (Key Encryption Key)
DEK is:      generated per-schema or per-entity, wrapped by KEK
             stored alongside the encrypted record or in KMS
```

**Supported KMS providers:**

| Provider | Integration |
|---|---|
| AWS KMS | Native — uses IAM roles for DEK retrieval |
| Azure Key Vault | Native — uses Azure Managed Identity or service principal |
| Google Cloud KMS | Native — uses GCP service account |
| HashiCorp Vault | Via custom KMS driver |
| Proprietary vaults | Custom KMS driver (implementing the KMS provider interface) |

**Producer KMS config:**

```properties
# AWS KMS example
rule.executors._default_.param.secret.access.key=<aws-secret-key>
rule.executors._default_.param.access.key.id=<aws-access-key-id>
# Or use instance profile / IAM role (preferred in production)

schema.registry.url=https://your-schema-registry-url
use.latest.version=true
auto.register.schemas=false
```

## Key Rotation

Centralised KMS integration enables automated key rotation without modifying producer or consumer applications. The KMS rotates the KEK; existing DEKs wrapped with the old KEK are re-wrapped with the new KEK. Records encrypted with previous DEKs remain readable — the DEK itself does not change, only its wrapper.

This is **not** per-entity key rotation. All records encrypted with a given KEK share the same rotation schedule. For per-entity key management (needed for right-to-erasure), see crypto-shredding below.

## Authorised Consumer Access

A consumer can read encrypted fields only if it has:

1. **KMS IAM permission** — the consumer's identity (service account, IAM role) must have `kms:Decrypt` permission on the specific KMS key used for the field
2. **RBAC role** — the consumer must have at minimum `DeveloperRead` on the topic in Confluent Cloud
3. **Schema Registry access** — to retrieve the schema and its encryption rules

Consumers without KMS permission receive the ciphertext. The record deserialises normally — the encrypted field contains a byte string instead of the plaintext value. The application sees an opaque value, not an error, unless the consumer is configured to fail on missing KMS access.

## CSFLE in Stream Processing

Apache Flink on Confluent Cloud can process CSFLE-encrypted records. The Flink source connector decrypts fields using the configured KMS credentials, the processing logic operates on plaintext, and the Flink sink connector re-encrypts the output fields before writing to the downstream topic. This allows fraud detection and enrichment pipelines to operate on sensitive data without persisting plaintext anywhere in the processing layer.

## CSFLE vs Crypto-Shredding

These two are frequently conflated. They solve different problems and can be combined.

| | CSFLE | Crypto-Shredding |
|---|---|---|
| **What it is** | Encryption mechanism — transforms plaintext fields to ciphertext | Erasure pattern — makes existing encrypted data permanently unreadable |
| **Purpose** | Access control — only authorised consumers can read sensitive fields | Right-to-erasure — satisfy GDPR/HIPAA individual deletion requests without modifying Kafka logs |
| **Key scope** | Typically per-schema or per-topic | Per-entity (per customer, per user) — one DEK per entity |
| **Key lifecycle** | Rotation on a schedule; data remains readable with new key | Deletion = erasure; destroying the DEK makes all records for that entity permanently unreadable |
| **Broker log** | Ciphertext stored; broker cannot read plaintext | Ciphertext stored; after DEK deletion, ciphertext is permanently unreadable |
| **Erasure capability** | No — rotating a shared key does not erase individual records | Yes — deleting an entity's DEK is the erasure event |

**Using CSFLE to enable crypto-shredding:** assign a unique DEK per customer entity instead of a shared per-schema DEK. CSFLE encrypts the fields; destroying the customer's DEK in KMS is the erasure event. This requires per-entity key management, which increases KMS cost and operational complexity. See `08-Stream-Governance/pii-tracking.md` for the crypto-shredding pattern.

## Decision

| Requirement | Use |
|---|---|
| Sensitive fields must be unreadable to the broker and unauthorised consumers | CSFLE |
| Individual entity erasure required (GDPR, Privacy Act) | CSFLE with per-entity DEK (crypto-shredding pattern) |
| Periodic security key hygiene only; no erasure requirement | CSFLE with shared DEK + KMS rotation schedule |
| Full record must be opaque (not just specific fields) | Full-payload encryption at the producer — CSFLE is not the right tool |
| Data at rest protection in object storage (tiered storage) | CSFLE encrypts the fields before they reach the broker — tiered storage inherits the encryption |
