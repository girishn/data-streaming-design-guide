# PII Tracking and Crypto-Shredding

## PII Tagging in Stream Governance

Confluent Stream Governance supports PII classification at two levels of granularity:

**Record-level tagging:** an entire schema is tagged as containing sensitive data. Downstream consumers and governance policies apply to all records from topics using that schema. Use for schemas where every field is sensitive — identity documents, financial records.

**Field-level tagging:** individual fields within a schema are tagged (e.g., `PII`, `GDPR`, `CCPA`). The Stream Catalog renders a navigable schema tree showing each field with its classification. Use for mixed schemas where only specific fields are regulated — an order event that contains `order_id` (not sensitive) and `customer_email` (PII).

**Automated detection:** Confluent Intelligence applies AI/ML-based pattern matching to detect PII in existing streams — email addresses, phone numbers, SSNs, credit card numbers. Detection suggestions are surfaced in the Stream Catalog for governance teams to confirm or dismiss. Automated detection is a starting point, not a substitute for explicit tagging — false negatives are possible for domain-specific sensitive data that does not match standard patterns.

Tags apply to schema versions in the Registry and propagate through the Stream Lineage graph, making it visible which downstream consumers are processing tagged data. This is the foundation for compliance reporting — see `08-Stream-Governance/stream-lineage.md`.

## The Crypto-Shredding Pattern

Kafka topics are immutable append-only logs. Traditional deletion — finding a record and removing it — is not natively supported. Log compaction removes superseded keys but requires producing a tombstone, which still leaves the old records accessible until compaction runs. For regulatory right-to-erasure requirements (GDPR Article 17), this is insufficient on its own.

**Crypto-shredding** achieves functional erasure without modifying the log:

1. **At write time:** sensitive fields are encrypted using **Client-Side Field Level Encryption (CSFLE)** with a unique encryption key per entity (e.g., one key per `user_id`). The encrypted ciphertext is written to the topic; the plaintext never reaches Kafka.

2. **At erasure time:** the encryption key for the target entity is destroyed in the key management system. The encrypted bytes remain permanently in the topic log, but with no key to decrypt them, they are mathematically inaccessible. For regulatory purposes, inaccessible ciphertext is treated as effectively deleted.

3. **Ongoing reads:** consumers decrypt fields at read time using the KMS-managed key. After key destruction, decryption fails — consumers receive an error or a null value for the encrypted field, depending on how the CSFLE client is configured to handle missing keys.

The pattern requires all producers to encrypt sensitive fields before writing and all consumers to decrypt at read time. Producers that bypass CSFLE write plaintext that cannot be shredded — governance controls must enforce encryption at the producer level.

## KMS Integration

Encryption keys are managed externally to Kafka. Confluent CSFLE integrates with:

- **AWS KMS** — envelope encryption using KMS customer-managed keys
- **Azure Key Vault** — key wrapping via Azure managed keys
- **Google Cloud KMS** — Cloud KMS key rings
- **Custom KMS drivers** — for proprietary vaulting systems or HSMs

The integration pattern is envelope encryption: a Data Encryption Key (DEK) encrypts the field value; the DEK itself is encrypted by a Key Encryption Key (KEK) stored in the KMS. The encrypted DEK is stored alongside the record. To encrypt or decrypt, the client calls the KMS to unwrap the DEK — the KMS never sees the plaintext data.

Key rotation is managed by the KMS administrator. Rotating a KEK does not require re-encrypting historical records — the DEK envelope is re-wrapped with the new KEK on next access. Destroying a KEK renders all DEKs wrapped by it permanently inaccessible, which is the mechanism for entity-level erasure.

## Limitations

**Data occupies storage until purged:** crypto-shredding makes data unreadable but does not remove it from disk. Encrypted records remain in topic segments until standard retention or compaction policies delete the segments. For long-retention topics (years), physically unreadable ciphertext may sit on disk for extended periods. Regulators may require evidence that data cannot be recovered, not just that the key is destroyed — confirm with legal counsel whether ciphertext-without-key satisfies the applicable regulatory standard.

**Metadata remains visible:** crypto-shredding encrypts field values. It does not encrypt message keys, headers, schema IDs, timestamps, or partition assignments. If the `user_id` used as a message key is itself PII, shredding the value fields is insufficient — the key remains readable in the log indefinitely. Design message keys to be non-PII identifiers (internal UUIDs rather than email addresses or government IDs) or apply separate key-level encryption.

**Consumer error handling required:** after key destruction, any consumer attempting to decrypt the erased entity's records will receive a decryption failure. Consumers must be designed to handle this gracefully — logging the failure, emitting a null value, or skipping the record — rather than crashing. Test the post-erasure consumer path explicitly before relying on crypto-shredding for compliance.
