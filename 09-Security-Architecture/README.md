# Module 09 — Security Architecture

Authentication flows, identity federation at scale, and zero-downtime protocol migration for Confluent Platform and Confluent Cloud.

## Files

- [mtls-oauth.md](mtls-oauth.md) — mTLS certificate-based authentication, OAuth/OIDC token flow, broker validation mechanics, and when to use each.
- [cel-identity-pools.md](cel-identity-pools.md) — Identity Pools as a bridge from external IdP claims to Confluent RBAC, CEL filter syntax, scale benefits over per-service accounts, and Kubernetes patterns.
- [multi-protocol-auth.md](multi-protocol-auth.md) — Running multiple authentication protocols simultaneously, listener configuration, zero-downtime migration from legacy SASL mechanisms, and listener isolation by network path.
