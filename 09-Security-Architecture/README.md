# Module 09 — Security Architecture

Authentication flows, identity federation at scale, and zero-downtime protocol migration for Confluent Platform and Confluent Cloud.

## Files

- [mtls-oauth.md](mtls-oauth.md) — mTLS certificate-based authentication, OAuth/OIDC token flow, broker validation mechanics, and when to use each.
- [cel-identity-pools.md](cel-identity-pools.md) — Identity Pools as a bridge from external IdP claims to Confluent RBAC, CEL filter syntax, scale benefits over per-service accounts, and Kubernetes patterns.
- [multi-protocol-auth.md](multi-protocol-auth.md) — Running multiple authentication protocols simultaneously, listener configuration, zero-downtime migration from legacy SASL mechanisms, and listener isolation by network path.
- [rbac.md](rbac.md) — Role-Based Access Control in Confluent Cloud: role taxonomy (MetricsViewer, DeveloperRead/Write, ResourceOwner, CloudClusterAdmin), role bindings at Environment/Cluster/Topic scope, RBAC vs ACLs distinction, and Identity Pool + CEL + RBAC integration.
- [private-networking.md](private-networking.md) — Private networking for Confluent Cloud Dedicated: AWS PrivateLink vs VPC Peering vs Transit Gateway, data plane vs control plane separation, CoreDNS configuration for private broker hostname resolution.
- [cloud-idp-integration.md](cloud-idp-integration.md) — Multi-tenant platform authentication approaches: service account API keys, credential-based SPN/service account keys, and credential-free workload identity (AWS IRSA, Azure AKS Workload Identity, GCP Workload Identity). Tradeoff table and GitOps implications per approach.
