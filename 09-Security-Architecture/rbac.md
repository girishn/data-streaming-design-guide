# RBAC in Confluent Cloud — Role-Based Access Control

Confluent Cloud RBAC assigns predefined roles to principals (users, service accounts, or Identity Pool identities) at a specific resource scope. It is the strategic access control mechanism for Confluent Cloud, sitting above the lower-level ACL system and integrating directly with Identity Pools and SSO.

## RBAC vs ACLs — The Distinction

Both coexist in Confluent Cloud. They operate at different levels and serve different purposes.

| | RBAC | ACLs |
|---|---|---|
| **Level** | Higher-level — roles bundle multiple permissions | Lower-level — individual allow/deny per operation |
| **Management** | Confluent Cloud UI, CLI, REST API, Terraform | CLI or REST API only (for Identity Pool-linked ACLs) |
| **Scope** | Organization → Environment → Cluster → Topic/Group/Subject | Topic, Consumer Group, Transactional ID, Cluster operation |
| **Principal types** | Users, service accounts, Identity Pool identities | Service accounts, Identity Pool identities |
| **Recommended for** | Most access control in Confluent Cloud | Granular overrides; legacy compatibility; Identity Pool fine-grained rules |
| **Integration with Identity Pools** | CEL expression in Identity Pool maps to RBAC role binding | CEL expression maps to ACL policy |
| **Managed Kafka Connect** | Not applicable — Connect uses service accounts with RBAC | Applicable for self-managed Connect service accounts |

**No documented technical precedence** between RBAC and ACLs exists in Confluent documentation. In practice, RBAC is the primary control plane for Confluent Cloud; ACLs are used where RBAC role granularity is insufficient or where existing tooling requires them.

## Role Taxonomy

Roles are scoped. The same role name at different scopes grants different permissions.

### Organization-level roles

| Role | What it grants |
|---|---|
| `OrganizationAdmin` | Full control over the entire Confluent Cloud organisation — environments, clusters, billing, user management |
| `MetricsViewer` | Read access to the Confluent Cloud Metrics API across all clusters — used by monitoring service accounts (Datadog, Prometheus integrations) |

### Environment-level roles

| Role | What it grants |
|---|---|
| `EnvironmentAdmin` | Full control within an environment — cluster creation, Schema Registry, network config |

### Cluster-level roles

| Role | What it grants |
|---|---|
| `CloudClusterAdmin` | Full control within a cluster — topic management, ACL management, connector management |
| `DeveloperManage` | Create/delete topics, connectors, and consumer groups — no ACL management |
| `DeveloperRead` | Read from topics, read consumer group offsets — no produce, no manage |
| `DeveloperWrite` | Produce to topics — no consume, no manage |

### Resource-level roles (Topic, Consumer Group, Subject)

Roles at resource level scope to a specific named resource or a resource prefix pattern:

| Role | Scope | What it grants |
|---|---|---|
| `ResourceOwner` | Topic / Consumer Group / Subject | Full lifecycle management of the specific resource — equivalent to topic-level admin |
| `DeveloperRead` | Topic | Read from a specific topic or prefix pattern |
| `DeveloperWrite` | Topic | Produce to a specific topic or prefix pattern |
| `DeveloperRead` | Consumer Group | Read consumer group offsets for a specific group |

**Prefix patterns** are the primary tool for multi-tenant platforms. A team granted `DeveloperWrite` on topic prefix `payments.` can produce to any topic starting with `payments.` — new topics matching the prefix are automatically covered without updating the role binding.

## Role Bindings

A role binding maps a principal to a role at a scope:

```bash
# Grant a service account DeveloperRead on a specific topic
confluent iam rbac role-binding create \
  --principal User:sa-abc123 \
  --role DeveloperRead \
  --resource Topic:payments.initiated \
  --kafka-cluster-id <cluster-id>

# Grant DeveloperWrite on a topic prefix (covers all matching topics)
confluent iam rbac role-binding create \
  --principal User:sa-abc123 \
  --role DeveloperWrite \
  --resource Topic:payments. \
  --prefix \
  --kafka-cluster-id <cluster-id>

# Grant MetricsViewer at org level for a monitoring service account
confluent iam rbac role-binding create \
  --principal User:sa-monitoring \
  --role MetricsViewer
```

In Terraform:

```hcl
resource "confluent_role_binding" "payments_producer" {
  principal   = "User:${confluent_service_account.payments_service.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=payments.*"
}

resource "confluent_role_binding" "monitoring_metrics" {
  principal   = "User:${confluent_service_account.datadog.id}"
  role_name   = "MetricsViewer"
  crn_pattern = confluent_organization.main.resource_name
}
```

## Identity Pools + RBAC

Identity Pools bridge external identity providers (SSO, OAuth/OIDC) to Confluent Cloud RBAC. A CEL expression on the Identity Pool evaluates claims from the external token and determines which Confluent principal (and thus which RBAC role bindings) the request maps to.

```
External service authenticates with IdP (e.g., Azure AD)
  → Token carries claims: {"team": "payments", "env": "prod", "app": "payment-processor"}
  → Confluent Identity Pool evaluates CEL filter: claims.team == "payments" && claims.env == "prod"
  → Matches → request is treated as the mapped Confluent service account
  → That service account has RBAC role bindings: DeveloperWrite on Topic:payments.*
```

This allows access control to be governed by identity attributes without managing Confluent-specific credentials per service. A new instance of the payments service authenticates with the IdP, presents a valid token, and inherits the correct RBAC permissions automatically.

See `09-Security-Architecture/cel-identity-pools.md` for CEL expression patterns and Identity Pool configuration.

## Multi-Tenant Platform Access Model

On a shared platform with multiple teams, the standard pattern is:

```
Platform team: CloudClusterAdmin (manages cluster-level config, quotas, Schema Registry)

Team A:
  Producer service account → DeveloperWrite on Topic:team-a.*
  Consumer service account → DeveloperRead on Topic:team-a.* + DeveloperRead on ConsumerGroup:team-a.*

Team B:
  Producer service account → DeveloperWrite on Topic:team-b.*
  Consumer service account → DeveloperRead on Topic:team-b.* + DeveloperRead on ConsumerGroup:team-b.*

Cross-team read (declared, governed):
  Team B consumer → DeveloperRead on Topic:team-a.events.public (specific topic, not prefix)
```

The topic naming convention encodes the team prefix — RBAC prefix bindings then provide automatic access to new topics within the team's namespace. Cross-team access requires an explicit resource-level role binding, creating a discoverable, auditable access record.

## Schema Registry RBAC

Schema Registry subjects have their own RBAC scope within the environment:

```bash
# Grant a team's producer service account permission to register schemas
confluent iam rbac role-binding create \
  --principal User:sa-payments-producer \
  --role DeveloperWrite \
  --resource Subject:payments- \
  --prefix \
  --schema-registry-cluster-id <sr-cluster-id>
```

Combined with `auto.register.schemas=false` on all producers, schema registration is a controlled operation requiring explicit RBAC permission — producers cannot push unreviewed schemas to production.

## When to Use RBAC vs ACLs

| Scenario | Use |
|---|---|
| Team-level access to a topic namespace (prefix) | RBAC role binding with prefix pattern |
| Service account needs Metrics API access for monitoring | RBAC MetricsViewer at org scope |
| Identity Pool needs to grant access based on token claims | RBAC role binding on the mapped service account |
| Fine-grained transactional ID control | ACL — RBAC does not have a transactional ID resource type |
| Self-managed Kafka Connect connector ACLs | ACL — connector worker service account needs specific ACL operations |
| Cross-team read access to a specific topic | RBAC DeveloperRead at resource level (not prefix) |

## Cross-References

- Identity Pools and CEL expressions — [09-Security-Architecture/cel-identity-pools.md](cel-identity-pools.md)
- mTLS and OAuth/OIDC authentication flows — [09-Security-Architecture/mtls-oauth.md](mtls-oauth.md)
- Quota management per principal — [13-Performance-Tuning/quota-management.md](../13-Performance-Tuning/quota-management.md)
- GitOps Terraform for role binding automation — [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)
