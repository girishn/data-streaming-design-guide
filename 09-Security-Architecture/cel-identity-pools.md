# CEL Identity Pools — Claim-Based Access at Scale

## The Service Account Problem

Traditional Confluent Cloud access management creates one service account per application or workload. Each service account gets an API key pair. At ten services this is manageable; at a hundred it becomes a credential management problem — keys must be issued, rotated, revoked, and audited individually. When a service is decommissioned, its keys must be found and deleted. When a new service is deployed, a human must create an account and distribute credentials.

Identity Pools solve this by replacing static per-service credentials with dynamic, claim-based identity mapping. Instead of "service X has API key Y," the model becomes "any token from IdP Z that contains claim group=kafka-producers is mapped to Confluent role R."

## What Identity Pools Are

An Identity Pool is a Confluent Cloud construct that:

1. Binds to an external Identity Provider (registered separately in Confluent Cloud)
2. Defines a **CEL filter** that selects which JWT tokens from that IdP qualify for this pool
3. Assigns a **Confluent RBAC role** (or resource-level bindings) to matching identities

When a Kafka client presents a JWT token via `SASL/OAUTHBEARER`, Confluent Cloud evaluates the token's claims against every Identity Pool associated with the cluster's IdP. If the token matches a pool's CEL filter, the client receives the permissions bound to that pool.

Multiple pools can match a single token — permissions are the union of all matching pools.

## CEL Filter Syntax

CEL (Common Expression Language) is a lightweight expression language designed for claim evaluation. Confluent uses it to match JWT claim values against pool membership criteria.

**Standard JWT claims available in CEL:**

| Claim | Description |
|---|---|
| `claims.sub` | Subject — typically the client ID or user identifier |
| `claims.iss` | Issuer — the IdP URL |
| `claims.aud` | Audience — the intended recipient of the token |
| `claims.exp` | Expiry timestamp |
| `claims["custom-claim"]` | Any custom claim in the token payload |

**Example CEL filters:**

```cel
# Match all tokens from a specific client application
claims.sub == "payments-service-prod"

# Match tokens where a custom group claim contains a value
"kafka-producers" in claims.groups

# Match tokens from a specific department attribute
claims.department == "platform-engineering"

# Combine conditions
claims.environment == "production" && "data-pipeline" in claims.roles

# Match by issuer (scope a pool to one IdP application)
claims.iss == "https://idp.company.com/oauth2/ausXXXXX"
```

CEL filters are evaluated at authentication time against the token's actual claims — no pre-registration of individual identities required. A new service instance that presents a token from the same IdP with the matching claims is automatically granted access.

## Assigning Permissions to Pools

Identity Pools support the same RBAC role bindings as service accounts:

```bash
# Assign DeveloperRead role to pool on a specific topic
confluent iam rbac role-binding create \
  --principal IdentityPool:<pool-id> \
  --role DeveloperRead \
  --resource Topic:orders \
  --kafka-cluster-id <cluster-id>

# Assign CloudClusterAdmin at the cluster level
confluent iam rbac role-binding create \
  --principal IdentityPool:<pool-id> \
  --role CloudClusterAdmin \
  --kafka-cluster-id <cluster-id>
```

Permissions can be scoped to specific resources (topics, consumer groups, connectors) or granted at the cluster level. Principle of least privilege applies — pools should be scoped as narrowly as the workload requires.

## Replacing Static API Keys at Scale

The operational shift when moving to Identity Pools:

| Before (service accounts) | After (Identity Pools) |
|---|---|
| Create service account per deployment | Define pool once, matches all qualifying tokens |
| Distribute API key to each service | Service authenticates to IdP; IdP issues JWT |
| Rotate keys manually or via secret manager | Token lifetime is short; IdP handles rotation |
| Revoke individual accounts on decommission | Revoke IdP application credential; all tokens from it immediately invalid |
| Audit: per-key access logs | Audit: token claims visible in Confluent audit logs |

For organisations with hundreds of microservices, the shift from N service accounts to O(1) pool definitions (one per logical role) is the primary scaling benefit.

## Kubernetes Workload Pattern

Kubernetes workloads using OIDC typically authenticate via service account tokens projected into the pod:

```yaml
# Pod spec — project service account token as a volume
volumes:
  - name: oidc-token
    projected:
      sources:
      - serviceAccountToken:
          path: token
          audience: confluent-cloud
          expirationSeconds: 3600
```

The Kafka client reads the token from the mounted file and presents it via `SASL/OAUTHBEARER`. Kubernetes rotates the token automatically before expiry — no manual refresh logic needed. The token's `sub` claim contains the Kubernetes service account identity (`system:serviceaccount:namespace:name`), which the CEL filter can match against.

This pattern eliminates static secrets from Kubernetes entirely — no API keys in ConfigMaps or Secrets, no manual rotation, and revocation is handled by deleting the Kubernetes service account.

Confluent CLI and Control Center also support OIDC SSO using the same enterprise IdP — engineers authenticate once and access is governed by the same claim-based pool definitions as production services.
