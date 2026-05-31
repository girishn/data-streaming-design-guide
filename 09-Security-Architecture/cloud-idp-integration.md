# Cloud Identity Provider Integration — Multi-Tenant Platform Patterns

Multi-tenant Kafka platforms need to answer two questions for every team that onboards: **who is this service?** (authentication) and **what topics can it access?** (authorization). These are separate layers with separate tools, and the choice of authentication approach determines how much friction each new team onboarding adds.

This file covers the three authentication approaches available — service accounts with API keys, credential-based external IdP (SPN/Service Account Key), and credential-free workload identity — and evaluates each from a multi-tenant platform operator's perspective.

---

## The Two Layers

```
Authentication layer          Authorization layer
───────────────────           ───────────────────
How the service proves        What topics it can read/write
who it is
                              ACL:  principal = service account ID
SPN + client secret      →    RBAC: principal = IdentityPool:<pool-id>
Managed Identity token   →    RBAC: principal = IdentityPool:<pool-id>
Confluent API key        →    ACL or RBAC: principal = service account ID
```

The GitOps pipeline manages the **authorization layer** — ACLs, RBAC role bindings, quotas. The authentication layer is set up once (per cluster or per team) and stays out of the day-to-day pipeline.

Getting these confused is the most common source of onboarding complexity on new platforms.

---

## Approach A — Confluent Service Account + API Key

The simplest approach. No external IdP required. The service authenticates directly to Confluent Cloud using an API key pair (key + secret) generated from a Confluent Service Account.

```
Confluent Service Account (created via Terraform)
         ↓
API Key + Secret (generated, stored in secret manager)
         ↓
ACL or RBAC role binding on topics
         ↓
Application uses key+secret in Kafka client config
```

**Multi-tenant platform implications:**

Each onboarding team gets one service account (or one producer + one consumer account). The platform team creates the account, generates the API key, and distributes it via a secret manager (HashiCorp Vault, AWS Secrets Manager, Azure Key Vault).

| Factor | Detail |
|---|---|
| Platform team effort per team | Create SA + API key + distribute secret |
| Credential rotation | Manual or via secret manager automation |
| Revocation | Delete the API key — immediate |
| CEL / Identity Pool | Not needed — SA ID is the principal directly |
| Teams manage secrets? | Yes — app must store and rotate the API key |

**When to use:** small platforms (< 20 teams), teams that cannot use cloud-native identity, non-Kubernetes workloads, or as a fallback when external IdP integration is not yet in place.

**Tradeoff:** scales poorly. At 100 teams you have 100–200 service accounts, 200–400 API keys, and a rotation audit burden proportional to team count. Each decommissioned team leaves orphan credentials unless actively cleaned up.

---

## Approach B — Credential-Based External IdP (SPN / Service Account Key)

The service authenticates to the cloud's identity provider using a long-lived credential (SPN client secret, GCP service account JSON key). The IdP issues a short-lived JWT. The JWT is presented to Kafka via `SASL/OAUTHBEARER`. The Identity Pool matches it.

```
Azure SPN: client_id + client_secret → Azure AD → JWT → Kafka
GCP:       service account JSON key  → Google   → JWT → Kafka
AWS:       access key + secret key   → (not JWT-based — see note)
```

**AWS note:** AWS IAM does not issue OIDC JWTs natively from access keys. For credential-based AWS authentication to Confluent Cloud, the standard is to use Amazon Cognito as the OIDC-compliant IdP, or use an external IdP (Okta, Azure AD) regardless of cloud.

### Azure — Service Principal (SPN)

Setup per team:
1. Create an App Registration in Azure AD (Entra ID) → produces `client_id` and `tenant_id`
2. Create a client secret (has an expiry date — must be rotated)
3. Register the Azure AD tenant as an Identity Provider in Confluent Cloud (once per cluster)
4. Create an Identity Pool with a CEL filter matching the SPN's `sub` or `azp` claim
5. Bind RBAC role to the Identity Pool on the required topics

CEL filter example:
```cel
claims.sub == "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # SPN object ID
# or match on application ID claim
claims.azp == "my-app-client-id"
```

The `sub` claim in Azure AD tokens is a GUID — not human-readable. Prefer matching on `azp` (authorised party, the client_id) which is the application's client ID and is readable.

### GCP — Service Account Key

Setup per team:
1. Create a GCP Service Account in IAM
2. Download a JSON key file
3. Register Google as an Identity Provider in Confluent Cloud (once per cluster)
4. Create an Identity Pool with CEL filter on `sub` (service account email)
5. Bind RBAC role to the Identity Pool on the required topics

CEL filter example:
```cel
claims.sub == "payments-consumer@my-project.iam.gserviceaccount.com"
```

The `sub` claim is a service account email — readable and meaningful.

**Multi-tenant platform implications for Approach B:**

| Factor | Azure SPN | GCP Service Account Key |
|---|---|---|
| Platform team effort per team | Create SPN, configure CEL filter | Create SA, download key, configure CEL filter |
| Credential rotation | Client secret expiry — must be tracked | JSON key file — no expiry by default, rotation is manual |
| Revocation | Delete client secret or SPN | Delete key file from IAM |
| Teams manage secrets? | Yes — client secret must be stored securely | Yes — JSON key file must be stored securely |
| CEL filter readability | Medium — match on `azp` (client_id) | High — match on service account email |

**Tradeoff:** better than API keys because short-lived JWTs reduce the blast radius of a credential leak. But long-lived secrets (client secret, JSON key) still exist and must be rotated. Each team onboard still requires platform team intervention to create the SPN/service account and wire up the Identity Pool CEL filter.

---

## Approach C — Credential-Free Workload Identity

No long-lived credential is stored anywhere. The cloud platform injects an identity into the running workload. The workload calls a local metadata endpoint (no secret required) and receives a short-lived JWT. That JWT is presented to Kafka via `SASL/OAUTHBEARER`.

```
Workload calls local metadata endpoint (no secret)
         ↓
Cloud platform issues short-lived JWT (auto-rotated)
         ↓
JWT → Kafka via SASL/OAUTHBEARER
         ↓
Identity Pool CEL filter matches JWT claims
         ↓
RBAC role binding grants topic access
```

### AWS — IRSA and EKS Pod Identity

Two patterns — IRSA is the established standard; EKS Pod Identity is newer and simpler.

**IRSA (IAM Roles for Service Accounts):**
- Annotate the Kubernetes service account with an IAM Role ARN
- Kubernetes projects an OIDC token into the pod as a mounted file
- The pod reads the token and presents it to Confluent via `SASL/OAUTHBEARER`
- Confluent validates it against the EKS cluster's OIDC issuer endpoint

```yaml
# Kubernetes service account annotation — this is the only team-side config needed
apiVersion: v1
kind: ServiceAccount
metadata:
  name: payments-consumer
  namespace: payments
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/payments-consumer-role
```

CEL filter on Identity Pool:
```cel
claims.sub == "system:serviceaccount:payments:payments-consumer"
```

**EKS Pod Identity (newer, simpler):**
- No OIDC issuer configuration needed at the cluster level
- Pod identity association is created via EKS API — links the K8s service account to an IAM role
- Token is projected automatically into the pod

EKS Pod Identity removes the OIDC issuer setup step, making it slightly simpler for platform teams to operationalise across many clusters.

**AWS self-service fit — strongest of the three clouds:**
- The `sub` claim encodes namespace + service account name: `system:serviceaccount:payments:payments-consumer`
- Readable, auditable, directly maps to Kubernetes team namespaces
- Platform team registers the EKS OIDC issuer once per cluster
- Teams control their own Kubernetes service accounts — no platform team ticket per team
- A single Identity Pool with a namespace-scoped CEL filter covers all teams in a namespace:

```cel
# All workloads in the payments namespace — no per-team pool needed
claims.sub.startsWith("system:serviceaccount:payments:")
```

### Azure — AKS Workload Identity

AKS Workload Identity replaces the older AAD Pod Identity approach.

Setup per team:
1. Platform team creates a User-Assigned Managed Identity in Azure
2. Platform team creates a federated credential linking the Managed Identity to the team's Kubernetes service account
3. Platform team registers the federated credential in the Confluent Identity Pool
4. Team annotates their Kubernetes service account (same as AWS)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: payments-consumer
  namespace: payments
  annotations:
    azure.workload.identity/client-id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

The JWT `sub` claim contains the Managed Identity's object ID — a GUID. Not human-readable in the CEL filter. Prefer matching on `azp` or a custom `oid` mapping if the IdP allows it.

**Azure self-service fit — more platform team involvement per team:**

Every team onboard requires the platform team to create a Managed Identity and federated credential in Azure. This is an Azure-side operation outside Kubernetes — it cannot be self-served by the consuming team. The Identity Pool CEL filter must also be updated per team unless a broader claim (such as a group membership) is used.

Mitigation: assign a single User-Assigned Managed Identity to a team namespace rather than per service, and use group claims in the CEL filter:

```cel
"kafka-consumers" in claims.groups
```

This covers all workloads in a namespace with one pool definition — but loses per-service granularity.

### GCP — Workload Identity Federation on GKE

Setup per team:
1. Create a GCP Service Account per team
2. Create a Workload Identity binding: map the Kubernetes service account to the GCP Service Account
3. Register Google as Identity Provider in Confluent (once per cluster)
4. Create Identity Pool with CEL filter on `sub` (GCP service account email)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: payments-consumer
  namespace: payments
  annotations:
    iam.gke.io/gcp-service-account: payments-consumer@my-project.iam.gserviceaccount.com
```

CEL filter:
```cel
claims.sub == "payments-consumer@my-project.iam.gserviceaccount.com"
```

The `sub` claim is the GCP service account email — readable and meaningful. GCP Workload Identity sits between AWS and Azure for self-service ease: teams control their K8s service account annotation, but the platform team must create the GCP service account and IAM binding.

---

## Auto Pool Mapping

Confluent Cloud supports **Auto Pool Mapping** — the broker automatically matches an incoming JWT to the correct Identity Pool based on token claims, without the client needing to know which pool it belongs to.

Without Auto Pool Mapping: the client config must include the pool ID explicitly.
With Auto Pool Mapping: the broker evaluates all pools for the registered IdP, finds the matching CEL filter, and applies the correct RBAC bindings. The client config only needs the IdP's token endpoint.

This is significant for self-service: new teams onboard by adding an RBAC binding to an existing pool (or creating a new pool), without any change to the application's Kafka client config.

---

## Multi-Tenant Platform Decision Matrix

| Factor | Approach A (API Key) | Approach B (SPN/SA Key) | Approach C — AWS IRSA | Approach C — Azure Workload Identity | Approach C — GCP Workload Identity |
|---|---|---|---|---|---|
| Long-lived secrets? | Yes (API key) | Yes (client secret / JSON key) | No | No | No |
| Platform team effort per team | Low | Medium | Low (after cluster setup) | High | Medium |
| Team self-service? | Partial | Partial | Strong | Weak | Moderate |
| CEL filter readability | N/A | Medium (Azure), High (GCP) | High | Low (GUID) | High |
| Rotation burden | API key rotation | Secret expiry tracking | None (auto-rotated) | None (auto-rotated) | None (auto-rotated) |
| Revocation speed | Immediate (delete key) | Immediate (delete secret) | Immediate (remove annotation) | Immediate (remove binding) | Immediate (remove binding) |
| Scales to 100+ teams | No | Partially | Yes | No without group claims | Partially |
| Non-Kubernetes workloads | Yes | Yes | No (K8s only) | No (K8s only) | No (K8s only) |

**Recommended path for a new platform:**

1. Start with **Approach A** (API keys) if the platform is small (< 20 teams) or the timeline does not allow IdP wiring.
2. Move to **Approach C** (credential-free) when the platform runs on Kubernetes at scale. AWS IRSA gives the best self-service ratio; Azure requires more upfront platform team work per team.
3. Use **Approach B** (SPN/Service Account Key) for non-Kubernetes workloads or when external systems outside the cluster need Kafka access and cannot use managed identity.

---

## GitOps Implications

The authorization layer (RBAC bindings, ACLs, quotas) is managed by the GitOps pipeline regardless of authentication approach. What changes between approaches is what goes in the `principal` field and whether a new Identity Pool must be created per team.

**Approach A — new team GitOps PR:**
```hcl
resource "confluent_service_account" "payments_consumer" { ... }
resource "confluent_api_key" "payments_consumer_key" { ... }
resource "confluent_role_binding" "payments_consumer_read" {
  principal   = "User:${confluent_service_account.payments_consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "...topic=orders.*"
}
```

**Approach B/C — new team GitOps PR (pool already exists):**
```hcl
resource "confluent_role_binding" "payments_consumer_read" {
  principal   = "IdentityPool:${confluent_identity_pool.aws_eks_consumers.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "...topic=orders.*"
}
```

With a shared pool and Auto Pool Mapping, onboarding a new team on Approach C is a single role binding — no new service account, no API key, no secret distribution. This is the target state for a mature self-service platform.

---

## Cross-References

- Identity Pool CEL filter syntax and RBAC bindings — [09-Security-Architecture/cel-identity-pools.md](cel-identity-pools.md)
- mTLS and OAuth/OIDC flow — [09-Security-Architecture/mtls-oauth.md](mtls-oauth.md)
- RBAC role taxonomy — [09-Security-Architecture/rbac.md](rbac.md)
- Consumer onboarding gates including service principal provisioning — [10-Operational-Patterns/consumer-onboarding.md](../10-Operational-Patterns/consumer-onboarding.md)
- GitOps Terraform patterns for ACLs and role bindings — [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)
