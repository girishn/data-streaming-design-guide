# GitOps and Terraform for Kafka Topic Management

At scale, manual topic creation and ACL management breaks down. A shared platform serving hundreds of teams produces topic sprawl, ACL drift, and schema inconsistency within weeks. GitOps — managing Kafka infrastructure as code through git-based pipelines — is the production standard for multi-tenant streaming platforms.

The Confluent Terraform provider (`confluentinc/confluent`) makes the full Confluent Cloud stack declarative: topics, ACLs, RBAC role bindings, schemas, connectors, Flink compute pools, and networking are all Terraform resources. Every change goes through a pull request, gets peer-reviewed, and is applied by a pipeline — not a human with CLI access.

## Two-Pipeline Model

Large organisations separate infrastructure changes from cluster-level changes. These two pipelines run independently:

```
Infrastructure Pipeline                 Cluster Pipeline
──────────────────────                 ────────────────
Triggered by: infra/ changes           Triggered by: cluster/ changes
Applies:                               Applies:
  • Environments                         • Topics
  • Kafka clusters                       • ACLs
  • PrivateLink / VPC Peering            • RBAC role bindings
  • Identity Pools                       • Schema registration
  • Service accounts (platform level)    • Connectors
  • Flink compute pools                  • Quotas
                                         • Consumer group configs

Runs as: platform team pipeline        Runs as: platform team pipeline
                                         (or delegated to team pipelines
                                          within namespace guardrails)
```

The infrastructure pipeline is privileged — it manages the cluster itself. The cluster pipeline manages what lives inside the cluster. Separating them limits blast radius: a bad topic config change cannot accidentally destroy a cluster.

## What the Terraform Provider Manages

### Topics

```hcl
resource "confluent_kafka_topic" "payments_initiated" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  topic_name       = "payments.initiated.v1"
  partitions_count = 12
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  config = {
    "cleanup.policy"                      = "delete"
    "retention.ms"                        = "604800000"   # 7 days
    "min.insync.replicas"                 = "2"
    "confluent.value.schema.validation"   = "true"        # broker-side validation
  }

  credentials {
    key    = confluent_api_key.terraform_cluster_key.id
    secret = confluent_api_key.terraform_cluster_key.secret
  }
}
```

The topic name is the source of truth. Naming conventions that encode team, domain, and version (`{team}.{entity}.{version}`) allow the pipeline to derive ACL principals and Schema Registry subjects automatically from the topic definition.

### ACLs

```hcl
# Producer ACL — payments team producer service account
resource "confluent_kafka_acl" "payments_producer_write" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.payments_producer.id}"
  operation     = "WRITE"
  permission    = "ALLOW"
  host          = "*"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint
  credentials {
    key    = confluent_api_key.terraform_cluster_key.id
    secret = confluent_api_key.terraform_cluster_key.secret
  }
}

# Consumer ACL — payments team consumer service account
resource "confluent_kafka_acl" "payments_consumer_read" {
  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.payments_consumer.id}"
  operation     = "READ"
  permission    = "ALLOW"
  host          = "*"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint
  credentials {
    key    = confluent_api_key.terraform_cluster_key.id
    secret = confluent_api_key.terraform_cluster_key.secret
  }
}
```

Prefix ACLs (`pattern_type = "PREFIXED"`) are the key tool for multi-tenant platforms. Once a team's prefix binding is in place, new topics matching the prefix are automatically covered — no ACL update needed when a team adds a new topic within their namespace.

### RBAC Role Bindings

```hcl
# DeveloperWrite for payments producer on the topic namespace
resource "confluent_role_binding" "payments_producer_write" {
  principal   = "User:${confluent_service_account.payments_producer.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=payments.*"
}

# MetricsViewer for the Datadog monitoring service account
resource "confluent_role_binding" "datadog_metrics_viewer" {
  principal   = "User:${confluent_service_account.datadog.id}"
  role_name   = "MetricsViewer"
  crn_pattern = data.confluent_organization.main.resource_name
}
```

### Schema Registration

```hcl
resource "confluent_schema" "payments_initiated_v1" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }
  subject_name = "payments.initiated.v1-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/payments-initiated-v1.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }
}

# Set compatibility mode per subject — FULL_TRANSITIVE on shared platform topics
resource "confluent_schema_registry_config" "payments_initiated_compat" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }
  subject_name             = "payments.initiated.v1-value"
  compatibility_level      = "FULL_TRANSITIVE"
  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }
}
```

Schema registration via Terraform means:
- Schemas go through pull request review before reaching production
- Compatibility is enforced in CI before `terraform apply`
- `auto.register.schemas=false` in all producer configs — producers cannot push schemas directly
- Schema history is auditable via git history

### Connectors

```hcl
resource "confluent_connector" "payments_s3_sink" {
  environment {
    id = confluent_environment.main.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  config_sensitive = {
    "kafka.api.key"    = confluent_api_key.connector_key.id
    "kafka.api.secret" = confluent_api_key.connector_key.secret
  }

  config_nonsensitive = {
    "connector.class"          = "S3_SINK"
    "name"                     = "payments-s3-sink"
    "kafka.topic"              = "payments.initiated.v1"
    "input.data.format"        = "AVRO"
    "output.data.format"       = "PARQUET"
    "s3.bucket.name"           = "payments-archive"
    "s3.part.size"             = "67108864"
    "tasks.max"                = "4"
    "time.interval"            = "HOURLY"
  }
}
```

### Flink Compute Pools

```hcl
resource "confluent_flink_compute_pool" "fraud_detection" {
  display_name = "fraud-detection-pool"
  cloud        = "AWS"
  region       = "ap-southeast-2"
  max_cfu      = 20    # Confluent Flink Units — scales up to this limit

  environment {
    id = confluent_environment.main.id
  }
}
```

## Topic Naming Convention → ACL Automation

The naming standard is the key that makes self-service scalable. When topic names encode ownership, the pipeline can derive ACL principals automatically without manual specification per topic.

**Naming pattern:** `{domain}.{entity}.{event}.{version}`

Examples:
- `payments.transaction.initiated.v1`
- `risk.customer.score.updated.v2`
- `notifications.email.sent.v1`

**Terraform module: topic + ACL as one unit**

```hcl
module "topic" {
  source = "./modules/kafka-topic"

  topic_name  = "payments.transaction.initiated.v1"
  domain      = "payments"       # derived from topic name
  partitions  = 12
  retention   = "604800000"

  # Module looks up the service accounts for the domain and creates ACLs
  producer_sa = local.domain_service_accounts["payments"].producer
  consumer_sa = local.domain_service_accounts["payments"].consumer
}
```

A new team onboarding means: (1) create a service account, (2) add it to `domain_service_accounts`, (3) declare topics using the team's prefix — ACLs and role bindings are created automatically by the module.

## CI/CD Pipeline Structure

```
Developer creates PR:
  changes/
    topics/payments-refund-v1.tf
    schemas/payments-refund-v1.avsc

Pipeline (on PR):
  1. terraform fmt --check          # formatting gate
  2. terraform validate             # HCL syntax
  3. terraform plan                 # show what will change
  4. confluent schema validate      # compatibility check against Schema Registry
     (or schema-registry-maven-plugin in CI)
  5. Post plan as PR comment

On merge to main:
  1. terraform apply                # apply topics, ACLs, schemas
  2. Schema registered in SR        # as part of apply
  3. ACLs updated                   # as part of apply
  4. Notify team via webhook
```

Schema compatibility check in CI catches breaking changes before they reach `terraform apply`. A failed compatibility check blocks the PR merge — this is the enforcement mechanism that makes `auto.register.schemas=false` safe at scale.

## Self-Managed Connect on Kubernetes (CFK)

For self-managed Kafka Connect deployed on EKS using **Confluent for Kubernetes (CFK)**, the GitOps pattern extends to Kubernetes manifests managed by ArgoCD or Flux.

CFK provides Kubernetes Custom Resource Definitions (CRDs) for Confluent components:

```yaml
# KafkaConnector CRD — managed by ArgoCD, applied to EKS
apiVersion: platform.confluent.io/v1beta1
kind: KafkaConnector
metadata:
  name: payments-jdbc-source
  namespace: kafka-connect
spec:
  class: io.confluent.connect.jdbc.JdbcSourceConnector
  taskMax: 4
  configs:
    connection.url: "jdbc:postgresql://rds.internal:5432/payments"
    mode: incrementing
    incrementing.column.name: id
    topic.prefix: "payments."
    poll.interval.ms: "1000"
  # Credentials injected via CSI Secrets Provider from AWS SSM
  # Not stored in the CRD
```

Credentials are never stored in the git repository or in Kubernetes Secrets directly. The CSI Secrets Provider fetches credentials from AWS Secrets Manager at pod startup and mounts them as files or environment variables.

```yaml
# CSI SecretProviderClass — fetches DB credentials from SSM
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: connect-db-credentials
spec:
  provider: aws
  parameters:
    objects: |
      - objectName: "/kafka-connect/payments-jdbc/db-password"
        objectType: "ssmparameter"
```

## ArgoCD Scope — Kubernetes Only, Not Confluent Cloud

This is a common source of confusion in platform design. ArgoCD is a Kubernetes GitOps controller — it watches Git for Kubernetes manifests and continuously reconciles them to a cluster. It does not understand Confluent Cloud resources natively.

**What ArgoCD manages (continuous reconciliation):**
- CFK `KafkaConnector` CRDs on EKS — ArgoCD detects drift and auto-corrects
- Consumer and producer `Deployment` manifests on EKS
- `SecretProviderClass` and other Kubernetes resources

**What ArgoCD does NOT manage:**
- Confluent Cloud topics, ACLs, schemas, service accounts, quotas — these are Terraform resources applied via the CI/CD pipeline on PR merge, not continuously reconciled

The distinction matters operationally: if someone manually changes a topic config via the Confluent Console, Terraform will not auto-correct it. The drift is only detected the next time `terraform plan` runs in the pipeline.

```
Confluent Cloud resources          Kubernetes / EKS resources
(topics, ACLs, schemas)            (CFK CRDs, Deployments)
         │                                   │
    Terraform                            ArgoCD
    + CI pipeline                        (continuous)
    (on PR merge)                              │
         │                                    ▼
         ▼                            Auto-corrects drift
    One-shot apply                     from desired state
```

### Continuous Reconciliation for Confluent Cloud — Crossplane

**Crossplane** is the option if continuous drift correction for Confluent Cloud resources is required. It runs as a Kubernetes controller, exposes Confluent Cloud resources as Kubernetes CRDs, and reconciles them continuously — the same model ArgoCD uses for native K8s manifests.

```yaml
# Crossplane — Confluent Cloud topic as a Kubernetes CRD
apiVersion: kafka.confluent.crossplane.io/v1alpha1
kind: Topic
metadata:
  name: payments-initiated-v1
spec:
  forProvider:
    name: payments.initiated.v1
    partitionsCount: 12
    config:
      cleanup.policy: delete
      retention.ms: "604800000"
  providerConfigRef:
    name: confluent-cloud-provider
```

ArgoCD syncs this CRD to EKS → Crossplane controller picks it up → Crossplane calls the Confluent Cloud API → drift auto-corrected if someone changes the topic manually.

**Trade-off:**

| Approach | Reconciliation | Operational cost | Drift detection |
|---|---|---|---|
| Terraform + CI | On PR merge only | Low — no extra controller | Manual `terraform plan` |
| Crossplane + ArgoCD | Continuous | High — Crossplane running in EKS | Automatic |

Crossplane is worth the overhead when many teams make frequent config changes and auto-correction of manual drift matters (e.g., regulated environments where Console access must be locked down). For most platforms, Terraform + CI pipeline on PR merge is sufficient — drift is rare when Console access is RBAC-restricted to the platform team.

## What GitOps Solves vs What It Does Not Solve

**Solves:**
- Topic and ACL drift — git is the single source of truth; drift is detectable via `terraform plan`
- Schema registration audit trail — every schema version has a git commit and PR
- Onboarding time — new team gets topics, ACLs, and schema subjects in one PR review
- Consistency across environments — same Terraform modules applied to dev, staging, prod

**Does not solve:**
- Runtime connector failures — Terraform deploys connectors; it does not manage connector health. That is an observability concern (see `11-Monitoring-Observability/`)
- Consumer group lag — not a Terraform resource
- Data quality — schema compatibility checks in CI are structural; CEL quality rules in Data Contracts handle field-level semantics (see `08-Stream-Governance/data-contracts.md`)

## Cross-References

- RBAC role bindings automated via Terraform — [09-Security-Architecture/rbac.md](../09-Security-Architecture/rbac.md)
- Private networking provisioned via Terraform — [09-Security-Architecture/private-networking.md](../09-Security-Architecture/private-networking.md)
- Schema compatibility modes — [08-Stream-Governance/schema-evolution.md](../08-Stream-Governance/schema-evolution.md)
- Data contracts registered via Terraform — [08-Stream-Governance/data-contracts.md](../08-Stream-Governance/data-contracts.md)
- Kafka Connect error handling and DLQ — [05-Enterprise-Connect/error-handling-dlq.md](../05-Enterprise-Connect/error-handling-dlq.md)
- Quota management via Terraform — [13-Performance-Tuning/quota-management.md](../13-Performance-Tuning/quota-management.md)
