# OPA Policy Enforcement for Kafka Platforms

## What It Is

Open Policy Agent (OPA) decouples policy decisions from policy enforcement. For a Kafka platform, policies — partition count limits, replication factor floors, naming convention validation, retention bounds — are written as Rego rules and evaluated at the enforcement point, not embedded in application code or CI scripts that drift over time.

Three enforcement points exist for Kafka, and the right choice depends on where you control the gate:

| Deployment model | Primary enforcement point |
|---|---|
| Confluent Cloud (GitOps) | `conftest` Rego policy in CI, runs against Terraform plan before `apply` |
| CFK on GKE | OPA Gatekeeper admission webhook on `KafkaTopic` CRDs |
| Self-managed CP / OSS Kafka | Broker-side `CreateTopicPolicy` Java plugin |

CI `conftest` and Gatekeeper are complementary, not alternatives. `conftest` catches violations in the PR; Gatekeeper is the last-resort gate that fires even if someone bypasses CI and applies directly to the cluster.

---

## Enforcement Point 1 — GitOps: `conftest` Against Terraform Plan

The CI pipeline generates a Terraform plan JSON and passes it to `conftest` before `terraform apply` runs. The Rego policy inspects every `confluent_kafka_topic` resource change in the plan.

This pattern works for Confluent Cloud where you have no control over the broker — the CI pipeline is the only gate available.

**Environment signal:** the CI job already knows which environment it's running for — it's the Terraform workspace/tfvars it just planned against (dev, staging, or prod each has its own Confluent Cloud environment/cluster, per `topic-design-framework.md` Layer 5). `conftest` has a built-in mechanism for exactly this — the `--data`/`-d` flag loads a separate JSON/YAML file and exposes it under `data`, decoupled from `input` (which stays the Terraform plan, untouched). Use that instead of parsing environment out of the topic name, and instead of hand-rolling a wrapper around the plan JSON.

**Rego policy (`policies/kafka_topics.rego`):**

```rego
package kafka.topics

import future.keywords.in

# Deny dev topics with more than 2 partitions
deny[msg] {
  data.environment == "dev"

  resource := input.resource_changes[_]
  resource.type == "confluent_kafka_topic"
  resource.change.actions[_] in ["create", "update"]

  topic_name := resource.change.after.topic_name
  partitions := resource.change.after.partitions_count
  partitions > 2

  msg := sprintf(
    "dev topic '%v' exceeds partition limit: got %v, max 2",
    [topic_name, partitions]
  )
}

# Deny dev topics with replication factor > 1
deny[msg] {
  data.environment == "dev"

  resource := input.resource_changes[_]
  resource.type == "confluent_kafka_topic"
  resource.change.actions[_] in ["create", "update"]

  topic_name := resource.change.after.topic_name
  rf := to_number(resource.change.after.config["replication.factor"])
  rf > 1

  msg := sprintf(
    "dev topic '%v': replication.factor must be 1 in dev (got %v)",
    [topic_name, rf]
  )
}

# Deny any topic without a retention policy explicitly set
deny[msg] {
  resource := input.resource_changes[_]
  resource.type == "confluent_kafka_topic"
  resource.change.actions[_] == "create"

  topic_name := resource.change.after.topic_name
  not resource.change.after.config["retention.ms"]

  msg := sprintf("topic '%v' must set retention.ms explicitly", [topic_name])
}

# Deny topics that violate naming convention. Valid shapes, per topic-design-framework.md
# Layer 5 (this policy must stay in sync with that file, the canonical source):
#   domain.entity.vN                          (default — event_type is a schema field)
#   domain.entity.event-type.vN               (independent events only)
#   domain.tenant_id.entity.vN                (per-tenant isolated)
#   domain.tenant_id.entity.event-type.vN     (per-tenant, independent events)
# All four shapes are: 2-4 lowercase dot-separated segments, then a literal vN version
# segment — one regex covers all of them without needing to know which shape applies.
deny[msg] {
  resource := input.resource_changes[_]
  resource.type == "confluent_kafka_topic"
  resource.change.actions[_] == "create"

  topic_name := resource.change.after.topic_name
  not regex.match(`^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*){1,3}\.v[0-9]+$`, topic_name)

  msg := sprintf(
    "topic '%v' violates naming convention: must be domain.entity.vN, with an optional tenant_id and/or event-type segment in between — see topic-design-framework.md Layer 5",
    [topic_name]
  )
}
```

**CI step (GitHub Actions):** write the environment as its own small data file and pass it via `-d`; `input` stays the plan JSON exactly as `terraform show -json` produced it:

```yaml
- name: Generate Terraform plan JSON
  run: terraform plan -out=tfplan.bin && terraform show -json tfplan.bin > tfplan.json

- name: Write environment context for conftest
  run: echo '{"environment": "'"$TF_WORKSPACE"'"}' > environment.json

- name: Enforce Kafka topic policies
  run: conftest test tfplan.json --data environment.json --policy policies/ --namespace kafka.topics
```

A policy violation fails the CI job. The PR cannot be merged until the Terraform config is corrected. The violation message appears directly in the CI log.

---

## Enforcement Point 2 — CFK on GKE: OPA Gatekeeper on `KafkaTopic` CRDs

Gatekeeper acts as a Kubernetes validating admission webhook. Every `kubectl apply` or GitOps sync that creates or updates a `KafkaTopic` CRD is evaluated against the Rego policy before the CFK operator processes it.

This is the backstop for CFK deployments — it fires even when CI is bypassed, when a developer applies directly with `kubectl`, or when an automated tool creates topics outside the GitOps pipeline.

**Step 1 — ConstraintTemplate** (defines the policy type and the Rego logic):

```yaml
apiVersion: templates.gatekeeper.sh/v1beta1
kind: ConstraintTemplate
metadata:
  name: kafkatopicpartitionlimit
spec:
  crd:
    spec:
      names:
        kind: KafkaTopicPartitionLimit
      validation:
        openAPIV3Schema:
          type: object
          properties:
            maxPartitions:
              type: integer
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package kafkatopicpartitionlimit

        violation[{"msg": msg}] {
          input.review.object.kind == "KafkaTopic"
          partitions := input.review.object.spec.partitionCount
          partitions > input.parameters.maxPartitions
          msg := sprintf(
            "KafkaTopic '%v' has %v partitions; max allowed in this namespace is %v",
            [
              input.review.object.metadata.name,
              partitions,
              input.parameters.maxPartitions
            ]
          )
        }
```

**Step 2 — Constraint** (applies the policy to a specific namespace with parameters):

```yaml
apiVersion: constraints.gatekeeper.sh/v1beta1
kind: KafkaTopicPartitionLimit
metadata:
  name: dev-partition-limit
spec:
  match:
    namespaces: ["kafka-dev"]
    kinds:
      - apiGroups: ["platform.confluent.io"]
        kinds: ["KafkaTopic"]
  parameters:
    maxPartitions: 2
```

The ConstraintTemplate is cluster-scoped and never changes. The Constraint is what changes per environment: `kafka-dev` gets `maxPartitions: 2`, `kafka-staging` gets `maxPartitions: 6`, `kafka-prod` gets no constraint (or a separate Constraint with a higher limit). Adding an environment means adding a Constraint manifest — the Rego logic is not touched.

**Multiple policies, same template:** stack additional ConstraintTemplates for other rules (replication factor, naming convention, retention). Each is independent and separately parameterisable.

---

## Enforcement Point 3 — Broker-Side `CreateTopicPolicy` (Self-Managed Only)

For self-managed Confluent Platform or OSS Kafka, the broker enforces a Java policy class on every topic creation request, regardless of how the request arrives — Admin API, `kafka-topics.sh`, or Kafka Streams internal topic creation. Clients receive `POLICY_VIOLATION` error code on rejection.

This is the strongest gate for self-managed deployments: no CI bypass, no kubectl bypass — the broker itself rejects the request.

**Not available on Confluent Cloud** — you do not control the broker configuration.

**Implementation:** the cluster this plugin runs on *is* a specific environment — a self-managed dev cluster only ever creates dev topics — so, same reasoning as the CI policy above, there's no need to parse environment out of the topic name. The limit comes from that broker's own `server.properties`, which is already environment-specific:

```java
public class TopicPolicy implements CreateTopicPolicy {

    private int maxPartitions;
    private short maxReplicationFactor;

    @Override
    public void configure(Map<String, ?> configs) {
        maxPartitions = Integer.parseInt(
            configs.getOrDefault("policy.max.partitions", "50").toString()
        );
        maxReplicationFactor = Short.parseShort(
            configs.getOrDefault("policy.max.replication.factor", "3").toString()
        );
    }

    @Override
    public void validate(RequestMetadata meta) throws PolicyViolationException {
        String topic = meta.topic();
        int partitions = meta.numPartitions() != null ? meta.numPartitions() : 1;

        if (partitions > maxPartitions) {
            throw new PolicyViolationException(String.format(
                "topic '%s': partition count %d exceeds limit %d for this cluster",
                topic, partitions, maxPartitions
            ));
        }

        short rf = meta.replicationFactor() != null ? meta.replicationFactor() : 1;
        if (rf > maxReplicationFactor) {
            throw new PolicyViolationException(String.format(
                "topic '%s': replication factor %d exceeds limit %d for this cluster",
                topic, rf, maxReplicationFactor
            ));
        }
    }

    @Override
    public void close() {}
}
```

**Broker config (`server.properties`)** — the limits themselves are what vary per environment, set independently on each cluster's own config, not derived from the topic name:

```properties
# On the dev cluster — cap both partitions and RF low:
create.topic.policy.class.name=com.yourorg.kafka.TopicPolicy
policy.max.partitions=2
policy.max.replication.factor=1

# On the prod cluster, same class, higher limits:
# policy.max.partitions=50
# policy.max.replication.factor=3
```

Package the class into a JAR and place it on the broker classpath. The broker instantiates it at startup via the default constructor.

---

## Policy Coverage Matrix

| Policy | `conftest` (CI) | Gatekeeper (CFK) | `CreateTopicPolicy` |
|---|---|---|---|
| Max partitions per env | Yes | Yes | Yes |
| Min replication factor | Yes | Yes | Yes |
| Naming convention | Yes | Yes | Yes |
| Retention.ms required | Yes | Yes | Yes |
| Blocks kubectl bypass | No | **Yes** | N/A |
| Blocks direct Admin API | No | No | **Yes** |
| Confluent Cloud support | **Yes** | Depends on CFK | No |

**Recommended combination:**
- Confluent Cloud: `conftest` in CI only
- CFK on GKE: `conftest` in CI + Gatekeeper on `KafkaTopic` CRDs
- Self-managed: `conftest` in CI + `CreateTopicPolicy` on broker

---

## What OPA Does Not Cover

- **Schema compatibility** — enforced by Schema Registry compatibility mode and the Maven/Gradle plugin in CI. OPA does not understand schema semantics.
- **Quota enforcement** — per-producer and per-consumer byte-rate quotas are a broker configuration concern. See `13-Performance-Tuning/quota-management.md`.
- **ACL correctness** — OPA can validate that an ACL Terraform resource is well-formed, but it cannot verify that the ACL grants exactly the right permissions for the service's actual access pattern. That requires a human review gate in the onboarding pipeline.
- **Runtime config drift** — OPA evaluates at admission time. A topic configuration changed directly via Admin API after creation bypasses all three enforcement points. Periodic reconciliation (Terraform state refresh, config drift alerts) is the detection mechanism.

---

## Cross-References

- GitOps Terraform two-pipeline model and CI structure — [gitops-terraform.md](gitops-terraform.md)
- Platform automation pyramid and tool selection — [platform-automation.md](platform-automation.md)
- Quota management for multi-tenant enforcement — [13-Performance-Tuning/quota-management.md](../13-Performance-Tuning/quota-management.md)
- Topic naming convention driving ACL structure — [topic-design-framework.md](../topic-design-framework.md)
