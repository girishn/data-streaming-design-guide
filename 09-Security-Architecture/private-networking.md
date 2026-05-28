# Private Networking for Confluent Cloud

Confluent Cloud Dedicated clusters support private networking so that Kafka data plane traffic stays within your private network boundary and never traverses the public internet. This is a hard requirement for financial services, healthcare, and any workload where broker traffic must originate and terminate within routable private address space.

Private networking is available on **Dedicated clusters only** — Basic and Standard clusters do not support it.

## Data Plane vs Control Plane

Understanding which traffic goes where is the foundation of Confluent Cloud network design.

```
┌─────────────────────────────────────────────────────┐
│  Data Plane (private — protected by your networking) │
│                                                      │
│  • Producer → Broker (produce records)               │
│  • Consumer → Broker (fetch records)                 │
│  • Kafka Connect workers → Broker                    │
│  • Schema Registry client requests (when SR is       │
│    co-located with the cluster on Dedicated)         │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  Control Plane (internet — Confluent-managed)        │
│                                                      │
│  • Confluent Cloud Console (Web UI)                  │
│  • Confluent CLI (environment/cluster management)    │
│  • Terraform provider API calls                      │
│  • Schema Registry REST API (when accessed directly  │
│    by operators, not by SR client library)           │
│  • Metrics API                                       │
└─────────────────────────────────────────────────────┘
```

Your applications connect to the data plane. Your operators and automation pipelines connect to the control plane. The data plane is what private networking protects — your Kafka records never leave your private network perimeter.

## Networking Options by Cloud

### AWS

| Option | How it works | Best for |
|---|---|---|
| **AWS PrivateLink** | Confluent exposes the Dedicated cluster as a VPC Endpoint Service. You create an Interface VPC Endpoint in your VPC. Traffic flows over AWS internal network, not the internet. | Most deployments — no CIDR management, one-way connection model, no route table changes |
| **VPC Peering** | Direct network path between your VPC and the Confluent-managed VPC. Bidirectional routing. | Existing peering infrastructure; CIDR blocks must not overlap |
| **AWS Transit Gateway** | Hub-and-spoke — multiple VPCs connect to the Transit Gateway, which routes to the Confluent cluster via an attachment. | Multiple VPCs needing access to the same cluster; centralised network management |

**PrivateLink is preferred for new deployments.** The one-way connection model means Confluent-managed infrastructure cannot initiate connections into your VPC. No CIDR planning is required because PrivateLink creates a local endpoint that appears within your address space. VPC Peering requires non-overlapping CIDRs across all connected VPCs — a constraint that becomes painful in large organisations.

### Azure

| Option | How it works |
|---|---|
| **Azure Private Link** | Cluster exposed as an Azure Private Link Service. You create a Private Endpoint in your VNet. Traffic routed over Microsoft network. |
| **VNet Peering** | Direct VNet-to-VNet network path. CIDR blocks must not overlap. |

### Google Cloud

| Option | How it works |
|---|---|
| **Private Service Connect** | GCP-equivalent of PrivateLink. Cluster exposed as a Published Service. You create a PSC endpoint in your VPC. |
| **VPC Peering** | Direct VPC-to-VPC path. Auto-allocated peering ranges. |

## DNS Resolution

Private networking changes the Kafka broker hostnames. When a Dedicated cluster is configured with PrivateLink or Peering, the bootstrap server and broker hostnames resolve to private IP addresses — but only if your DNS is configured to resolve them correctly.

**The problem:** Confluent Cloud broker hostnames (e.g., `pkc-xxxxx.us-east-1.aws.confluent.cloud`) resolve publicly to Confluent's public IPs. After enabling PrivateLink, you need those same hostnames to resolve to the private endpoint IPs within your VPC.

**AWS solution — Route 53 Private Hosted Zone:**

```
1. Confluent provides the broker hostname pattern for your cluster
2. Create a Route 53 Private Hosted Zone for the cluster's domain
3. Add CNAME or A records pointing the broker hostnames to your PrivateLink endpoint IPs
4. Associate the hosted zone with your VPCs
5. Kafka clients in those VPCs now resolve broker hostnames to private IPs
```

**CoreDNS (Kubernetes / EKS):**

If your Kafka clients run in Kubernetes, CoreDNS handles in-cluster DNS. Configure a CoreDNS stub zone or rewrite rule to forward broker hostname resolution to your Route 53 Private Hosted Zone or internal DNS server:

```yaml
# CoreDNS ConfigMap snippet
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
data:
  Corefile: |
    .:53 {
        errors
        health
        ready
        kubernetes cluster.local in-addr.arpa ip6.arpa {
          pods insecure
          fallthrough in-addr.arpa ip6.arpa
        }
        # Forward Confluent broker hostnames to your internal DNS / Route53 resolver
        forward pkc-xxxxx.us-east-1.aws.confluent.cloud 10.0.0.2
        forward .confluent.cloud 10.0.0.2
        cache 30
        loop
        reload
        loadbalance
    }
```

Where `10.0.0.2` is your VPC DNS resolver (Route 53 Resolver endpoint or equivalent).

## Schema Registry and Control Plane Access

On a Confluent Cloud Dedicated cluster, Schema Registry is co-located — its data plane traffic (schema lookups by the SR client library embedded in your producers/consumers) goes over the same private network path as the broker traffic.

However, direct REST API access to Schema Registry (by operators, CI/CD pipelines, Terraform) goes through the control plane — a public internet endpoint. This is by design. The Schema Registry client library in your application uses the private path; operator tooling uses the public control plane.

If your security policy prohibits any outbound internet access from application hosts, the Schema Registry client library must be configured to use the private SR endpoint. Confluent provides a private Schema Registry endpoint for Dedicated clusters alongside the private Kafka bootstrap — check cluster networking settings.

## Outbound Connectivity for Self-Managed Connect

Self-managed Kafka Connect clusters (e.g., on EKS using CFK) sit in your private network and need:

1. **Inbound from source systems** (or outbound to sink systems) — connector-specific, stays within your private network
2. **Outbound to Confluent Cloud data plane** — via PrivateLink / Peering (private)
3. **Outbound to Confluent Cloud control plane** — via internet for Connect REST API registration and offset management
4. **Outbound to AWS SSM / Secrets Manager** — for credential retrieval via CSI Secrets Provider

This is the network topology the spec describes: Connect workers in EKS (private) → broker via PrivateLink (private) → source/sink systems (private). Control plane traffic (registering connectors, checking status) exits to the internet.

## Decision: PrivateLink vs VPC Peering vs Transit Gateway

| Concern | PrivateLink | VPC Peering | Transit Gateway |
|---|---|---|---|
| CIDR overlap risk | None — endpoint appears in your VPC address space | High — non-overlapping CIDRs required across all peered VPCs | Medium — Transit Gateway has its own attachment CIDRs |
| Connection direction | One-way — Confluent cannot initiate to your VPC | Bidirectional | Configurable per route |
| Latency | Minimal overhead | Minimal overhead | Slight overhead per hop |
| Multiple VPC access | One endpoint per VPC | One peering per VPC pair | Single attachment covers all VPCs via TGW routing |
| Cost | Per-endpoint + data transfer | Data transfer only | TGW attachment + data transfer |
| Preferred for | Most new deployments | Existing peering infrastructure | Centralised multi-VPC networking |

## Cross-References

- mTLS and OAuth/OIDC over private endpoints — [09-Security-Architecture/mtls-oauth.md](mtls-oauth.md)
- RBAC and service account setup — [09-Security-Architecture/rbac.md](rbac.md)
- Self-managed Kafka Connect on Kubernetes — [05-Enterprise-Connect/managed-connectors.md](../05-Enterprise-Connect/managed-connectors.md)
- GitOps Terraform for networking configuration — [10-Operational-Patterns/gitops-terraform.md](../10-Operational-Patterns/gitops-terraform.md)
