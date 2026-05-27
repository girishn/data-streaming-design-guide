# Static Membership — Rebalance Stability for Kubernetes Deployments

## The Rolling Restart Problem

In dynamic environments, every consumer restart generates a new ephemeral `member.id`. From the Group Coordinator's perspective, this looks like a member leaving and an entirely new member joining — two separate rebalances per restart. A rolling deployment of 10 consumer pods triggers 20 rebalances. Even with cooperative rebalancing, each pod restart causes at least a brief partition handoff cycle for the partitions that pod held.

In practice this means:
- Consumer lag spikes during every deployment
- State store rebuilds on reassigned partitions (for stateful consumers)
- Elevated end-to-end latency throughout the rollout window
- Operations teams treating routine deployments as risk events

## Static Membership with `group.instance.id`

Static membership allows a consumer to rejoin a group with a **persistent identity** that survives restarts.

```properties
group.instance.id=my-consumer-app-0
```

When a static member restarts and rejoins within `session.timeout.ms`:

1. The coordinator recognises the `group.instance.id` as an existing member
2. It restores the member's prior partition assignment without initiating a rebalance
3. Processing resumes from the last committed offset with no partition handoff

The key invariant: the `group.instance.id` must be **unique within the group** and **stable across restarts**. Two consumers sharing the same ID causes the coordinator to fence the older one — the same zombie-fencing mechanism used by transactional producers.

## The Failure Detection Trade-Off

Static membership shifts the rebalance trigger from `member.id` change to `session.timeout.ms` expiry. This is the central trade-off:

| Scenario | Dynamic membership | Static membership |
|---|---|---|
| Pod restarts within 30s | 2 rebalances | 0 rebalances |
| Pod crashes permanently | Rebalance after `session.timeout.ms` | Rebalance after `session.timeout.ms` |
| Pod slow but alive | Rebalance if heartbeat missed | Same |

For static membership to absorb planned restarts, `session.timeout.ms` must be long enough to cover the full restart cycle — container shutdown, image pull if needed, JVM startup, and Kafka client initialisation. A typical target is **45,000–90,000ms** depending on application startup time.

The cost: when a consumer genuinely crashes and cannot recover, its partitions remain unprocessed for the full `session.timeout.ms` before the coordinator redistributes them. Consumer lag accumulates during that window. This is acceptable for planned deployments but must be weighed against the criticality of the workload — a 90-second lag spike on crash may be unacceptable for latency-sensitive pipelines.

Set `session.timeout.ms` to `max(expected restart time, acceptable lag window on crash)`. There is no setting that eliminates both rebalance disruption and fast failure detection simultaneously.

## Kubernetes Configuration

**Use StatefulSets, not Deployments.** StatefulSets assign stable, predictable hostnames to pods (`app-0`, `app-1`, ...) that persist across restarts. Deployments generate random pod names — mapping `group.instance.id` to the pod hostname would produce a new value on every restart, defeating static membership entirely.

**Map `group.instance.id` to the pod hostname:**

```yaml
env:
  - name: HOSTNAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name   # e.g., my-consumer-0
```

```properties
group.instance.id=${HOSTNAME}
```

**Align termination grace period with `session.timeout.ms`:** Kubernetes sends `SIGTERM` and waits `terminationGracePeriodSeconds` before sending `SIGKILL`. If the consumer shuts down cleanly within this window — committing offsets and sending a LeaveGroup RPC — the coordinator immediately frees its partitions without waiting for the session timeout. Set `terminationGracePeriodSeconds` to cover your application's graceful shutdown time (typically 15–30s) and ensure your shutdown hook calls `consumer.close()`.

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    consumer.wakeup();  // unblock poll()
}));
// in poll loop, catch WakeupException and call consumer.close()
```

A clean LeaveGroup during shutdown means static membership's slow failure detection only applies to genuine crashes — not planned pod terminations. This is the configuration that makes static membership effective in practice.

## Interaction with Cooperative Rebalancing

Static membership and cooperative rebalancing are complementary and should be used together:

- **Static membership** eliminates rebalances caused by planned restarts
- **Cooperative rebalancing** minimises disruption when rebalances do occur (genuine failures, scaling events)

Configure both:

```properties
group.instance.id=${HOSTNAME}
session.timeout.ms=60000
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
