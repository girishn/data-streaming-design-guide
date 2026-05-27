# KRaft Mode — ZooKeeper Removal and Controller Architecture

## Why ZooKeeper Was a Bottleneck

In legacy Kafka, Apache ZooKeeper was the external source of truth for all cluster metadata: broker registrations, topic configurations, partition assignments, and leadership state. The Kafka controller — one elected broker — read from and wrote to ZooKeeper to manage the cluster.

The failure mode that mattered: when the active controller failed, the new controller had to perform a **synchronous, full state rebuild** from ZooKeeper before it could serve requests. In large clusters with high partition counts, this recovery took tens of seconds to minutes. During that window, partition leadership was unavailable — producers and consumers stalled.

ZooKeeper also imposed a hard ceiling on cluster scale. Managing metadata for hundreds of thousands of partitions through ZooKeeper watches created latency and resource pressure that limited practical partition counts to around 200,000 per cluster.

## KRaft Architecture

KRaft (Kafka Raft) eliminates ZooKeeper by internalising metadata management into Kafka itself.

**Metadata as a log:** Cluster state is stored in a specialised internal topic named `__cluster_metadata`. Every metadata change — topic creation, partition reassignment, broker join — is appended as a record to this log. Controllers and brokers track state by reading offsets from this log, not by polling an external system.

**Controller quorum:** A dedicated set of 3 or 5 KRaft controller nodes maintains the `__cluster_metadata` log using the Raft consensus protocol. One node is elected **active controller** and handles all metadata RPCs. The remaining controllers are hot standbys — they replicate every log entry in real time and can take over immediately without rebuilding state.

**Broker metadata propagation:** Standard brokers fetch metadata updates by reading the `__cluster_metadata` log, tracking their position via offsets. There are no ZooKeeper watches; brokers pull diffs rather than receiving push notifications of full state snapshots.

## Operational Differences

**Failover speed:** Controller failover drops from minutes (ZooKeeper state rebuild) to **milliseconds** (standby controllers are already current on the log). Partition unavailability during controller elections effectively disappears.

**Partition scale ceiling:** KRaft supports up to **2 million partitions per cluster** — a 10x increase over ZooKeeper-based limits. This enables single-cluster deployments at scales that previously required federation.

**Architectural simplification:** Removing ZooKeeper eliminates a separate system to operate, monitor, secure, and upgrade. mTLS and ACL configuration no longer needs to cover ZooKeeper nodes separately from brokers.

**Combined mode is not production-supported:** KRaft technically allows a node to act as both broker and controller ("combined mode"). Confluent does not support this in production. The control plane (controller quorum) must run on dedicated nodes to prevent client workload traffic from starving metadata operations of CPU and memory.

**Rolling maintenance:** KRaft allows assigning new topic partitions to brokers that are in a controlled offline state, making cluster shrinking and rebalancing during maintenance windows more predictable than with ZooKeeper-based coordination.

## Migration from ZooKeeper to KRaft

Migration is not optional for organisations on Confluent Platform — it is **required before upgrading to Confluent Platform 8.0**. There is no path to CP 8.0 from a ZooKeeper-based cluster.

The migration process uses KIP-853 (dynamic controller quorum membership) to bootstrap KRaft controllers alongside the existing ZooKeeper-based cluster, then migrates metadata ownership before decommissioning ZooKeeper. The cluster remains operational during migration.

For Confluent Cloud clusters, KRaft is the default and ZooKeeper was never exposed — no migration is required.
