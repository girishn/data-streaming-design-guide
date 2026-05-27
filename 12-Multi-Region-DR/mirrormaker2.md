# MirrorMaker 2 — Framework-Based Cross-Cluster Replication

## Architecture

MirrorMaker 2 (MM2) is an Apache Kafka project built on the Kafka Connect framework. It runs as a set of connectors on a Connect worker cluster and replicates data between Kafka clusters using a consumer-producer model: a source connector consumes records from the source cluster and a producer re-publishes them to the destination cluster.

Because MM2 re-produces records rather than replicating bytes directly, destination offsets are independent of source offsets. A record at offset 5000 on the source may land at offset 4998 or 5001 on the destination depending on the history of each cluster. This offset divergence is the fundamental operational difference from Cluster Linking and drives the additional complexity of consumer group migration at failover time.

**Core connectors MM2 deploys:**
- `MirrorSourceConnector` — replicates topic data from source to destination
- `MirrorCheckpointConnector` — periodically writes consumer group offset translations to the destination cluster
- `MirrorHeartbeatConnector` — writes heartbeat records to measure replication lag

## Topic Naming and Namespace Isolation

By default, MM2 prefixes replicated topic names with the source cluster alias to avoid collision in active-active setups:

```
Source cluster alias: us-east
Source topic:         orders
Destination topic:    us-east.orders
```

This prefixing is configurable via `replication.policy.class`. The default `DefaultReplicationPolicy` applies the prefix. `IdentityReplicationPolicy` preserves the original topic name — useful for active-passive failover where consumers should seamlessly switch clusters without topic name changes.

```properties
# mm2.properties
clusters = us-east, eu-west
us-east.bootstrap.servers = broker-us.company.com:9092
eu-west.bootstrap.servers = broker-eu.company.com:9092

us-east->eu-west.enabled = true
us-east->eu-west.topics = orders, payments, inventory

# Preserve source topic names (active-passive pattern)
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy
```

## Offset Translation

MM2's `MirrorCheckpointConnector` periodically records a mapping of source consumer group offsets to destination offsets in a topic called `{source-alias}.checkpoints.internal` on the destination cluster. This translation table allows consumers to find their approximate position on the destination after failover.

The translation is approximate, not exact. Because MM2 re-produces records, offsets diverge. The checkpoint records the source offset and the corresponding destination offset at the time of the checkpoint. A consumer resuming from a translated offset picks up at the nearest destination offset, which may re-process a small number of records (the gap since the last checkpoint write).

**Checkpoint interval:** controlled by `emit.checkpoints.interval.seconds`. Shorter intervals reduce the re-processing window at failover but increase metadata write volume.

## Consumer Group Migration at Failover

MM2 does not automatically migrate consumer groups. At failover, consumers must be told where to start on the destination cluster. Two approaches:

**Option 1 — `MirrorClient` offset translation (programmatic):**
```java
// At failover, translate committed offsets from source to destination
MirrorClient mirrorClient = new MirrorClient(mm2Config);

Map<TopicPartition, OffsetAndMetadata> sourceOffsets =
    adminClient.listConsumerGroupOffsets("my-group")
               .partitionsToOffsetAndMetadata().get();

Map<TopicPartition, OffsetAndMetadata> destinationOffsets =
    mirrorClient.translateOffsets(sourceOffsets, "us-east");

// Commit translated offsets on destination before starting consumers
adminClient.alterConsumerGroupOffsets("my-group", destinationOffsets).all().get();
```

**Option 2 — `ConsumerTimestampsInterceptor` (automatic):**
Configure consumers with the timestamp-based interceptor. At startup on the destination, the interceptor reads the checkpoint topic to find the timestamp of the last committed offset on the source and seeks the destination consumer to the first record at or after that timestamp.

```properties
# Consumer config
interceptor.classes=org.apache.kafka.connect.mirror.MirrorConsumerInterceptor
```

Both approaches are at-least-once at failover — some records will be re-processed. Consumers must be idempotent.

## Replication Lag Monitoring

The `MirrorHeartbeatConnector` writes timestamped records to `heartbeats` topics on the source and destination. Replication lag is the difference between the heartbeat timestamp and the time it appears on the destination:

```bash
# Check replication lag via MirrorClient
MirrorClient client = new MirrorClient(mm2Config);
Map<TopicPartition, Duration> lag = client.replicationLag("us-east");
```

This is the operational signal for RPO estimation: if replication lag is 45 seconds, the maximum data loss window in a sudden source failure is 45 seconds.

Monitor `kafka.connect:type=MirrorSourceConnector,name=replication-latency-ms-avg` via JMX for alerting.

## Operational Considerations

**Connect infrastructure overhead:** MM2 requires a running Kafka Connect cluster, which is an additional operational surface — worker JVM tuning, connector task distribution, plugin management. For teams already running Connect for other connectors, this cost is marginal. For teams running Connect solely for MM2, the overhead may favour Cluster Linking if Confluent is available.

**Back-pressure and throughput:** MM2 is bounded by the Connect worker's consumer and producer throughput. At very high replication rates, the worker cluster may need to be sized with more tasks (`tasks.max`) and more worker instances. MM2 does not have the direct replication path that Cluster Linking's native fetcher protocol provides.

**OSS Kafka compatibility:** MM2 works with any Kafka cluster — no Confluent license required on source or destination. This is its primary advantage over Cluster Linking in heterogeneous environments.

## When to Choose MirrorMaker 2

- Source or destination cluster runs OSS Apache Kafka without Confluent Server
- Active-active bidirectional replication with topic namespace isolation (the prefix avoids mirroring loops)
- Existing Connect infrastructure already in operation
- Tolerance for approximate offset translation and at-least-once re-processing at failover
- Need for fine-grained topic filtering, transformation via SMTs, or connector-level monitoring

For offset-exact failover with second-scale RTO and a Confluent cluster on both sides, see `12-Multi-Region-DR/cluster-linking.md`.
