# Stream Lineage — End-to-End Data Flow Visibility

## What Stream Lineage Provides

Stream Lineage is an interactive graph that maps the movement of data through the Kafka ecosystem — from source producers, through topics, into stream processors and connectors, and out to sink systems. The graph is directional and queryable: given a topic, it shows what produces into it and what consumes from it, transitively across the entire pipeline.

The primary engineering value is **visibility without instrumentation**. In organisations with dozens of teams each owning different parts of a pipeline, understanding the full data flow typically requires reading documentation, source code, and connector configurations across multiple systems. Stream Lineage makes the actual runtime topology available in a single view.

## How the Graph Is Built

The lineage graph is constructed automatically from metadata observed through broker activity — no client-side code changes or manual diagramming required.

**Sources of lineage metadata:**
- Producer client metadata: application name, topic written to
- Consumer group metadata: group ID, topics consumed
- Kafka Connect task metadata: connector name, source/sink topic bindings
- Flink and ksqlDB job metadata: input topics, output topics, transformation names

The platform stitches these into a directed graph: node types are producers, topics, processors (Flink jobs, ksqlDB queries, Connect connectors), and sinks. Edges represent data movement — "this connector reads from topic A and writes to topic B."

The graph reflects real-time activity. A node that was active recently appears in the lineage; a node that has not produced or consumed within the observation window may not.

## Engineering Use Cases

**Impact analysis before schema changes:** before evolving a schema on a high-traffic topic, engineers use Stream Lineage to enumerate every downstream consumer and intermediate processor. A breaking change to an upstream topic schema without checking lineage is the most common cause of cascading pipeline failures. Lineage makes the blast radius visible before the change is deployed.

**Debugging data quality regressions:** when a downstream consumer reports unexpected data — wrong values, missing fields, unexpected nulls — lineage provides a structured path for root cause analysis. Starting from the affected sink, trace back through each transformation node to identify where the value was last correct. Each node in the graph can be drilled into to see its schema, configuration, and recent throughput.

**PII compliance audits:** regulatory audits require demonstrating that PII data is handled correctly from source to destination. Stream Lineage combined with Stream Catalog PII tags produces an auditable answer to "show us everywhere this user's data flows." The lineage graph shows the path; the tags on each node confirm what governance controls apply at each stage. See `08-Stream-Governance/pii-tracking.md` for the tagging model.

**Decommissioning safely:** before deleting a topic or shutting down a connector, lineage confirms whether any active downstream consumers would be broken. Topics that appear as sources to no active downstream nodes are safe to decommission. Topics with active downstream consumers require coordination.

## Limitations

**Inactive clients are not visible:** lineage is built from observed activity. A consumer group that has not polled recently — because it is paused, experiencing lag, or intentionally offline — may not appear as an active downstream node. Lineage shows the runtime topology, not the configured topology. A consumer that should be running but is not will be missing from the graph.

**External system opacity:** lineage tracks data movement to and from Kafka. Once data is consumed from Kafka into an external application, Stream Lineage cannot follow it — it does not know what the application does with the data internally, what databases it writes to, or what downstream APIs it calls. The graph boundary is the Kafka ecosystem. For end-to-end lineage across external systems, integrate with a broader data catalogue or data observability platform.

**No historical snapshots by default:** the lineage graph reflects current activity. If a connector was active last week but has since been deleted, it does not appear in the current lineage. Point-in-time lineage queries (useful for incident post-mortems) are not available unless the organisation maintains their own snapshots of the lineage API output.
