---
title: Virtual topics
sidebar_position: 1
---

# Virtual topics

Virtual topics let clients use **virtual topic names** while the cluster only ever sees **physical topics**. The gateway
rewrites names in both directions, request and response, for every API it decodes.

```yaml
virtualTopics:
  orders.eu:          # virtual name — what clients see
    topic: orders-v2  # physical name — what exists on the cluster
```

## What the client experiences

- A producer sends to `orders.eu`; kawa rewrites the Produce request to `orders-v2`
  before forwarding.
- Metadata requests never list `orders-v2`. Instead, the physical entry is renamed to its virtual name in place — from
  the client's view, `orders.eu` is a normal topic with real partitions and leaders.
- Consumers subscribe to `orders.eu` and receive records as if nothing happened; Fetch requests are rewritten to
  `orders-v2` under the hood.

Set `exposePhysicalTopic: true` if you *do* want the physical topic to remain listed alongside its virtual name in
Metadata responses (hidden by default).

## Covered operations

Rewriting is not limited to produce/consume. Per-API transforms handle topic names in:

- Produce / Fetch / ListOffsets
- Metadata (renaming + hiding)
- OffsetCommit / OffsetFetch / OffsetDelete / DeleteRecords
- FindCoordinator, AddPartitionsToTxn, TxnOffsetCommit, DescribeTransactions
- CreateTopics / CreatePartitions / DeleteTopics
- DescribeConfigs / AlterConfigs / IncrementalAlterConfigs
- ACL operations (Create/Describe/Delete)
- Fetch session bookkeeping (`FetchSessionRegistry`) so incremental fetches stay consistent

Requests for APIs without a transform pass through untouched.

## Server-side consume filters

A virtual topic can define a consume filter that drops non-matching records during Fetch — at the gateway, before they
reach the consumer:

```yaml
virtualTopics:
  orders.eu:
    topic: orders-v2
    filter:
      type: headerEquals   # keep records where header 'region' == 'eu'
      header: region
      value: eu
```

`headerEquals` is one of a family of header predicates — `headerContains`,
`headerStartsWith`, and `headerMatches` (full-match regex) use the same `header` + `value`
shape. See [configuration](/docs/configuration#filter-headercontains) for all filter types.

Behaviour details worth knowing:

- Filtering happens server-side using Kafka's own batch filtering (`MemoryRecords.filterTo`), so it is cheap and
  preserves record framing.
- **Offsets of surviving records are unchanged.** Dropped records leave gaps in the offset sequence — consumers see the
  same offsets they would on a compacted stream, which keeps offset commits and rebalances working normally.
- The filter applies per partition of the mapped physical topic; producers can still write everything to `orders-v2`,
  and each virtual view filters independently.

### CEL expressions

For anything beyond the header predicates (`headerEquals`, `headerContains`,
`headerStartsWith`, `headerMatches`), use the `cel` filter type. It evaluates a [CEL](https://cel.dev) expression
against each record and keeps the record when the expression is `true`:

```yaml
virtualTopics:
  orders.eu:
    topic: orders-v2
    filter:
      type: cel
      expression: headers.region == "eu" && value.contains("error")
```

The expression can reference `key`, `value`, `headers` (a map), and `timestamp`. Missing headers resolve to `""`, so
`headers.region == "eu"` is simply `false` when the header is absent — use the `has(headers.region)` macro to test
presence explicitly.

CEL is non-Turing-complete and evaluates in linear time, so it is safe to run on the hot path. Expressions are compiled
once and cached; only the evaluation runs per record.

