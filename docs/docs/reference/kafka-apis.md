---
title: Kafka APIs
sidebar_position: 1
---

# Supported Kafka APIs

kawa decodes requests for the APIs below so interceptors can inspect and rewrite them (virtual topic mapping, endpoint
rewriting, consume filtering). Requests for any **unregistered API pass through to the broker un-inspected**.

| API key | Name                    | Supported versions |
|--------:|-------------------------|--------------------|
|       0 | Produce                 | 0–8                |
|       1 | Fetch                   | 0–11               |
|       2 | ListOffsets             | 0–5                |
|       3 | Metadata                | 0–8                |
|       8 | OffsetCommit            | 0–7                |
|       9 | OffsetFetch             | 0–5                |
|      10 | FindCoordinator         | 0–2                |
|      18 | ApiVersions             | 0–3                |
|      19 | CreateTopics            | 0–7                |
|      20 | DeleteTopics            | 0–6                |
|      21 | DeleteRecords           | 0–2                |
|      22 | InitProducerId          | all¹               |
|      24 | AddPartitionsToTxn      | 0–3                |
|      28 | TxnOffsetCommit         | 0–4                |
|      29 | DescribeAcls            | 0–3                |
|      30 | CreateAcls              | 0–3                |
|      31 | DeleteAcls              | 0–3                |
|      32 | DescribeConfigs         | 0–4                |
|      33 | AlterConfigs            | 0–2                |
|      37 | CreatePartitions        | 0–3                |
|      44 | IncrementalAlterConfigs | 0–1                |
|      47 | OffsetDelete            | 0–0                |
|      65 | DescribeTransactions    | 0–0                |
|      68 | ConsumerGroupHeartbeat  | all¹               |

¹ Every stable version of the bundled Kafka client library.

The supported version range is what kawa can *decode*; it does not restrict which protocol versions clients may
negotiate. Within these ranges kawa rewrites topic names in both directions — e.g. Produce, Fetch, Metadata,
OffsetCommit/Fetch, FindCoordinator, CreateTopics, DeleteTopics, ACL operations, transactions and more (roughly 25
per-API transforms in `kawa-virtual-topic`).

## Version negotiation

On connect, clients send an `ApiVersions` request. kawa answers with its decode set, so clients pin each API to a
version kawa understands.

## Extending the decode set

Adding a new API is deliberately cheap:

1. Register a `KafkaApiSpec` (api key, name, `VersionRange`, request/response message factories) in `KafkaApiRegistry`.
2. Add one transform class in `kawa-virtual-topic` and register it in the
   `VirtualTopicTransformRegistry`.

APIs that need no virtual-topic awareness work with step 1 alone.
