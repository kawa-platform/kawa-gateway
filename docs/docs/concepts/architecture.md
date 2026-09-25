---
title: Architecture
sidebar_position: 2
---

# Architecture

kawa is a Netty-based reverse proxy that speaks the Kafka wire protocol. It decodes requests, runs them through an
interceptor pipeline, and forwards them to the upstream cluster over dedicated broker connections.

## Request flow

```text
 client                       kawa                                Kafka cluster
   |                            |                                       |
   |-- ApiVersions ------------>| negotiate decode set                  |
   |-- Metadata --------------->| rewrite endpoints + topic names       |
   |<---------------------------| single advertised node (the gateway)  |
   |                            |                                       |
   |-- Produce orders.eu ------>| virtual -> physical rewrite           |
   |                            |-- Produce orders-v2 ----------------->| leader broker
   |                            |<-------- response --------------------|
   |<---- mapped back ----------| restore virtual names                 |
```

## Modules

| Module                | Responsibility                                                                                        |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| `kawa-config`         | Immutable YAML config model (`GatewayConfig` & friends) and `ConfigLoader`                            |
| `kawa-core`           | `Gateway` lifecycle, `Interceptor`/`InterceptorPipeline`, `Router`, metadata cache, metrics facade    |
| `kawa-protocol-kafka` | Wire protocol: `KafkaFrameDecoder`/`Encoder`, header/body codecs, `KafkaApiRegistry` version strategy |
| `kawa-virtual-topic`  | `VirtualTopicInterceptor` plus ~25 per-API transforms and consume filtering                           |
| `kawa-server`         | Netty transport: client sessions, broker connection pool, dispatch, routing                           |

Dependencies flow strictly downward: server → virtual-topic/core/protocol → config.

## Key components

### Client sessions and broker connections

Each client TCP connection gets a `ClientSession` on the Netty pipeline. Upstream, kawa maintains one pooled
`BrokerClient` per physical broker, refreshed in the background by `MetadataClient` into a shared `MetadataCache`.

### Routing

`LeaderRouter` picks the broker connection per partition leader. If leadership has moved, the broker replies
`NOT_LEADER` and the standard Kafka client retry logic triggers a fresh Metadata lookup through kawa — kawa itself does
not forward between brokers.

### Interceptor pipeline

`Interceptor` is a small SPI with `onRequest`/`onResponse` hooks; interceptors run in registration order via
`InterceptorPipeline`. The virtual-topic interceptor is the flagship implementation —
see [Virtual topics](/docs/concepts/virtual-topics).

### Protocol layer

Framing and codecs live in `kawa-protocol-kafka`. Requests are decoded only for APIs registered in `KafkaApiRegistry`;
everything else is passed through byte-for-byte. Each registered API carries an explicit `VersionRange` used during
`ApiVersions`
negotiation.

## Observability

`GatewayMetrics` (Micrometer) is instrumented at the transport and interceptor layers — counters for requests/responses,
a latency timer, byte counters, active connection gauges and virtual-topic hit counters.

## Current limitations

- Client-facing plaintext/SASL support remains separate from upstream PLAIN and provisioned MSK IAM (`SASL_SSL`) support;
  see [Authentication](/docs/concepts/authentication)
- Single upstream cluster
- No intra-gateway leader forwarding (relies on client `NOT_LEADER` retries)
- Advertised host must resolve from the client's network position
