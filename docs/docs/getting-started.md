---
title: Getting started
sidebar_position: 2
---

# Getting started

The fastest way to run kawa is the bundled Docker Compose setup, which starts a Kafka broker and a gateway. The gateway
boots **empty**: no virtual topics are configured and RBAC is **default-deny**, so nothing can be produced or consumed
until dynamic config is applied (see [Dynamic config](/docs/configuration#dynamic-config)).

## Prerequisites

- Docker (with Compose v2)
- Kafka command-line tools, including `kafka-console-producer.sh` (available on your `PATH`)

## Run the demo stack

```bash
make up
```

This always rebuilds the gateway image and force-recreates the containers, so code and config changes are picked up
reliably. (A plain `docker compose up` can silently keep running a stale image or container.) The SASL-enabled variant
is `make up-sasl`; see
`make help` for the full list of targets.

This starts three services:

| Service      | Image                             | Port                                               |
|--------------|-----------------------------------|----------------------------------------------------|
| `kafka`      | `apache/kafka-native:4.3.1`       | `19092` on the host (inspection only)              |
| `kafka-init` | `apache/kafka:4.3.1`              | creates the physical topic `orders-v2`, then exits |
| `gateway`    | built from this repo's Dockerfile | `9092` — **connect your clients here**             |

The gateway is mounted with [`docker/gateway.yaml`](https://GITHUB_URL/blob/main/docker/gateway.yaml), which carries
only startup-only settings:

- a listener on `0.0.0.0:9092`
- an upstream cluster pointing at `kafka:9092`
- an advertised listener at `localhost:9092`
- an admin API on `0.0.0.0:8080`, protected by kawa's built-in OAuth login with the demo user `admin` / `admin`,
  signed in through kawa studio on `http://localhost:5173`
  (see [Admin API authentication](/docs/concepts/authentication#admin-api-authentication))

Virtual topics, RBAC, client authentication and governance are not in this file — they are dynamic
(see [Dynamic config](/docs/configuration#dynamic-config)).

## Configure a virtual topic and produce through the gateway

The `orders.eu` virtual topic no longer ships in the static config. Configure virtual topics at runtime — via the admin
API or by writing a config-topic snapshot (see
[Admin API](/docs/configuration#admin-api)). This maps `orders.eu` to the physical topic
`orders-v2` created by `kafka-init`. Admin API calls need an access token. kawa issues tokens only through a browser
login (the OAuth authorization code flow), so sign in to kawa studio as `admin` / `admin` and copy the token from the
browser console:

```js
JSON.parse(localStorage.getItem('kawa.auth')).accessToken
```

```bash
TOKEN=<the copied token>
```

The token is valid for 15 minutes; copy a fresh one when calls start returning `401`.

```bash
curl -X POST http://localhost:8080/topics \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"type": "virtual", "name": "orders.eu", "topic": "orders-v2"}'
```

Because RBAC is **default-deny** and client auth is empty on first boot, producing also needs grants. The following
creates a PLAIN client with broad demo access:

```bash
curl -X PUT http://localhost:8080/auth/clients/alice \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mechanism":"PLAIN","password":"secret"}'
curl -X PUT http://localhost:8080/rbac/roles/allow-all \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"acls":[{"resource":{"type":"TOPIC","pattern":"","patternType":"PREFIXED"},"operation":"ALL"},{"resource":{"type":"GROUP","pattern":"","patternType":"PREFIXED"},"operation":"ALL"},{"resource":{"type":"CLUSTER"},"operation":"ALL"}]}'
curl -X PUT http://localhost:8080/rbac/groups/demo \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"clients":["alice"],"roles":["allow-all"]}'
```

For a Kafka CLI running on the host, create client properties:

```bash
cat > /tmp/kawa-client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";
EOF
```

Then point the Kafka client at `localhost:9092` (the advertised address in
`docker/gateway.yaml`) and use the virtual topic name:

```bash
# Produce to the VIRTUAL topic via the gateway
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --command-config /tmp/kawa-client.properties \
  --topic orders.eu <<'EOF'
{"orderId": "1", "region": "eu"}
EOF
```

```bash
# The cluster only ever sees the PHYSICAL topic; orders.eu is virtual
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
# -> orders-v2   (orders.eu never existed on the cluster)
```

## Run the gateway without Docker

Build the shaded fat jar and point it at a config file:

```bash
./mvnw -pl kawa-server -am package
java -jar kawa-server/target/kawa-server-*.jar --config path/to/config.yaml
```

If `--config` is omitted, kawa looks for `config.yaml` in the working directory.

## Verify it works

With the stack running, connect any Kafka tool to `localhost:9092`. Metadata requests return a single broker — the
gateway itself — advertising `localhost:9092`, so every subsequent connection also lands on kawa instead of the real
cluster.
