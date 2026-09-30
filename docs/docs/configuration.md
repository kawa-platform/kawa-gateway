---
title: Configuration
sidebar_position: 3
---

# Configuration

kawa is configured with a single YAML file, loaded at startup:

```bash
java -jar kawa-server.jar --config /path/to/config.yaml   # default: ./config.yaml
```

Unknown properties are ignored. Every field is optional unless stated otherwise.

The file is the **static bootstrap**: it carries the startup-only configuration (`listeners`, `clusters`, `advertised`,
`admin`, `auth.brokerAuth`, `configTopic`). Virtual topics, RBAC, client authentication and governance are **dynamic**
and are read from the config topic (see [Dynamic config](#dynamic-config)); an empty config topic on first boot is valid
and the gateway boots with no virtual topics, default-deny RBAC, no client auth, and no governance rules.

## Full example

```yaml
listeners:
  - host: 0.0.0.0
    port: 9092

clusters:
  default:
    bootstrapServers:
      - kafka:9092

auth:
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"

advertised:
  nodeId: 1
  host: localhost
  port: 9092

admin:
  enabled: true
  host: 0.0.0.0
  port: 8080
  cors:
    allowedOrigins:
      - http://localhost:8080

configTopic: __kawa
```

## Reference

### `listeners[]`

Client-facing listeners. The gateway accepts client connections here.

| Field  | Type   | Default   | Description                            |
|--------|--------|-----------|----------------------------------------|
| `host` | string | `0.0.0.0` | Bind address                           |
| `port` | int    | `9092`    | Bind port; `0` binds an ephemeral port |

The first listener is the default and determines the advertised port when
[`advertised.port`](#advertised) is unset.

### `clusters`

Map of cluster key → upstream Kafka cluster. The **first entry is the default cluster** that traffic is forwarded to
(milestone 1 supports a single cluster).

| Field              | Type        | Default      | Description                                             |
|--------------------|-------------|--------------|---------------------------------------------------------|
| `bootstrapServers` | string list | *(required)* | `host:port` list used for the initial broker connection |

```yaml
clusters:
  default:
    bootstrapServers:
      - broker1.internal:9092
      - broker2.internal:9092
```

### `virtualTopics`

Dynamic — see [Dynamic config](#dynamic-config).

Map of virtual name → virtual topic definition. The map key is the topic name clients see and use.

| Field                 | Type   | Default      | Description                                                                                  |
|-----------------------|--------|--------------|----------------------------------------------------------------------------------------------|
| `topic`               | string | *(required)* | Physical topic name on the upstream cluster                                                  |
| `filter`              | object | none         | Optional server-side consume filter                                                          |
| `exposePhysicalTopic` | bool   | `false`      | When `true`, the physical topic stays visible in Metadata responses next to its virtual name |

#### `filter` (`headerEquals`)

Keeps only records whose record header matches during Fetch. Non-matching records are dropped by the gateway while
preserving offsets of surviving records.

| Field    | Type   | Description            |
|----------|--------|------------------------|
| `type`   | string | Must be `headerEquals` |
| `header` | string | Header key to compare  |
| `value`  | string | Required header value  |

#### `filter` (`headerContains`)

Keeps only records whose record header value contains the configured substring during Fetch.

| Field    | Type   | Description                             |
|----------|--------|-----------------------------------------|
| `type`   | string | Must be `headerContains`                |
| `header` | string | Header key to compare                   |
| `value`  | string | Substring the header value must contain |

#### `filter` (`headerStartsWith`)

Keeps only records whose record header value starts with the configured prefix during Fetch.

| Field    | Type   | Description                             |
|----------|--------|-----------------------------------------|
| `type`   | string | Must be `headerStartsWith`              |
| `header` | string | Header key to compare                   |
| `value`  | string | Prefix the header value must start with |

#### `filter` (`headerMatches`)

Keeps only records whose record header value fully matches the configured regular expression (anchored, like Java's
`String.matches`). The regex is validated at config load time, so an invalid pattern fails startup rather than the first
Fetch.

| Field    | Type   | Description                                          |
|----------|--------|------------------------------------------------------|
| `type`   | string | Must be `headerMatches`                              |
| `header` | string | Header key to compare                                |
| `value`  | string | Regular expression the header value must fully match |

#### `filter` (`cel`)

Keeps only records for which a [CEL](https://cel.dev) (Common Expression Language)
expression evaluates to `true`. The expression is compiled once at first use and evaluated per record, so it is cheap
even on high-throughput topics.

| Field        | Type   | Description                        |
|--------------|--------|------------------------------------|
| `type`       | string | Must be `cel`                      |
| `expression` | string | CEL expression returning a boolean |

The expression has access to these record bindings:

| Binding     | Type                    | Notes                                                   |
|-------------|-------------------------|---------------------------------------------------------|
| `key`       | string                  | `""` when the record has no key                         |
| `value`     | string                  | `""` when the record has no value                       |
| `headers`   | map of string to string | A missing header resolves to `""` (falsy, not an error) |
| `timestamp` | int                     | Record timestamp in milliseconds                        |

```yaml
virtualTopics:
  orders.eu:
    topic: orders-v2
    filter:
      type: cel
      expression: headers.region == "eu" && value.contains("error")
```

See [Virtual topics](/docs/concepts/virtual-topics) for behaviour details.

### `auth`

Client SASL authentication configuration (`mechanisms`/`clients`) is **dynamic** — see
[Dynamic config](#dynamic-config). `auth.brokerAuth` below is startup-only.

When configured, clients must authenticate using the standard Kafka SASL handshake (`SaslHandshake` +
`SaslAuthenticate`).

| Field        | Type        | Default   | Description                           |
|--------------|-------------|-----------|---------------------------------------|
| `mechanisms` | string list | *(empty)* | SASL mechanisms advertised to clients |
| `clients`    | map         | *(empty)* | Client credentials                    |

Each client entry requires at minimum a `password`. If `mechanism` is omitted, the client inherits the first mechanism
from the global `mechanisms` list. If no global mechanism is configured, an error is raised at startup.

```yaml
auth:
  mechanisms:
    - PLAIN
    - SCRAM-SHA-256
    - SCRAM-SHA-512
  clients:
    alice:                          # inherits PLAIN
      password: "${ALICE_PASSWORD}"
    bob:
      mechanism: SCRAM-SHA-256      # explicit override
      password: "${BOB_PASSWORD}"
```

See [Authentication](/docs/concepts/authentication) for the full authentication model.

#### `auth.brokerAuth`

Upstream broker SASL authentication. When set, the gateway authenticates to the Kafka cluster using these credentials
instead of connecting in plaintext.

| Field       | Type   | Required | Description                                |
|-------------|--------|----------|--------------------------------------------|
| `mechanism` | string | yes      | SASL mechanism (currently `PLAIN` only)    |
| `username`  | string | yes      | Broker SASL username                       |
| `password`  | string | yes      | Plain-text or `${VAR}` / `${VAR:-default}` |

```yaml
auth:
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"
```

The gateway authenticates to the broker during the initial connection handshake (`SaslHandshake` + `SaslAuthenticate`),
before forwarding any client requests. This is transparent to clients — they authenticate to the gateway independently.

### `rbac`

Dynamic — see [Dynamic config](#dynamic-config).

Role-based access control. kawa checks each request against the principal's ACLs before forwarding it to the cluster.
RBAC is **always enforced** — there is no way to disable it, and a gateway with no roles or groups configured denies
every request from every client. RBAC is **default-deny**: a request is only allowed if at least one matching ACL grants
it, and any matching deny wins.

| Field    | Type | Default   | Description                                                           |
|----------|------|-----------|-----------------------------------------------------------------------|
| `roles`  | map  | *(empty)* | Named roles, each a list of ACLs                                      |
| `groups` | map  | *(empty)* | Named groups, each a client list plus the roles those clients inherit |

A user's effective ACLs are the union of every role referenced by every group they belong to.

```yaml
rbac:
  roles:
    producer:
      acls:
        - resource:
            type: TOPIC
            pattern: orders
          operation: WRITE
    admin:
      acls:
        - resource:
            type: CLUSTER
          operation: CREATE
  groups:
    producers:
      clients: [alice]
      roles: [producer]
    admins:
      clients: [bob]
      roles: [admin]
```

#### `rbac.roles.<name>.acls[]`

Each ACL grants or denies one operation on one resource.

| Field        | Type   | Default      | Description                                                |
|--------------|--------|--------------|------------------------------------------------------------|
| `resource`   | object | *(required)* | The resource the ACL applies to                            |
| `operation`  | string | *(required)* | `WRITE`, `READ`, `CREATE`, `DELETE`, `ALL`, ...            |
| `permission` | string | `ALLOW`      | `ALLOW` or `DENY`; a matching `DENY` wins over any `ALLOW` |

#### `rbac.roles.<name>.acls[].resource`

| Field         | Type   | Default                      | Description                                                                                                                   |
|---------------|--------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `type`        | string | *(required)*                 | `TOPIC`, `GROUP` or `CLUSTER`                                                                                                 |
| `pattern`     | string | *(required for TOPIC/GROUP)* | Resource name; ignored for `CLUSTER`. May be empty (`""`) when `patternType` is `PREFIXED` to match any resource of that type |
| `patternType` | string | `LITERAL`                    | `LITERAL` (exact match) or `PREFIXED` (name prefix)                                                                           |

#### `rbac.groups.<name>`

| Field     | Type        | Description                            |
|-----------|-------------|----------------------------------------|
| `clients` | string list | Authenticated usernames in this group  |
| `roles`   | string list | Roles whose ACLs every client inherits |

See [Access control (RBAC)](/docs/concepts/rbac) for the full model, what is enforced today, and how unauthenticated
requests are handled.

### `advertised`

The endpoint kawa advertises to clients in rewritten Metadata and FindCoordinator responses — effectively "the broker"
every client will connect to.

| Field    | Type   | Default               | Description                                                      |
|----------|--------|-----------------------|------------------------------------------------------------------|
| `nodeId` | int    | `1`                   | Broker node id advertised to clients                             |
| `host`   | string | `localhost`           | Host clients connect to                                          |
| `port`   | int    | first listener's port | Port clients connect to; `0` means "use the bound listener port" |

:::warning Choose the host from the client's point of view All broker endpoints are rewritten to this value, so
`advertised.host` must resolve **from the client's perspective**. Inside Docker Compose the gateway config uses
`localhost` because the port is published to the host where the clients run.
:::

### `admin`

The admin HTTP surface exposing gateway state (e.g. `GET /topics`) to a UI.

| Field     | Type   | Default      | Description                                                            |
|-----------|--------|--------------|------------------------------------------------------------------------|
| `enabled` | bool   | `false`      | Whether the admin HTTP server is started                               |
| `host`    | string | `0.0.0.0`    | Bind address                                                           |
| `port`    | int    | `8080`       | Bind port; `0` binds an ephemeral port                                 |
| `cors`    | object | *(disabled)* | CORS configuration for browser-based UIs served from another host/port |

```yaml
admin:
  enabled: true
  host: 0.0.0.0
  port: 8080
  cors:
    allowedOrigins:
      - http://localhost:8080
```

#### `admin.cors`

| Field              | Type        | Default                                                | Description                                                       |
|--------------------|-------------|--------------------------------------------------------|-------------------------------------------------------------------|
| `allowedOrigins`   | string list | `["*"]`                                                | Origins allowed to call the admin API; `["*"]` allows any origin  |
| `allowedMethods`   | string list | `["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]` | HTTP methods allowed in preflight responses                       |
| `allowedHeaders`   | string list | *(empty)*                                              | Request headers allowed in preflight responses                    |
| `allowCredentials` | bool        | `false`                                                | Whether credentialed requests (cookies, auth headers) are allowed |
| `maxAge`           | int         | *(omitted)*                                            | How long preflight results may be cached, in seconds              |

CORS is disabled entirely when `admin.cors` is omitted. When `allowCredentials` is `true`
with a wildcard origin, kawa echoes the request origin (with a `Vary` header) instead of returning `*`, as required by
the CORS spec.

### `governance`

Dynamic, and **managed only through the [Admin API](#config-endpoints)** — governance is never read from the YAML
file. A `governance` section in the file is dropped at load time: it is neither applied nor validated.

Topic governance: named CEL rules that new topics must satisfy. Each rule carries its own exemptions — named cases the
rule does not apply to. When no rules are configured, every topic creation is admitted.

| Field   | Type | Default   | Description            |
|---------|------|-----------|------------------------|
| `rules` | map  | *(empty)* | Named rules, see below |

These tables and the example below describe the governance section as it is **stored in the config topic snapshot**:
`rules` is a map keyed by name. The Admin API presents the same data differently — `GET /governance/rules` returns the
rules as a **list**, each rule carrying its own `name` — and reads and writes one rule at a time through
`GET` and `PUT /governance/rules/{name}` (see [Config endpoints](#config-endpoints)).

#### `governance.rules.<name>`

| Field          | Type                        | Description                                                           |
|----------------|-----------------------------|-----------------------------------------------------------------------|
| `name`         | string                      | Unique, human-readable name of the rule (required)                    |
| `errorMessage` | string                      | Message shown when the rule rejects a topic                           |
| `description`  | string                      | Longer explanation of what the rule enforces                          |
| `selector`     | [selector](#selector)       | Which Kafka resources the rule applies to                             |
| `expression`   | [expression](#expression)   | Must evaluate to `true` for the resource to be compliant (required)   |
| `exemptions`   | list of [exemption](#exemption) | Named cases the rule does not apply to; empty or absent means none |

##### `selector`

| Field          | Type                      | Description                                             |
|----------------|---------------------------|---------------------------------------------------------|
| `resourceType` | string                    | Kafka resource type the rule targets, e.g. `TOPIC`      |
| `expression`   | [expression](#expression) | Narrows the selected resources; `true` selects them all |

##### `expression`

| Field   | Type   | Description                          |
|---------|--------|--------------------------------------|
| `type`  | string | Expression language; currently `CEL` |
| `value` | string | The expression source                |

##### `exemption`

| Field         | Type                      | Description                                                  |
|---------------|---------------------------|--------------------------------------------------------------|
| `name`        | string                    | Unique name of the exemption within its rule (required)      |
| `description` | string                    | Why the exemption exists                                     |
| `expression`  | [expression](#expression) | When it evaluates to `true`, the rule is skipped (required)  |

An exemption only switches off **its own rule**; every other rule still applies. When any of a rule's exemptions
evaluates to `true` for a request, that rule is skipped. An exemption that fails to evaluate, or does not return
`true`, does not apply, so the rule stays enforced. Exemptions use the same bindings as rules, so they can match on the
principal, the service, the topic, or any combination — a topic-only exemption such as
`topic.name.endsWith('-changelog')` is allowed and exempts every principal from that one rule.

```json
{
  "rules": {
    "min-replication": {
      "name": "min-replication",
      "errorMessage": "replication factor must be at least 3",
      "description": "Topics need at least 3 replicas to survive a broker loss.",
      "selector": {"resourceType": "TOPIC", "expression": {"type": "CEL", "value": "true"}},
      "expression": {"type": "CEL", "value": "topic.replicationFactor >= 3"},
      "exemptions": [
        {
          "name": "ops-changelogs",
          "description": "Operators manage changelog topics by hand.",
          "expression": {"type": "CEL", "value": "principal.startsWith('ops-') && topic.name.endsWith('-changelog')"}
        }
      ]
    }
  }
}
```

CEL expressions — rules and exemptions alike — are compiled eagerly when the config is applied: a bad expression
rejects the config instead of failing the first request. They have access to these bindings:

| Binding                   | Type                    | Notes                                                               |
|---------------------------|-------------------------|---------------------------------------------------------------------|
| `principal`               | string                  | The principal requesting the topic                                  |
| `service`                 | string                  | The service the topic is created on                                 |
| `topic.name`              | string                  | Topic name                                                          |
| `topic.partitions`        | int                     | Requested partitions, or `-1` for the broker default                |
| `topic.replicationFactor` | int                     | Requested replication factor, or `-1` for the broker default        |
| `topic.configs`           | map of string to string | Topic configs; a missing key resolves to `""` (falsy, not an error) |

## Dynamic config

Virtual topics, RBAC, client authentication and governance are **dynamic**: they are read from the config topic (default
`__kawa`) and update live while the gateway runs. The static YAML file does **not** carry them - any `virtualTopics`,
`rbac`,
`auth.clients`/`auth.mechanisms` or `governance` in the file are ignored.

Each message in the config topic is a full JSON snapshot of the dynamic subset of
[`GatewayConfig`](#reference). The topic is expected to have a single partition and
`cleanup.policy=compact`; the last snapshot wins.

An **empty config topic on first boot is valid**: the gateway boots with no virtual topics, default-deny RBAC, no client
auth and no governance rules, and picks up the config as soon as the first snapshot is written. This is the normal
first-boot flow - there is no "seed the config topic before starting" requirement.

The startup-only configuration (`listeners`, `clusters`, `advertised`, `admin`,
`auth.brokerAuth`, `configTopic`) always comes from the static YAML file and cannot be changed live.

## Admin API

When `admin.enabled` is `true`, the admin HTTP server exposes gateway state and the dynamic config. All endpoints return
JSON.

### `GET /topics`

Lists the virtual and physical topics known to the gateway (see [Admin](#admin)). Virtual topics are listed even when
their physical backing topic does not exist yet.

### `POST /topics`

The unified topic creation surface. The request body carries a `type` discriminator plus the fields of one topic kind:

```json
{"type": "physical", "name": "orders", "partitions": 3, "replicationFactor": 3, "configs": {"cleanup.policy": "compact"}}
{"type": "virtual", "name": "orders", "topic": "orders-v2", "filter": {"kind": "header", "header": "tenant", "value": "acme"}}
```

A `physical` topic runs the governance admission check and then creates the topic on the broker: an exempt principal +
topic pair is admitted without evaluation, otherwise every rule must pass. A compliant topic returns `201` with the
stored spec; a topic that violates one or more rules returns `403` with the violation messages; a topic that already
exists returns `409`. A `virtual` topic writes the virtual topic config (no governance applies)
and returns `201` with the stored config. An invalid body, unknown `type`, or a virtual topic without a physical `topic`
returns `400`.

The admin HTTP layer has no authentication yet, so the requesting principal is a placeholder (`admin`) until real admin
auth lands.

### `PUT /topics/{name}`

Adds or replaces the virtual topic config for `name`. The body uses the same `type`
discriminator as `POST /topics` — only `"type": "virtual"` is accepted
(`{"type": "virtual", "topic": "orders-v2", "filter": {...}}`). Returns `200` with the stored config; an invalid body, a
`"type": "physical"` body, or a virtual body without a physical `topic` returns `400`. Physical topic alteration is not
supported.

### `DELETE /topics/{name}`

Removes a topic. When `name` is a virtual topic, its config is removed (no broker operation). Otherwise the physical
topic is deleted on the broker. Returns `204`; a name that is neither virtual nor physical returns `404`. A name that is
both resolves to the virtual config removal.

### Config endpoints

The `/rbac/...`, `/auth/...` and `/governance/rules` endpoints read and write the dynamic config (RBAC, client auth
and governance). `GET` lists a section, `PUT /{name}` upserts one entry,
`DELETE /{name}` removes it. Writes persist a full snapshot to the config topic and are applied live.

| Endpoint               | Method | Description                                      |
|------------------------|--------|--------------------------------------------------|
| `/rbac/roles`          | GET    | List roles                                       |
| `/rbac/roles/{name}`   | PUT    | Add or replace a role                            |
| `/rbac/roles/{name}`   | DELETE | Remove a role                                    |
| `/rbac/groups`         | GET    | List groups                                      |
| `/rbac/groups/{name}`  | PUT    | Add or replace a group                           |
| `/rbac/groups/{name}`  | DELETE | Remove a group                                   |
| `/auth/clients`        | GET    | List clients                                     |
| `/auth/clients/{name}` | PUT    | Add or replace a client                          |
| `/auth/clients/{name}` | DELETE | Remove a client                                  |
| `/governance/rules`         | GET    | List the governance rules                |
| `/governance/rules/{name}`  | GET    | Read one governance rule                 |
| `/governance/rules/{name}`  | PUT    | Add or replace a governance rule         |

The request body for a `PUT` is the entry's JSON object, using the same fields as the reference above — e.g.
`{"acls": [...]}` for a role, `{"clients": [...], "roles": [...]}`
for a group, `{"mechanism": "PLAIN", "password": "..."}` for a client, or a rule object
(`errorMessage`, `description`, `selector`, `expression`, `exemptions`; see [`governance`](#governance)) for a
governance rule.

Adding a client via `PUT /auth/clients/{name}` auto-expands the advertised SASL mechanisms to include the client's
mechanism, so the first client can be added to an empty config. The client's `mechanism` is required — a `PUT` without
it is rejected with `400`.

Deleting a group that still lists clients is rejected with `409` and a message naming the group's clients — clear the
group's clients first, then delete again. Deleting a client also removes it from every group that lists it, so a delete
cannot leave a dangling client reference behind.

Deleting a role also removes it from every group that references it: the groups and their clients are kept, but the
role's ACLs stop applying to them. Unlike group/client deletion, this is not rejected — the reference is cleaned up as
part of the delete.

A governance rule's name always comes from the path: the body's `name` is optional, and when present it must equal
the path name, or the `PUT` is rejected with `400`. Both the rule's and the selector's CEL expressions are compiled
before the snapshot is persisted — an invalid rule is rejected with `400` and nothing is written. A rule's exemptions
are part of the rule: they are written with it (their names must be unique within the rule, and each expression is
compiled too) and a `PUT` replaces them along with the rest of the rule. `GET /governance/rules/{name}` returns `404`
for an unknown rule.

`PUT` returns `200` with the stored entry, `DELETE` returns `204`, a missing entry on
`DELETE` returns `404`, and an invalid body returns `400`.
