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

Governance: named CEL rules that resources must satisfy, global exemptions that skip every rule, and typed variables
every expression can read. When no rules are configured, every request is admitted.

| Field        | Type | Default   | Description                                        |
|--------------|------|-----------|----------------------------------------------------|
| `rules`      | map  | *(empty)* | Named rules, see below                             |
| `exemptions` | map  | *(empty)* | Named global [exemptions](#exemption); each skips every rule |
| `variables`  | map  | *(empty)* | Named [variables](#variable)                       |

These tables describe the governance section as it is **stored in the config topic snapshot**: each section is a map
keyed by name. The Admin API lists them as arrays of objects carrying their own `name`, and reads and writes one entry
at a time (see [Config endpoints](#config-endpoints)).

#### What is enforced today

| Request                                                                              | Enforced as                                        | Refused with                                   |
|--------------------------------------------------------------------------------------|----------------------------------------------------|------------------------------------------------|
| `CreateTopics` (Kafka protocol) and `POST /topics` with `type: physical` (Admin API) | `TOPIC` rules on `CREATE` covering physical topics | `POLICY_VIOLATION` per topic                   |
| `AlterConfigs`, `IncrementalAlterConfigs` on topics                                   | `TOPIC` rules on `ALTER` covering physical topics  | `POLICY_VIOLATION` per resource                |
| `CreatePartitions`                                                                    | `TOPIC` rules on `ALTER` covering physical topics  | `POLICY_VIOLATION` per topic                   |
| `POST /topics` with `type: virtual`, `PUT /topics/{name}` (Admin API)                 | `TOPIC` rules covering virtual topics, on `CREATE` for a new virtual topic and `ALTER` for an existing one | `403` with the violations |
| `PATCH /topics/{name}` on a virtual topic (Admin API)                                 | `TOPIC` rules covering virtual topics on `ALTER`, under the new name | `403` with the violations |
| `DeleteTopics`                                                                        | `TOPIC` rules on `DELETE` covering physical topics | `POLICY_VIOLATION` per topic                   |
| `DELETE /topics/{name}` (Admin API)                                                   | `TOPIC` rules on `DELETE` covering the topic's kind | `403` with the violations                     |
| `JoinGroup`, `ConsumerGroupHeartbeat`                                                 | `GROUP` rules                                      | `INVALID_GROUP_ID`                             |
| `OffsetCommit`, `TxnOffsetCommit`                                                     | `GROUP` rules                                      | `INVALID_GROUP_ID` per partition               |
| `InitProducerId` with a transactional id                                              | `TRANSACTIONAL_ID` rules                           | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`        |

The governance message, e.g. `[model > compaction > compact] model topics must be compacted`, is sent in the
response's error message where the API has one (`CreateTopics`, the alter configs APIs, `CreatePartitions`, `DeleteTopics`,
`ConsumerGroupHeartbeat`), so
clients usually only see the error code for groups and transactional ids. Every refusal is logged by the gateway with
the full message. For requests with per-entry results the rest of the request goes through.

A consumer that is not allowed to use its group fails on `poll()` with `InvalidGroupIdException`; a transactional
producer fails on `initTransactions()` with `TransactionalIdAuthorizationException`. A `ConsumerGroupHeartbeat` that
leaves the group is never refused. Group and transactional-id verdicts are cached until the governance config changes.

On `ALTER` a rule sees the topic **as it will be after the change**: the gateway reads its current partitions,
replication factor and configs from the broker and applies the change on top. `AlterConfigs` replaces every config set
on the topic; `IncrementalAlterConfigs` sets, deletes, appends to or subtracts from them (appending to a config the topic
does not set starts from an empty list, not the broker default); `CreatePartitions` sets the partition count. So a
change to a topic that already breaks a rule is refused unless it fixes it. A change to a topic the broker does not
know is left for the broker to answer, and when the topic's state cannot be read the change is refused. The broker is
only asked while some rule runs on `ALTER`.

On `DELETE` a rule sees the topic **as it is**, read from the broker the same way (a virtual topic: its name and
physical topic). A `DeleteTopics` entry that names a topic by id only cannot be judged and is refused while some rule
runs on `DELETE`; delete by name instead.

A virtual topic is judged with `topic.name`, `topic.virtual == true` and `topic.physicalTopic`. A change through a
virtual topic's name on the Kafka protocol (an alter or `CreatePartitions`) changes its physical topic, so it is judged
as a change to that physical topic. Snapshots written straight to the config topic bypass the Admin API and are not
judged.

#### `governance.rules.<name>`

| Field          | Type                                | Description                                                         |
|----------------|-------------------------------------|---------------------------------------------------------------------|
| `name`         | string                              | Unique, human-readable name of the rule (required)                  |
| `errorMessage` | string                              | Message shown when the rule rejects a request                       |
| `description`  | string                              | Longer explanation of what the rule enforces                        |
| `selector`     | [selector](#selector)               | Which requests the rule applies to                                  |
| `match`        | `ALL` \| `ANY`                      | How `subRules` combine: every one must hold, or at least one        |
| `subRules`     | list of [sub-rule](#sub-rule)       | The checks and groups making up the rule (required, non-empty)      |
| `exemptions`   | list of [exemption](#exemption)     | Named cases this rule does not apply to; empty or absent means none |

A snapshot written before sub-rules existed, with a single top-level `expression`, is still read: it becomes one check
named after the rule.

##### `selector`

| Field          | Type                      | Default    | Description                                                        |
|----------------|---------------------------|------------|--------------------------------------------------------------------|
| `resourceType` | string                    |            | `TOPIC`, `GROUP` or `TRANSACTIONAL_ID`; decides the bound variable |
| `expression`   | [expression](#expression) | `null`     | Narrows the selected resources; `null` selects them all            |
| `scope`        | string                    | `BOTH`     | Topics only: `PHYSICAL`, `VIRTUAL` or `BOTH`                       |
| `operations`   | list of string            | `[CREATE]` | Topics only: `CREATE`, `ALTER` (config changes, `CreatePartitions`) and/or `DELETE`; ignored for other types |

##### `sub-rule`

A check (`kind: check`) or a group of checks (`kind: group`). Groups hold checks only, never other groups.

| Field          | Type                      | Description                                                            |
|----------------|---------------------------|------------------------------------------------------------------------|
| `kind`         | string                    | `check` or `group`                                                     |
| `name`         | string                    | Unique among its siblings                                              |
| `errorMessage` | string                    | Optional; when absent the nearest group's or the rule's message is used |
| `expression`   | [expression](#expression) | A check's expression                                                   |
| `match`        | `ALL` \| `ANY`            | How a group's checks combine                                           |
| `checks`       | list of sub-rule          | A group's checks                                                       |

Sub-rules are evaluated left to right and stop early like `&&` / `||`. When a rule fails, the message is the most
specific one along the failing path: under `ALL` the failing check's own message, else its group's, else the rule's;
under `ANY` (no branch held) the group's or rule's.

##### `expression`

| Field   | Type   | Description                          |
|---------|--------|--------------------------------------|
| `type`  | string | Expression language; currently `CEL` |
| `value` | string | The expression source                |

##### `exemption`

| Field         | Type                      | Description                                                  |
|---------------|---------------------------|--------------------------------------------------------------|
| `name`        | string                    | Unique name of the exemption (required)                      |
| `description` | string                    | Why the exemption exists                                     |
| `expression`  | [expression](#expression) | When it evaluates to `true`, the rule is skipped (required)  |

A rule's own exemption switches off **that rule** only. A global exemption (`governance.exemptions`) switches off
**every** rule; it may read any resource variable, but only the requested resource is bound, so `topic.name…` never
matches a consumer group request. An exemption that fails to evaluate, or does not return `true`, does not apply.

##### `variable`

| Field   | Type   | Description                                                                         |
|---------|--------|-------------------------------------------------------------------------------------|
| `name`  | string | A CEL identifier; not `principal`, `service`, `topic`, `group` or `transaction`     |
| `type`  | string | `string`, `int`, `double`, `bool`, `list<string>` or `list<int>`                    |
| `value` | string | The literal in JSON syntax, e.g. `"[1, 4, 6, 12]"`                                  |
| `note`  | string | What the variable is for                                                            |

`samples`, `notes` and `extraExamples` are admin UI display hints; the gateway stores them and never reads them. A
`string` variable holding a regex may use named groups (also display hints); they are evaluated as plain groups.

```json
{
  "rules": {
    "partition-tier": {
      "name": "partition-tier",
      "errorMessage": "Partition count must match a tier.",
      "selector": {"resourceType": "TOPIC", "expression": null, "scope": "PHYSICAL", "operations": ["CREATE"]},
      "match": "ALL",
      "subRules": [
        {"kind": "check", "name": "tier", "expression": {"type": "CEL", "value": "topic.partitions in partitionTiers"}}
      ],
      "exemptions": [
        {"name": "platform", "description": "Sized by hand.", "expression": {"type": "CEL", "value": "principal.startsWith('User:platform-')"}}
      ]
    }
  },
  "exemptions": {},
  "variables": {
    "partitionTiers": {"name": "partitionTiers", "type": "list<int>", "value": "[1, 4, 6, 12]", "note": "single, low, medium, high."}
  }
}
```

CEL expressions — rules, selectors and exemptions alike — are compiled eagerly when the config is applied: a bad
expression rejects the config instead of failing the first request. Removing or retyping a variable a rule still reads
is refused by the Admin API. Expressions have access to these bindings:

| Binding                   | Type                    | Notes                                                               |
|---------------------------|-------------------------|---------------------------------------------------------------------|
| `principal`               | string                  | The requesting principal                                            |
| `service`                 | string                  | `kafka` on the Kafka listener, `kawa` on the Admin API              |
| `topic.name`              | string                  | `TOPIC` rules: topic name                                           |
| `topic.virtual`           | bool                    | `TOPIC` rules: whether the topic is virtual                         |
| `topic.partitions`        | int                     | Physical topics: partitions; on `CREATE` `-1` for the broker default |
| `topic.replicationFactor` | int                     | Physical topics: replication factor; on `CREATE` `-1` for the broker default |
| `topic.configs`           | map of string to string | Physical topics: configs set on the topic (not broker defaults). Test presence with `'key' in topic.configs`; reading a key that is not set fails the check |
| `topic.physicalTopic`     | string                  | Virtual topics: the physical topic it maps onto                     |
| `group.id`                | string                  | `GROUP` rules: consumer group id                                    |
| `transaction.id`          | string                  | `TRANSACTIONAL_ID` rules: the transactional id                      |
| *variable name*           | its declared type       | Every governance variable                                           |

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
exists returns `409`. A `virtual` topic is judged by the rules covering virtual topics (`403` with the violations when
refused), then writes the virtual topic config and returns `201` with the stored config. An invalid body, unknown `type`, or a virtual topic without a physical `topic`
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
| `/governance/rules`         | GET    | The governance section: rules, global exemptions and variables |
| `/governance/rules/{name}`  | GET    | Read one governance rule                 |
| `/governance/rules/{name}`  | PUT    | Add or replace a governance rule         |
| `/governance/rules/{name}`  | DELETE | Remove a governance rule                 |
| `/governance/exemptions`         | GET    | List the global exemptions          |
| `/governance/exemptions/{name}`  | GET, PUT, DELETE | Read, add or replace, remove a global exemption |
| `/governance/variables`          | GET    | List the variables                  |
| `/governance/variables/{name}`   | PUT, DELETE | Add or replace, remove a variable (refused while a rule reads it) |
| `/governance/dry-run`            | POST   | Evaluate a request (or one unsaved rule) and return the full trace |

The request body for a `PUT` is the entry's JSON object, using the same fields as the reference above — e.g.
`{"acls": [...]}` for a role, `{"clients": [...], "roles": [...]}`
for a group, `{"mechanism": "PLAIN", "password": "..."}` for a client, or a rule object
(`errorMessage`, `description`, `selector`, `match`, `subRules`, `exemptions`; see [`governance`](#governance)) for a
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
