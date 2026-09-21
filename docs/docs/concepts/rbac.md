---
title: Access control (RBAC)
sidebar_position: 4
---

# Access control (RBAC)

kawa enforces **role-based access control** on the client-to-broker path. Once
[authentication](/docs/concepts/authentication) has established who a client is, kawa checks every request against a set
of ACLs before forwarding it to the cluster. Requests that are not authorized are answered locally with a Kafka
authorization error and never reach the broker.

:::warning RBAC is always enforced — there is no opt-out RBAC cannot be disabled. There is no `rbac.enabled` flag and no
way to turn it off. A gateway with **no roles or groups configured denies every request from every client** —
`RbacAuthorizer` is default-deny, so nothing matches and nothing is allowed. If you want permissive behavior, you must
configure it explicitly (see [Broad access](#broad-access)
below); omitting RBAC configuration is not a way to get it.
:::

RBAC is **default-deny**: a request is only allowed if at least one matching ACL grants it, and any matching deny wins
immediately.

## Model

RBAC is configured in terms of **roles**, **groups** and **ACLs**:

- A **role** is a named list of ACLs.
- A **group** lists clients (authenticated usernames) and the roles those clients inherit.
- An **ACL** grants or denies one operation on one resource.

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

Here `alice` inherits the `producer` role (TOPIC `WRITE` on `orders`), and `bob` inherits the `admin` role (CLUSTER
`CREATE`).

## ACLs

Each ACL has three parts:

| Field        | Type   | Description                                         |
|--------------|--------|-----------------------------------------------------|
| `resource`   | object | The resource the ACL applies to                     |
| `operation`  | string | The operation being granted or denied               |
| `permission` | string | `ALLOW` or `DENY`; defaults to `ALLOW` when omitted |

### Resource

| Field         | Type   | Description                                                                                |
|---------------|--------|--------------------------------------------------------------------------------------------|
| `type`        | string | `TOPIC`, `GROUP`, `TRANSACTIONAL_ID` or `CLUSTER`                                          |
| `pattern`     | string | Resource name; required for `TOPIC`, `GROUP` and `TRANSACTIONAL_ID`, ignored for `CLUSTER` |
| `patternType` | string | `LITERAL` (default) or `PREFIXED`                                                          |

- `CLUSTER` matches the cluster regardless of name — `pattern` is not used.
- `TOPIC`, `GROUP` and `TRANSACTIONAL_ID` match by name. `LITERAL` requires an exact match;
  `PREFIXED` matches any name starting with `pattern`.
- `TRANSACTIONAL_ID` gates FindCoordinator's TRANSACTION lookups (`keyType == 1`), DescribeTransactions, and the
  transactional produce/commit APIs `AddPartitionsToTxn` and
  `TxnOffsetCommit`. The remaining transactional APIs (`InitProducerId`, `AddOffsetsToTxn`,
  `EndTxn`) are not gated yet.

### Operation

The operation being checked. The most relevant values are:

| Operation | Used for                                                                |
|-----------|-------------------------------------------------------------------------|
| `WRITE`   | Producing to a topic                                                    |
| `READ`    | Consumer group management (JoinGroup, SyncGroup, Heartbeat, LeaveGroup) |
| `CREATE`  | Creating topics                                                         |
| `DELETE`  | Deleting topics                                                         |
| `ALL`     | Matches any operation                                                   |

### Permission

- `ALLOW` — grants the operation.
- `DENY` — explicitly forbids it. A matching `DENY` wins over any `ALLOW`, even if another ACL grants the same
  operation.

## What is enforced today

RBAC currently gates these APIs:

| API                                            | Resource                                                                  | Operation         |
|------------------------------------------------|---------------------------------------------------------------------------|-------------------|
| Produce                                        | `TOPIC` (per topic)                                                       | `WRITE`           |
| CreateTopics                                   | `CLUSTER`                                                                 | `CREATE`          |
| DeleteTopics                                   | `CLUSTER`                                                                 | `DELETE`          |
| CreatePartitions                               | `TOPIC` (per topic)                                                       | `ALTER`           |
| DescribeAcls                                   | `CLUSTER`                                                                 | `DESCRIBE`        |
| CreateAcls                                     | `CLUSTER`                                                                 | `ALTER`           |
| DeleteAcls                                     | `CLUSTER`                                                                 | `ALTER`           |
| DescribeLogDirs                                | `CLUSTER`                                                                 | `DESCRIBE`        |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | `GROUP` (the groupId)                                                     | `READ`            |
| DescribeGroups                                 | `GROUP` (per group)                                                       | `DESCRIBE`        |
| ListGroups                                     | `GROUP` (response filter)                                                 | `DESCRIBE`        |
| OffsetCommit                                   | `GROUP` (the groupId) + `TOPIC` (per topic)                               | `READ`            |
| OffsetFetch                                    | `GROUP` (the groupId) + `TOPIC` (per topic)                               | `DESCRIBE`        |
| OffsetDelete                                   | `GROUP` (the groupId) + `TOPIC` (per topic)                               | `DELETE` / `READ` |
| FindCoordinator (GROUP lookup)                 | `GROUP` (the key)                                                         | `DESCRIBE`        |
| FindCoordinator (TRANSACTION lookup)           | `TRANSACTIONAL_ID` (the key)                                              | `DESCRIBE`        |
| DescribeTransactions                           | `TRANSACTIONAL_ID` (per id)                                               | `DESCRIBE`        |
| AddPartitionsToTxn                             | `TRANSACTIONAL_ID` (the id) + `TOPIC` (per topic)                         | `WRITE`           |
| TxnOffsetCommit                                | `TRANSACTIONAL_ID` (the id) + `GROUP` (the groupId) + `TOPIC` (per topic) | `WRITE` / `READ`  |
| DescribeTopicPartitions                        | `TOPIC` (per topic)                                                       | `DESCRIBE`        |
| OffsetForLeaderEpoch                           | `TOPIC` (per topic)                                                       | `DESCRIBE`        |

### Produce

Produce is checked **per topic**. Each topic in a Produce batch is authorized independently on `TOPIC` `WRITE`:

- Authorized topics are forwarded to the broker unchanged.
- Denied topics are stripped from the request — their record batches never reach the cluster — and answered with a
  `TOPIC_AUTHORIZATION_FAILED` partition response merged into the broker's reply.
- If every topic in the batch is denied, the whole request is answered locally with a fully synthesized
  `TOPIC_AUTHORIZATION_FAILED` response and nothing is forwarded.

### Whole-request APIs

CreateTopics, DeleteTopics, DescribeAcls, CreateAcls, DeleteAcls, DescribeLogDirs and the group-management APIs are
checked as a whole. If the principal lacks the required permission, the request is short-circuited with the appropriate
error (`CLUSTER_AUTHORIZATION_FAILED` or
`GROUP_AUTHORIZATION_FAILED`).

## Unauthenticated requests

A request that reaches a gated API **without an authenticated principal** is denied with
`SASL_AUTHENTICATION_FAILED` rather than passed through. This closes the bypass where a client skips SASL entirely —
RBAC cannot be circumvented by not authenticating.

## Broad access

Because RBAC is always enforced, "wide open" is something you configure, not something you get by leaving RBAC out. The
sanctioned way to grant a principal access to **any** topic and **any** group is a `PREFIXED` ACL with an **empty
pattern** — every resource name starts with the empty string, so it matches everything of that type. Combined with
`operation: ALL` and a `CLUSTER` ACL (whose pattern is ignored), this grants the principal everything:

```yaml
rbac:
  roles:
    allow-all:
      acls:
        - resource:
            type: TOPIC
            pattern: ""
            patternType: PREFIXED
          operation: ALL
        - resource:
            type: GROUP
            pattern: ""
            patternType: PREFIXED
          operation: ALL
        - resource:
            type: CLUSTER
          operation: ALL
  groups:
    everyone:
      clients: [alice]
      roles: [allow-all]
```

`alice` can now produce to, consume from, and administer any topic and any group. Use this pattern for quick-start demos
and test fixtures that need broad access; prefer narrow, explicit ACLs in production.

## Example

A producer that may write to `orders` but nothing else, and an admin who can create topics:

```yaml
auth:
  mechanisms:
    - PLAIN
  clients:
    alice:
      password: <encoded-pbkdf2-hash>
    bob:
      password: <encoded-pbkdf2-hash>

rbac:
  roles:
    orders-producer:
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
        - resource:
            type: CLUSTER
          operation: DELETE
  groups:
    producers:
      clients: [alice]
      roles: [orders-producer]
    admins:
      clients: [bob]
      roles: [admin]
```

`alice` can produce to `orders` but cannot create or delete topics; `bob` can create and delete topics but cannot
produce. Any other user (or an unauthenticated connection) is denied by default.
