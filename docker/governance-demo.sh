#!/usr/bin/env bash
# Loads a demo governance setup for an imaginary toy factory into a running gateway.
#
#   ./docker/governance-demo.sh                 # admin API on http://localhost:8080
#   ADMIN=http://other-host:8080 ./docker/governance-demo.sh
#
# Topics:   workshop.<line>.<station>, orders.<channel>.placed|shipped, qa.<line>.passed|rejected
# Groups:   elf.<team>.<purpose>      Transactional ids: dispatch.<service>.tx
set -euo pipefail
ADMIN="${ADMIN:-http://localhost:8080}"

put() {
    local path="$1"
    printf 'PUT %-48s ' "$path"
    curl -sS -o /tmp/kawa-demo.out -w '%{http_code}\n' -X PUT -H 'content-type: application/json' \
        --data-binary @- "$ADMIN$path?consistency=applied"
    if ! grep -q '"name"' /tmp/kawa-demo.out; then cat /tmp/kawa-demo.out; echo; fi
}

# ── Variables (first: rules read them) ──
put /governance/variables/toyLines <<'JSON'
{"type": "list<string>", "value": "[\"plush\", \"blocks\", \"puzzles\", \"robots\"]", "note": "Production lines on the factory floor."}
JSON
put /governance/variables/partitionTiers <<'JSON'
{"type": "list<int>", "value": "[1, 3, 6, 12]", "note": "single, small, busy, peak season."}
JSON
put /governance/variables/minReplicas <<'JSON'
{"type": "int", "value": "3", "note": "No toy gets lost when one broker naps."}
JSON
put /governance/variables/qaRetentionMs <<'JSON'
{"type": "int", "value": "2592000000", "note": "30 days: complaints arrive late."}
JSON

# ── Rules ──
put /governance/rules/toy-topic-naming <<'JSON'
{
  "description": "Every topic follows one of the factory's naming forms.",
  "errorMessage": "Topic names must be workshop.<line>.<station>, orders.<channel>.placed|shipped or qa.<line>.passed|rejected.",
  "selector": {"resourceType": "TOPIC", "scope": "BOTH", "operations": ["CREATE"]},
  "match": "ANY",
  "subRules": [
    {"kind": "check", "name": "workshop",
     "expression": {"type": "CEL", "value": "topic.name.matches(\"^workshop\\\\.(?<line>[a-z]+)\\\\.(?<station>[a-z0-9-]+)$\")"}},
    {"kind": "group", "name": "orders", "match": "ANY",
     "errorMessage": "Order topics are orders.<channel>.placed or orders.<channel>.shipped.",
     "checks": [
       {"kind": "check", "name": "placed",
        "expression": {"type": "CEL", "value": "topic.name.matches(\"^orders\\\\.(?<channel>[a-z]+)\\\\.placed$\")"}},
       {"kind": "check", "name": "shipped",
        "expression": {"type": "CEL", "value": "topic.name.matches(\"^orders\\\\.(?<channel>[a-z]+)\\\\.shipped$\")"}}
     ]},
    {"kind": "check", "name": "qa",
     "expression": {"type": "CEL", "value": "topic.name.matches(\"^qa\\\\.(?<line>[a-z]+)\\\\.(passed|rejected)$\")"}}
  ],
  "exemptions": [
    {"name": "connect-internals", "description": "Kafka Connect keeps its own bookkeeping topics.",
     "expression": {"type": "CEL", "value": "principal == 'User:kafka-connect' && topic.name.startsWith('connect-')"}}
  ]
}
JSON

put /governance/rules/known-toy-line <<'JSON'
{
  "description": "Workshop and QA topics name a production line that exists.",
  "errorMessage": "Unknown toy line; it must be one of toyLines.",
  "selector": {"resourceType": "TOPIC", "scope": "BOTH", "operations": ["CREATE"],
               "expression": {"type": "CEL", "value": "topic.name.startsWith('workshop.') || topic.name.startsWith('qa.')"}},
  "match": "ALL",
  "subRules": [
    {"kind": "check", "name": "line",
     "expression": {"type": "CEL", "value": "toyLines.exists(l, topic.name.startsWith('workshop.' + l + '.') || topic.name.startsWith('qa.' + l + '.'))"}}
  ]
}
JSON

put /governance/rules/assembly-sizing <<'JSON'
{
  "description": "Physical topics come in fixed sizes and survive a broker nap.",
  "errorMessage": "Topic sizing does not meet factory standards.",
  "selector": {"resourceType": "TOPIC", "scope": "PHYSICAL", "operations": ["CREATE"]},
  "match": "ALL",
  "subRules": [
    {"kind": "group", "name": "sizing", "match": "ALL", "checks": [
      {"kind": "check", "name": "tier", "errorMessage": "Partitions must be one of 1, 3, 6 or 12.",
       "expression": {"type": "CEL", "value": "topic.partitions in partitionTiers"}},
      {"kind": "check", "name": "replicas", "errorMessage": "Replication factor must be at least 3.",
       "expression": {"type": "CEL", "value": "topic.replicationFactor >= minReplicas"}}
    ]}
  ],
  "exemptions": [
    {"name": "prototype-lab", "description": "The prototype lab tinkers with tiny single-replica topics.",
     "expression": {"type": "CEL", "value": "principal.startsWith('User:proto-lab')"}}
  ]
}
JSON

put /governance/rules/qa-results-kept <<'JSON'
{
  "description": "QA results stay around long enough to answer complaints; checked on create and on config changes.",
  "errorMessage": "QA topics keep results for at least 30 days (retention.ms >= 2592000000, or -1 for forever).",
  "selector": {"resourceType": "TOPIC", "scope": "PHYSICAL", "operations": ["CREATE", "ALTER"],
               "expression": {"type": "CEL", "value": "topic.name.startsWith('qa.')"}},
  "match": "ALL",
  "subRules": [
    {"kind": "check", "name": "retention",
     "expression": {"type": "CEL", "value": "!('retention.ms' in topic.configs) || int(topic.configs['retention.ms']) == -1 || int(topic.configs['retention.ms']) >= qaRetentionMs"}}
  ]
}
JSON

put /governance/rules/elf-consumer-groups <<'JSON'
{
  "description": "Consumer groups belong to an elf team, or are Kafka Connect workers.",
  "errorMessage": "Consumer groups are elf.<team>.<purpose> or connect-<connector>.",
  "selector": {"resourceType": "GROUP"},
  "match": "ANY",
  "subRules": [
    {"kind": "check", "name": "elf-team",
     "expression": {"type": "CEL", "value": "group.id.matches(\"^elf\\\\.(?<team>[a-z]+)\\\\.(?<purpose>[a-z-]+)$\")"}},
    {"kind": "check", "name": "connect", "expression": {"type": "CEL", "value": "group.id.startsWith('connect-')"}}
  ]
}
JSON

put /governance/rules/dispatch-transactions <<'JSON'
{
  "description": "Transactional ids carry the dispatching service, so fencing stays per service.",
  "errorMessage": "Transactional ids are dispatch.<service>.tx.",
  "selector": {"resourceType": "TRANSACTIONAL_ID"},
  "match": "ALL",
  "subRules": [
    {"kind": "check", "name": "dispatch",
     "expression": {"type": "CEL", "value": "transaction.id.matches(\"^dispatch\\\\.(?<service>[a-z-]+)\\\\.tx$\")"}}
  ]
}
JSON

# ── Global exemptions ──
put /governance/exemptions/factory-maintenance <<'JSON'
{"description": "The maintenance crew may create anything while repairing the line.",
 "expression": {"type": "CEL", "value": "principal == 'User:factory-maintenance'"}}
JSON

echo "Done. Try: curl -s -X POST $ADMIN/governance/dry-run -H 'content-type: application/json' \\"
echo "  -d '{\"resourceType\":\"TOPIC\",\"resource\":{\"name\":\"workshop.dolls.paint\",\"partitions\":3,\"replicationFactor\":3}}'"
