#!/bin/sh
# Seeds the plain demo broker (docker-compose.yml): creates orders-v2, the physical topic behind the
# virtual topic 'orders', and a demo consumer group with lag. Runs in the apache/kafka image.
set -eu

BOOTSTRAP=kafka:9092
BIN=/opt/kafka/bin
GROUP=demo-orders-reader

until "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  echo "waiting for kafka..."
  sleep 1
done

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic orders-v2 --partitions 1 --replication-factor 1
echo "orders-v2 ready"

# Demo consumer group with lag: 100 records on orders-v2, of which the group consumed and committed
# the first 10, so it shows a lag of 90. Skipped when the group already exists.
if "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$GROUP"; then
  echo "$GROUP already seeded"
  exit 0
fi
seq 1 100 | sed 's/.*/{"orderId":&}/' | "$BIN/kafka-console-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic orders-v2
# max.poll.records=1 makes the committed position exactly the 10 records consumed
"$BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic orders-v2 \
  --group "$GROUP" --from-beginning --max-messages 10 \
  --consumer-property max.poll.records=1 > /dev/null
echo "$GROUP seeded: 100 records, 10 consumed, lag 90"
