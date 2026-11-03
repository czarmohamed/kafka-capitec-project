#!/usr/bin/env bash
# Creates the digital payments topics on the local Kafka cluster.
# Runs kafka-topics inside broker kafka-0 via kubectl, so no local Kafka install is needed.
# Safe to re-run: --if-not-exists skips topics that already exist.
set -euo pipefail

BROKER_POD="${BROKER_POD:-kafka-0}"
BOOTSTRAP="${BOOTSTRAP:-kafka-service:9092}"

SEVEN_YEARS_MS=220752000000   # 7 x 365 days
THIRTY_DAYS_MS=2592000000     # 30 days
ONE_HOUR_MS=3600000           # warm-up messages are only useful briefly

kt() {
  kubectl exec "$BROKER_POD" -- kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"
}

create_topic() {
  local topic=$1 partitions=$2 retention_ms=$3
  echo "==> Creating $topic"
  kt --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config cleanup.policy=delete \
    --config retention.ms="$retention_ms" \
    --config retention.bytes=-1 \
    --config compression.type=zstd \
    --config max.message.bytes=1048576
}

create_topic digitalpayments.payment.lifecycle.v1 12 "$SEVEN_YEARS_MS"
create_topic digitalpayments.fraud.scores.v1 12 "$SEVEN_YEARS_MS"
create_topic digitalpayments.notification.events.v1 12 "$THIRTY_DAYS_MS"
# Health-check topic for producer warm-up. Not a payment topic, and nothing consumes it.
create_topic digitalpayments.health.warmup.v1 3 "$ONE_HOUR_MS"

echo
echo "==> Describe"
for topic in digitalpayments.payment.lifecycle.v1 digitalpayments.fraud.scores.v1 digitalpayments.notification.events.v1 digitalpayments.health.warmup.v1; do
  kt --describe --topic "$topic"
done
