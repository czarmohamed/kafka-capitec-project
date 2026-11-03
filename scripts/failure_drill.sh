#!/usr/bin/env bash
# Broker failure drill: delete broker pod kafka-1 while the Java producer is writing.
#
# Expected: no lost events and no failed sends. With replication factor 3 and
# min.insync.replicas=2, writes stay available while one broker is down.
#
# Run from the project root:  scripts/failure_drill.sh
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

TOPIC="${POC_LIFECYCLE_TOPIC:-digitalpayments.payment.lifecycle.v1}"
RUN_ROOT="${POC_RUN_ROOT:-output}"
JOURNEYS=800
SEED=99
RUN_DIR="$RUN_ROOT/drill-$(date +%s)"
JAVA_BIN="$(command -v java)"
CLASSPATH="$PWD/java/target/classes:$(cat "$PWD/java/target/cp.txt")"
mkdir -p "$RUN_DIR"
LOG="$RUN_DIR/drill.log"

log() { echo "$*" | tee -a "$LOG"; }

broker_total() {
  kubectl exec kafka-0 -- kafka-get-offsets --bootstrap-server kafka-service:9092 \
    --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '/^digitalpayments/ {s+=$3} END {print s+0}'
}

under_replicated() {
  kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 \
    --describe --under-replicated-partitions 2>/dev/null | grep -c "Topic:" || true
}

before=$(broker_total)
log "lifecycle end offsets before drill: $before"

"$JAVA_BIN" -Djava.net.preferIPv4Stack=true -cp "$CLASSPATH" poc.ProducerMain \
  --journeys "$JOURNEYS" --seed "$SEED" --output-dir "$RUN_DIR" \
  > "$RUN_DIR/producer_stdout.json" 2> "$RUN_DIR/producer_stderr.log" &
producer_pid=$!

sleep 5
log "$(date +%T) deleting pod kafka-1 while the producer is writing"
kubectl delete pod kafka-1 --wait=false >> "$LOG" 2>&1
sleep 10
log "$(date +%T) under-replicated partitions during outage: $(under_replicated)"

wait "$producer_pid"
producer_status=$?
log "producer exit status: $producer_status"

kubectl wait --for=condition=Ready pod/kafka-1 --timeout=240s >> "$LOG" 2>&1
log "$(date +%T) kafka-1 is ready again; under-replicated partitions now: $(under_replicated)"

after=$(broker_total)
log "lifecycle end offsets after drill: $after"
log "offsets added during drill: $((after - before))"
log "producer summary:"
grep -E '"(delivered|failed|retried_attempts|unsent_after_flush)"' "$RUN_DIR/summary.json" | tee -a "$LOG"
