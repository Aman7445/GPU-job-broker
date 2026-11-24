#!/usr/bin/env bash
set -euo pipefail

engine=${CONTAINER_ENGINE:-docker}
image=${1:?Provide the image to test}
name="gpu-broker-smoke-$$"
scratch=$(mktemp -d)

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    "$engine" logs "$name" >&2 || true
  fi
  "$engine" rm -f "$name" >/dev/null 2>&1 || true
  "$engine" volume rm "$name" >/dev/null 2>&1 || true
  rm -f "$scratch/snapshot.sqlite"
  rmdir "$scratch"
}
trap cleanup EXIT

start_broker() {
  "$engine" run --detach --name "$name" --publish 127.0.0.1::8080 \
    --volume "$name:/data" --env BROKER_CLOCK_SPEED=0 --read-only --tmpfs /tmp "$image" >/dev/null
  base="http://$("$engine" port "$name" 8080/tcp)"
  for _ in $(seq 1 60); do
    if curl --noproxy '*' --fail --silent --max-time 2 "$base/readyz" >/dev/null; then
      return
    fi
    sleep 0.25
  done
  echo "Broker did not become ready" >&2
  return 1
}

http() {
  curl --noproxy '*' --fail --silent --show-error --max-time 5 "$@"
}

stop_broker() {
  "$engine" stop --time 15 "$name" >/dev/null
  "$engine" rm "$name" >/dev/null
}

"$engine" volume create "$name" >/dev/null
start_broker
http "$base/livez" >/dev/null
created=$(http "$base/jobs" -H 'Content-Type: application/json' -H 'Idempotency-Key: deployment-smoke' \
  -d '{"team":"ops","gpus":2,"estimated_minutes":120}')
job_id=$(printf '%s' "$created" | "$engine" exec -i "$name" python -c 'import json, sys; print(json.load(sys.stdin)["id"])')
http "$base/sim/advance" -H 'Content-Type: application/json' -d '{"seconds":3600}' >/dev/null
before=$(http "$base/jobs/$job_id")
printf '%s' "$before" | "$engine" exec -i "$name" python -c 'import json, sys; assert json.load(sys.stdin)["status"] == "running"'
http "$base/metrics" | awk '/^job_broker_jobs\{state="running"\} 1$/ { found=1 } END { exit !found }'

stop_broker
start_broker
test "$(http "$base/jobs/$job_id")" = "$before"

"$engine" exec "$name" python -m sqlite_ops backup > "$scratch/snapshot.sqlite"
http -X POST "$base/jobs/$job_id/cancel" >/dev/null
cancelled=$(http "$base/jobs/$job_id")
test "$cancelled" != "$before"
stop_broker
"$engine" run --rm -i --volume "$name:/data" "$image" python -m sqlite_ops restore --replace \
  < "$scratch/snapshot.sqlite"
start_broker
test "$(http "$base/jobs/$job_id")" = "$before"
echo "Passed image readiness, metrics, persistent restart, and restore"
