#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

fail() { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "$1 not found; install it explicitly and retry"; }

validate_positive_integer() {
  local name=$1 value=$2 maximum=$3
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$name must be an integer from 1 to $maximum (received '$value')"
  (( 10#$value >= 1 && 10#$value <= maximum )) || fail "$name must be between 1 and $maximum (received '$value')"
}

validate_nonnegative_integer() {
  local name=$1 value=$2
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$name must be a non-negative integer (received '$value')"
}

combination_complete() {
  local profile=$1 scenario=$2 repetition=$3 metadata
  while IFS= read -r -d '' metadata; do
    jq -e --arg profile "$profile" --arg scenario "$scenario" --argjson repetition "$repetition" \
      '(.run_id | startswith("official-")) and .status == "passed" and .dry_run == false and .profile == $profile and .scenario == $scenario and .repetition == $repetition' \
      "$metadata" >/dev/null 2>&1 && return 0
  done < <(find data/raw -mindepth 2 -maxdepth 2 -type f -name metadata.json -path 'data/raw/official-*/*' -print0 2>/dev/null | sort -z)
  return 1
}

wait_environment_ready() {
  printf '[matrix] waiting for Kafka resource and all brokers to be Ready\n'
  kubectl wait kafka/experiment -n kafka --for=condition=Ready --timeout=15m
  local broker_count
  broker_count="$(kubectl get pods -n kafka -l 'strimzi.io/cluster=experiment,strimzi.io/pool-name=brokers' -o json | jq '.items | length')"
  (( broker_count > 0 )) || fail "no Strimzi broker pods found"
  kubectl wait pod -n kafka -l 'strimzi.io/cluster=experiment,strimzi.io/pool-name=brokers' --for=condition=Ready --timeout=15m
  if kubectl get jobs -n kafka -o json | jq -e '.items[] | select((.metadata.labels["experiment/run-id"] // "") | startswith("official-")) | select(([.status.conditions[]? | select((.type == "Complete" or .type == "Failed") and .status == "True")] | length) == 0)' >/dev/null; then
    fail "an official-* Job is still pending or running; inspect it before resuming the matrix"
  fi
}

cooldown() {
  local seconds=$1
  if (( seconds > 0 )); then
    printf '[matrix] cooldown: %ss\n' "$seconds"
    sleep "$seconds"
  fi
}

run_combination() {
  local profile=$1 scenario=$2 repetition=$3 target
  if combination_complete "$profile" "$scenario" "$repetition"; then
    printf '[matrix] SKIP profile=%s scenario=%s repetition=%d (valid official run already exists)\n' "$profile" "$scenario" "$repetition"
    return
  fi
  target="${scenario}-$(printf '%s' "$profile" | tr '[:upper:]' '[:lower:]')"
  printf '[matrix] START profile=%s scenario=%s repetition=%d\n' "$profile" "$scenario" "$repetition"
  wait_environment_ready
  cooldown "$COOLDOWN_SECONDS"
  if ! CONFIRM_DELETE=yes make --no-print-directory "$target" REPETITION="$repetition"; then
    printf '[matrix] FAIL profile=%s scenario=%s repetition=%d; resources were left untouched for diagnosis\n' "$profile" "$scenario" "$repetition" >&2
    return 1
  fi
  wait_environment_ready
  cooldown "$COOLDOWN_SECONDS"
  printf '[matrix] SUCCESS profile=%s scenario=%s repetition=%d\n' "$profile" "$scenario" "$repetition"
}

[[ "${CONFIRM_DELETE:-}" == yes ]] || fail "matrix requires CONFIRM_DELETE=yes; no experiment was started"
need kubectl; need jq; need make
REPETITIONS=${REPETITIONS:-5}
COOLDOWN_SECONDS=${COOLDOWN_SECONDS:-30}
validate_positive_integer REPETITIONS "$REPETITIONS" 99
validate_nonnegative_integer COOLDOWN_SECONDS "$COOLDOWN_SECONDS"
mkdir -p data/raw

for ((repetition = 1; repetition <= 10#$REPETITIONS; repetition++)); do
  if (( repetition % 2 == 1 )); then profiles=(A B); else profiles=(B A); fi
  for profile in "${profiles[@]}"; do
    run_combination "$profile" baseline "$repetition"
    run_combination "$profile" fault "$repetition"
  done
done

printf '[matrix] validating completed matrix\n'
make --no-print-directory matrix-check REPETITIONS="$REPETITIONS"
printf '[matrix] aggregating official results\n'
make --no-print-directory aggregate
printf '[matrix] COMPLETE: %d official executions validated and aggregated\n' "$((10#$REPETITIONS * 4))"
