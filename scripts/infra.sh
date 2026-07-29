#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

fail() { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "$1 not found; install it explicitly and retry"; }

set -a
# shellcheck disable=SC1091
source versions.env
set +a

cluster_exists() { kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; }
require_cluster() { cluster_exists || fail "Kind cluster '$CLUSTER_NAME' does not exist; run make cluster-up first"; }

doctor() {
  local failed=0
  for tool in go docker kubectl kind helm; do
    if ! command -v "$tool" >/dev/null 2>&1; then printf '%-10s missing\n' "$tool"; failed=1; continue; fi
    printf '%-10s ' "$tool"
    case "$tool" in
      go) go version ;;
      docker) docker --version ;;
      kubectl) kubectl version --client -o json | jq -r '.clientVersion.gitVersion | ltrimstr("v")' ;;
      kind) kind version ;;
      helm) helm version --short ;;
    esac
  done
  if command -v kind >/dev/null 2>&1; then
    actual="$(kind version 2>/dev/null | sed -n 's/^kind v\([^ ]*\).*/\1/p')"
    [[ "$actual" == "$KIND_VERSION" ]] || { printf 'Kind required: %s (found %s)\n' "$KIND_VERSION" "${actual:-unknown}"; failed=1; }
  fi
  docker info >/dev/null 2>&1 || { printf 'docker daemon unavailable\n'; failed=1; }
  (( failed == 0 )) || return 1
}

validate() {
  need go
  go run ./cmd/experiment-runner validate --config configs/profile-a.yaml
  go run ./cmd/experiment-runner validate --config configs/profile-b.yaml
  go run ./cmd/experiment-runner validate --config configs/smoke-baseline.yaml
  grep -Fq "version: $KAFKA_VERSION" deployments/kafka/kafka.yaml || fail "Kafka manifest version differs from versions.env"
  grep -Fq "image: $KIND_NODE_IMAGE" deployments/kind/cluster.yaml || fail "Kind node image differs from versions.env"
  go run ./cmd/manifest-validator configs deployments
  printf 'local validation passed\n'
}

cluster_up() {
  need kind; need kubectl; need docker
  cluster_exists && fail "cluster '$CLUSTER_NAME' already exists; refusing to recreate it"
  kind create cluster --name "$CLUSTER_NAME" --config deployments/kind/cluster.yaml --image "$KIND_NODE_IMAGE"
  kubectl apply -f deployments/namespaces.yaml
}

cluster_status() {
  need kind
  require_cluster
  kubectl cluster-info --context "kind-$CLUSTER_NAME"
  kubectl get nodes -o wide
}

monitoring_up() {
  need helm; require_cluster
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
  helm repo update
  if helm status kube-prometheus-stack -n monitoring >/dev/null 2>&1; then
    installed="$(helm list -n monitoring -f '^kube-prometheus-stack$' -o json | grep -o 'kube-prometheus-stack-[0-9][^"]*' | head -1)"
    [[ "$installed" == "kube-prometheus-stack-$KUBE_PROMETHEUS_STACK_VERSION" ]] ||
      fail "kube-prometheus-stack already exists as '$installed'; refusing a silent version change"
  fi
  helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace monitoring --version "$KUBE_PROMETHEUS_STACK_VERSION" \
    --values deployments/monitoring/values.yaml --wait --timeout 10m
}

kafka_up() {
  need helm; require_cluster
  helm repo add strimzi https://strimzi.io/charts/
  helm repo update
  if helm status strimzi-kafka-operator -n kafka >/dev/null 2>&1; then
    installed="$(helm list -n kafka -f '^strimzi-kafka-operator$' -o json | grep -o 'strimzi-kafka-operator-[0-9][^"]*' | head -1)"
    [[ "$installed" == "strimzi-kafka-operator-$STRIMZI_VERSION" ]] ||
      fail "Strimzi already exists as '$installed'; refusing a silent version change"
  fi
  helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace kafka --version "$STRIMZI_VERSION" --values deployments/strimzi/values.yaml --wait --timeout 5m
  kubectl apply -f deployments/kafka/metrics-config.yaml
  kubectl apply -f deployments/kafka/node-pools.yaml
  kubectl apply -f deployments/kafka/kafka.yaml
  kubectl wait kafka/experiment -n kafka --for=condition=Ready --timeout=15m
  kubectl apply -f deployments/kafka/topics.yaml
  kubectl apply -f deployments/kafka/monitors.yaml
}

images() {
  need docker
  docker build -f Dockerfile.producer -t kafka-sd-starter-producer:dev .
  docker build -f Dockerfile.consumer -t kafka-sd-starter-consumer:dev .
  docker build -f Dockerfile.runner -t kafka-sd-starter-runner:dev .
}

load_images() {
  need kind; require_cluster
  kind load docker-image --name "$CLUSTER_NAME" kafka-sd-starter-producer:dev kafka-sd-starter-consumer:dev kafka-sd-starter-runner:dev
}

apps_up() {
  require_cluster
  kubectl apply -f deployments/applications/workloads.yaml
  kubectl rollout status -n experiment deployment/producer --timeout=3m
  kubectl rollout status -n experiment deployment/consumer --timeout=3m
}

smoke_baseline() {
  need kubectl
  require_cluster
  local run_id job_name config_map_name pvc_name results_pod_name
  local rendered_job rendered_results run_dir
  run_id="smoke-$(date -u +%Y%m%d%H%M%S)-$(printf '%04x' "$RANDOM")"
  job_name="$run_id"
  config_map_name="$run_id-config"
  pvc_name="$run_id-results"
  results_pod_name="$run_id-copy"
  run_dir="data/raw/$run_id"
  rendered_job="$(mktemp)"
  rendered_results="$(mktemp)"

  render_smoke_manifest deployments/applications/smoke-baseline-job.yaml "$rendered_job" \
    "$run_id" "$job_name" "$config_map_name" "$pvc_name" "$results_pod_name"
  render_smoke_manifest deployments/applications/smoke-results-pod.yaml "$rendered_results" \
    "$run_id" "$job_name" "$config_map_name" "$pvc_name" "$results_pod_name"

  kubectl create configmap "$config_map_name" -n kafka \
    --from-file=smoke-baseline.yaml=configs/smoke-baseline.yaml \
    --from-file=versions.env=versions.env
  if ! kubectl create -f "$rendered_job"; then
    smoke_diagnostics "$job_name"
    printf 'Smoke resources preserved because Job creation failed\n' >&2
    return 1
  fi

  if ! wait_smoke_job "$job_name" "${SMOKE_JOB_TIMEOUT:-8m}"; then
    smoke_diagnostics "$job_name"
    copy_smoke_results "$run_id" "$job_name" "$results_pod_name" "$rendered_results" "$run_dir" || true
    printf 'Smoke resources preserved for diagnosis: job/%s, configmap/%s, pvc/%s\n' \
      "$job_name" "$config_map_name" "$pvc_name" >&2
    return 1
  fi
  kubectl logs -n kafka "job/$job_name" --all-containers=true

  if ! copy_smoke_results "$run_id" "$job_name" "$results_pod_name" "$rendered_results" "$run_dir"; then
    return 1
  fi
  if ! validate_smoke_results "$run_dir"; then
    smoke_diagnostics "$job_name"
    kubectl describe pod "$results_pod_name" -n kafka || true
    printf 'Smoke resources preserved because copied results failed validation\n' >&2
    return 1
  fi

  kubectl delete pod "$results_pod_name" -n kafka --wait=true
  kubectl delete job "$job_name" -n kafka --wait=true
  kubectl delete configmap "$config_map_name" -n kafka
  kubectl delete pvc "$pvc_name" -n kafka --wait=true
  rm -f "$rendered_job" "$rendered_results"
  printf 'Smoke baseline passed; results copied to %s\n' "$run_dir"
}

fault_experiment() {
  local profile=$1 mode=$2
  need kubectl
  require_cluster
  if [[ "$mode" == pilot && "${CONFIRM_DELETE:-}" != yes ]]; then
    fail "fault pilot requires CONFIRM_DELETE=yes; no Kubernetes resource was created"
  fi
  local suffix run_id job_name config_map_name pvc_name results_pod_name config_file mode_arg
  local rendered_job rendered_results run_dir
  suffix="$(printf '%s' "$profile" | tr '[:upper:]' '[:lower:]')"
  run_id="fault-${suffix}-$(date -u +%Y%m%d%H%M%S)-$(printf '%04x' "$RANDOM")"
  job_name="$run_id"
  config_map_name="$run_id-config"
  pvc_name="$run_id-results"
  results_pod_name="$run_id-copy"
  config_file="configs/profile-$suffix.yaml"
  run_dir="data/raw/$run_id"
  mode_arg="--dry-run"
  [[ "$mode" == pilot ]] && mode_arg="--confirm-delete"
  rendered_job="$(mktemp)"
  rendered_results="$(mktemp)"

  render_fault_manifest deployments/applications/fault-job.yaml "$rendered_job" \
    "$run_id" "$job_name" "$config_map_name" "$pvc_name" "$profile" "$mode_arg"
  render_smoke_manifest deployments/applications/smoke-results-pod.yaml "$rendered_results" \
    "$run_id" "$job_name" "$config_map_name" "$pvc_name" "$results_pod_name"

  kubectl apply -f deployments/applications/fault-runner-rbac.yaml
  kubectl create configmap "$config_map_name" -n kafka \
    --from-file=profile.yaml="$config_file" --from-file=versions.env=versions.env
  if ! kubectl create -f "$rendered_job"; then
    smoke_diagnostics "$job_name"
    printf 'Fault resources preserved because Job creation failed\n' >&2
    return 1
  fi
  if ! wait_smoke_job "$job_name" "${FAULT_JOB_TIMEOUT:-25m}"; then
    smoke_diagnostics "$job_name"
    copy_smoke_results "$run_id" "$job_name" "$results_pod_name" "$rendered_results" "$run_dir" || true
    printf 'Fault resources preserved for diagnosis: job/%s, configmap/%s, pvc/%s\n' \
      "$job_name" "$config_map_name" "$pvc_name" >&2
    return 1
  fi
  kubectl logs -n kafka "job/$job_name" --all-containers=true
  copy_smoke_results "$run_id" "$job_name" "$results_pod_name" "$rendered_results" "$run_dir"
  if ! validate_fault_results "$run_dir"; then
    smoke_diagnostics "$job_name"
    printf 'Fault resources preserved because copied results failed validation\n' >&2
    return 1
  fi
  kubectl delete pod "$results_pod_name" -n kafka --wait=true
  kubectl delete job "$job_name" -n kafka --wait=true
  kubectl delete configmap "$config_map_name" -n kafka
  kubectl delete pvc "$pvc_name" -n kafka --wait=true
  rm -f "$rendered_job" "$rendered_results"
  printf 'Fault %s passed; results copied to %s\n' "$mode" "$run_dir"
}

duration_seconds() {
  local value=$1
  [[ "$value" =~ ^[0-9]+[smh]$ ]] || return 1
  case "$value" in
    *s) printf '%d\n' "${value%s}" ;;
    *m) printf '%d\n' "$(( ${value%m} * 60 ))" ;;
    *h) printf '%d\n' "$(( ${value%h} * 3600 ))" ;;
    *) return 1 ;;
  esac
}

wait_smoke_job() {
  local job_name=$1 timeout=$2 timeout_seconds started condition
  timeout_seconds="$(duration_seconds "$timeout")" ||
    fail "unsupported SMOKE_JOB_TIMEOUT '$timeout'; use an integer followed by s, m, or h"
  started=$SECONDS
  while (( SECONDS - started < timeout_seconds )); do
    condition="$(kubectl get job "$job_name" -n kafka \
      -o jsonpath='{range .status.conditions[*]}{.type}={.status}{"\n"}{end}' 2>/dev/null || true)"
    if grep -Fxq 'Complete=True' <<<"$condition"; then
      printf 'complete\n'
      return 0
    fi
    if grep -Fxq 'Failed=True' <<<"$condition"; then
      printf 'error: job/%s reached Failed=True\n' "$job_name" >&2
      return 1
    fi
    sleep 2
  done
  printf 'error: timed out after %s waiting for job/%s\n' "$timeout" "$job_name" >&2
  return 1
}

copy_smoke_results() {
  local run_id=$1 job_name=$2 results_pod_name=$3 rendered_results=$4 run_dir=$5
  if ! kubectl create -f "$rendered_results"; then
    smoke_diagnostics "$job_name"
    printf 'Smoke resources preserved because the results pod could not be created\n' >&2
    return 1
  fi
  if ! kubectl wait -n kafka --for=condition=Ready "pod/$results_pod_name" --timeout=2m; then
    smoke_diagnostics "$job_name"
    kubectl describe pod "$results_pod_name" -n kafka || true
    printf 'Smoke resources preserved because the results pod was not ready: pod/%s\n' "$results_pod_name" >&2
    return 1
  fi
  if [[ -e "$run_dir" ]]; then
    smoke_diagnostics "$job_name"
    kubectl describe pod "$results_pod_name" -n kafka || true
    printf "error: refusing to overwrite existing results directory '%s'; Kubernetes resources were preserved\n" "$run_dir" >&2
    return 1
  fi
  mkdir -p "$run_dir"
  if ! kubectl cp -n kafka -c results "$results_pod_name:/results/$run_id/." "$run_dir"; then
    smoke_diagnostics "$job_name"
    kubectl describe pod "$results_pod_name" -n kafka || true
    printf 'Smoke resources preserved because result copying failed: pod/%s\n' "$results_pod_name" >&2
    return 1
  fi
}

render_smoke_manifest() {
  local source=$1 destination=$2 run_id=$3 job_name=$4 config_map_name=$5 pvc_name=$6 results_pod_name=$7
  sed \
    -e "s|\${RUN_ID}|$run_id|g" \
    -e "s|\${JOB_NAME}|$job_name|g" \
    -e "s|\${CONFIG_MAP_NAME}|$config_map_name|g" \
    -e "s|\${PVC_NAME}|$pvc_name|g" \
    -e "s|\${RESULTS_POD_NAME}|$results_pod_name|g" \
    "$source" >"$destination"
}

render_fault_manifest() {
  local source=$1 destination=$2 run_id=$3 job_name=$4 config_map_name=$5 pvc_name=$6 profile=$7 mode_arg=$8
  sed \
    -e "s|\${RUN_ID}|$run_id|g" \
    -e "s|\${JOB_NAME}|$job_name|g" \
    -e "s|\${CONFIG_MAP_NAME}|$config_map_name|g" \
    -e "s|\${PVC_NAME}|$pvc_name|g" \
    -e "s|\${PROFILE}|$profile|g" \
    -e "s|\${FAULT_MODE_ARG}|$mode_arg|g" \
    "$source" >"$destination"
}

smoke_diagnostics() {
  local job_name=$1
  kubectl logs -n kafka "job/$job_name" --all-containers=true || true
  kubectl describe job "$job_name" -n kafka || true
  kubectl describe pods -n kafka -l "job-name=$job_name" || true
  kubectl get events -n kafka --sort-by=.lastTimestamp || true
}

validate_smoke_results() {
  local run_dir=$1 file
  for file in metadata.json timeline.jsonl summary.json integrity.json; do
    if [[ ! -s "$run_dir/$file" ]]; then
      printf "error: required result '%s/%s' is missing or empty\n" "$run_dir" "$file" >&2
      return 1
    fi
  done
}

validate_fault_results() {
  local run_dir=$1 file
  for file in metadata.json fault-plan.json timeline.jsonl producer.jsonl consumer.jsonl \
    summary.json integrity.json kubernetes/pods-before.json kubernetes/pods-after.json \
    kubernetes/events.jsonl kafka/topic-before.json kafka/topic-during.json \
    kafka/topic-after.json recovery.json; do
    if [[ ! -e "$run_dir/$file" ]]; then
      printf "error: required fault result '%s/%s' is missing\n" "$run_dir" "$file" >&2
      return 1
    fi
  done
}

status() {
  require_cluster
  kubectl get pods -A
  kubectl get kafka,kafkanodepool,kafkatopic -n kafka
  kubectl get podmonitor -A
}

apps_down() {
  require_cluster
  kubectl delete -f deployments/applications/workloads.yaml --ignore-not-found
}

cluster_down() {
  need kind
  [[ "${CONFIRM_CLUSTER_DOWN:-}" == "$CLUSTER_NAME" ]] ||
    fail "destructive: rerun with CONFIRM_CLUSTER_DOWN=$CLUSTER_NAME; PVC/data are removed with the Kind cluster"
  [[ "${CONFIRM_DELETE_DATA:-}" == "yes" ]] ||
    fail "PVC/data deletion requires the additional flag CONFIRM_DELETE_DATA=yes"
  kind delete cluster --name "$CLUSTER_NAME"
}

case "${1:-}" in
  doctor) doctor ;;
  validate) validate ;;
  cluster-up) cluster_up ;;
  cluster-status) cluster_status ;;
  monitoring-up) monitoring_up ;;
  kafka-up) kafka_up ;;
  images) images ;;
  load-images) load_images ;;
  apps-up) apps_up ;;
  smoke-baseline) smoke_baseline ;;
  fault-dry-run-a) fault_experiment A dry-run ;;
  fault-pilot-a) fault_experiment A pilot ;;
  fault-dry-run-b) fault_experiment B dry-run ;;
  fault-pilot-b) fault_experiment B pilot ;;
  status) status ;;
  apps-down) apps_down ;;
  cluster-down) cluster_down ;;
  *) fail "unknown command '${1:-}'" ;;
esac
