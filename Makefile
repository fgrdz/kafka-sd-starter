SHELL := bash
.DEFAULT_GOAL := help
SMOKE_JOB_TIMEOUT ?= 8m
FAULT_JOB_TIMEOUT ?= 25m
BASELINE_JOB_TIMEOUT ?= 15m
REPETITION ?= 1
REPETITIONS ?= 5
COOLDOWN_SECONDS ?= 30

.PHONY: help doctor validate fmt test vet build cluster-up cluster-status monitoring-up kafka-up images load-images apps-up smoke-baseline baseline-a baseline-b fault-a fault-b fault-dry-run-a fault-pilot-a fault-dry-run-b fault-pilot-b matrix aggregate matrix-check status apps-down cluster-down

help:
	@echo "Use: make doctor|validate|test|cluster-up|monitoring-up|kafka-up|images|load-images|apps-up|smoke-baseline|baseline-a|baseline-b|fault-a|fault-b|matrix|matrix-check|aggregate|status"
	@echo "smoke-baseline runs kafka-sd-starter-runner:dev as a Job in namespace kafka (timeout: $(SMOKE_JOB_TIMEOUT))"
	@echo "fault-pilot-* requires CONFIRM_DELETE=yes (timeout: $(FAULT_JOB_TIMEOUT))"
	@echo "Official runs use REPETITION=$(REPETITION); matrix uses REPETITIONS=$(REPETITIONS) and COOLDOWN_SECONDS=$(COOLDOWN_SECONDS)"
	@echo "fault-a, fault-b, and matrix require CONFIRM_DELETE=yes"

doctor:
	@bash scripts/infra.sh doctor

validate:
	@bash scripts/infra.sh validate

fmt:
	gofmt -w .

test:
	go test ./...

vet:
	go vet ./...

build:
	go build ./...

cluster-up:
	@bash scripts/infra.sh cluster-up

cluster-status:
	@bash scripts/infra.sh cluster-status

monitoring-up:
	@bash scripts/infra.sh monitoring-up

kafka-up:
	@bash scripts/infra.sh kafka-up

images:
	@bash scripts/infra.sh images

load-images:
	@bash scripts/infra.sh load-images

apps-up:
	@bash scripts/infra.sh apps-up

smoke-baseline:
	@SMOKE_JOB_TIMEOUT="$(SMOKE_JOB_TIMEOUT)" bash scripts/infra.sh smoke-baseline

baseline-a:
	@BASELINE_JOB_TIMEOUT="$(BASELINE_JOB_TIMEOUT)" bash scripts/infra.sh baseline A "$(REPETITION)"

baseline-b:
	@BASELINE_JOB_TIMEOUT="$(BASELINE_JOB_TIMEOUT)" bash scripts/infra.sh baseline B "$(REPETITION)"

fault-a:
	@test "$(CONFIRM_DELETE)" = yes || { echo "error: fault-a requires CONFIRM_DELETE=yes; no Job was created" >&2; exit 1; }
	@CONFIRM_DELETE=yes FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault A "$(REPETITION)" official

fault-b:
	@test "$(CONFIRM_DELETE)" = yes || { echo "error: fault-b requires CONFIRM_DELETE=yes; no Job was created" >&2; exit 1; }
	@CONFIRM_DELETE=yes FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault B "$(REPETITION)" official

fault-dry-run-a:
	@FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault-dry-run-a

fault-pilot-a:
	@test "$(CONFIRM_DELETE)" = yes || { echo "error: fault-pilot-a requires CONFIRM_DELETE=yes; no Job was created" >&2; exit 1; }
	@CONFIRM_DELETE=yes FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault-pilot-a

fault-dry-run-b:
	@FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault-dry-run-b

fault-pilot-b:
	@test "$(CONFIRM_DELETE)" = yes || { echo "error: fault-pilot-b requires CONFIRM_DELETE=yes; no Job was created" >&2; exit 1; }
	@CONFIRM_DELETE=yes FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/infra.sh fault-pilot-b

matrix:
	@test "$(CONFIRM_DELETE)" = yes || { echo "error: matrix requires CONFIRM_DELETE=yes; no experiment was started" >&2; exit 1; }
	@CONFIRM_DELETE=yes REPETITIONS="$(REPETITIONS)" COOLDOWN_SECONDS="$(COOLDOWN_SECONDS)" \
		BASELINE_JOB_TIMEOUT="$(BASELINE_JOB_TIMEOUT)" FAULT_JOB_TIMEOUT="$(FAULT_JOB_TIMEOUT)" bash scripts/matrix.sh

aggregate:
	@python3 scripts/matrix_results.py aggregate --raw-dir data/raw --output-dir data/processed

matrix-check:
	@python3 scripts/matrix_results.py check --raw-dir data/raw --repetitions "$(REPETITIONS)"

status:
	@bash scripts/infra.sh status

apps-down:
	@bash scripts/infra.sh apps-down

cluster-down:
	@bash scripts/infra.sh cluster-down
