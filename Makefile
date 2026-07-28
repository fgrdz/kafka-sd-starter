SHELL := bash
.DEFAULT_GOAL := help
SMOKE_JOB_TIMEOUT ?= 8m

.PHONY: help doctor validate fmt test vet build cluster-up cluster-status monitoring-up kafka-up images load-images apps-up smoke-baseline status apps-down cluster-down

help:
	@echo "Use: make doctor|validate|test|cluster-up|monitoring-up|kafka-up|images|load-images|apps-up|smoke-baseline|status"
	@echo "smoke-baseline runs kafka-sd-starter-runner:dev as a Job in namespace kafka (timeout: $(SMOKE_JOB_TIMEOUT))"

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

status:
	@bash scripts/infra.sh status

apps-down:
	@bash scripts/infra.sh apps-down

cluster-down:
	@bash scripts/infra.sh cluster-down
