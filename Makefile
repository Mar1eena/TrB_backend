name = trb
envoy_config = ./configs/envoy/envoy.yaml
go_dockerfile = ./build/docker/services/go/Dockerfile
cmd_nats = ./internal/services/api/nats/cmd/main.go
cmd_clickhouse = ./internal/services/api/clickhouse/cmd/main.go
cmd_historiccandle = ./internal/services/historicCandle/cmd/main.go
cmd_historiccandle_scheduler = ./internal/services/historicCandle_scheduler/cmd/main.go
cmd_invest = ./internal/services/api/invest/cmd/main.go
cmd_data = ./internal/services/api/data/cmd/main.go
cmd_test = ./internal/services/test/cmd/main.go
TRB_PROTO_REF ?= main

.PHONY: build up upd down envoy nats clickhouse historicCandle historicCandleScheduler data test services gene invest

build:
	docker-compose --project-name=${name} build

up:
	docker-compose --project-name=${name} up -d

upd:
	docker-compose --project-name=${name} up --build -d

down:
	docker-compose --project-name=${name} down

ENVOY_IMAGE ?= envoyproxy/envoy
ENVOY_VARIANT ?= v1.36.4

envoy:
	docker build -f ./build/docker/envoy/Dockerfile . --build-arg ENVOY_IMAGE=${ENVOY_IMAGE} --build-arg ENVOY_VARIANT=${ENVOY_VARIANT} --build-arg ENVOY_CONFIG=${envoy_config} --build-arg TRB_PROTO_REF=${TRB_PROTO_REF} -t envoy:latest

nats:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_nats} -t nats-api:latest

clickhouse:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_clickhouse} -t clickhouse-api:latest

historicCandle:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_historiccandle} -t historiccandle:latest

historicCandleScheduler:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_historiccandle_scheduler} -t historiccandle-scheduler:latest

invest:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_invest} -t invest:latest

data:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_data} -t data:latest

test:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_test} -t test:latest

# Последовательная сборка всех Go-сервисов
services: historicCandle historicCandleScheduler invest data test clickhouse nats envoy

gene:
	protoc -I./configs/clickhouse/format_schemas \
		./configs/clickhouse/format_schemas/*.proto \
		--go_out=./configs/clickhouse/format_schemas \
		--go_opt=paths=source_relative \
		--go-grpc_out=./configs/clickhouse/format_schemas \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=./configs/clickhouse/format_schemas \
		--grpc-gateway_opt=paths=source_relative \
		--include_imports \
		--include_source_info \
		--descriptor_set_out=./configs/clickhouse/format_schemas/descriptor.pb \
		--python_out=./configs/clickhouse/format_schemas \
		--pyi_out=./configs/clickhouse/format_schemas
