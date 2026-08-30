name = trb
envoy_config = ./configs/envoy/envoy.yaml
go_dockerfile = ./build/docker/services/go/Dockerfile
postgre_1c_dockerfile = ./build/docker/postgre-1c-db/Dockerfile
cmd_nats = ./internal/services/nats/cmd/main.go
cmd_clickhouse = ./internal/services/clickhouse/cmd/main.go
cmd_historiccandle = ./internal/services/historicCandle/cmd/main.go
cmd_historiccandle_scheduler = ./internal/services/historicCandle_scheduler/cmd/main.go
cmd_invest = ./internal/services/invest/cmd/main.go
cmd_postgre = ./internal/services/postgre/cmd/main.go
cmd_test = ./internal/services/test/cmd/main.go
cmd_indicators = ./internal/services/indicators/main.py
indicators_dockerfile = ./build/docker/services/indicators/Dockerfile
TRB_PROTO_REF ?= main
TRB_PROTO_VERSION ?= 1.0.34
TRB_PROTO_SOURCE ?= pypi

.PHONY: build up upd down envoy envoy_proto_sync nats clickhouse historicCandle historicCandleScheduler postgre postgre-1c-db test services gene invest ver indicators

up:
	docker-compose --project-name=${name} up -d

down:
	docker-compose --project-name=${name} down

ENVOY_IMAGE ?= envoyproxy/envoy
ENVOY_VARIANT ?= v1.36.4

envoy_proto_sync:
	python -c "import shutil, pathlib, sys; src=pathlib.Path('../TrB_proto/gen/desc/trb_protos.pb'); dst=pathlib.Path('configs/envoy/protos/trb_protos.pb'); dst.parent.mkdir(parents=True, exist_ok=True); (shutil.copy2(src, dst) if src.is_file() else sys.exit('TrB_proto/gen/desc/trb_protos.pb не найден — выполните make gene в TrB_proto'))"

envoy: envoy_proto_sync
	docker build -f ./build/docker/envoy/Dockerfile . --build-arg ENVOY_IMAGE=${ENVOY_IMAGE} --build-arg ENVOY_VARIANT=${ENVOY_VARIANT} --build-arg ENVOY_CONFIG=${envoy_config} -t envoy:latest

nats:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_nats} -t nats-api:latest

clickhouse:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_clickhouse} -t clickhouse-api:latest

historicCandle:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_historiccandle} -t historiccandle:latest

historicCandleScheduler:
	@test -f ${cmd_historiccandle_scheduler} || (echo "historicCandle_scheduler: каталог internal/services/historicCandle_scheduler ещё не реализован" && exit 1)
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_historiccandle_scheduler} -t historiccandle-scheduler:latest

invest:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_invest} -t invest:latest

postgre:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_postgre} -t postgre:latest

postgre-1c-db:
	docker build -f ${postgre_1c_dockerfile} ./build/docker/postgre-1c-db -t postgre-1c-db:latest

test:
	docker build -f ${go_dockerfile} . --build-arg CMD_PATH=${cmd_test} -t test:latest

indicators:
	python ${cmd_indicators}

indicators-docker:
	docker build -f ${indicators_dockerfile} . \
		--build-context proto=../TrB_proto \
		-t indicators:latest

ver:
	go get github.com/Mar1eena/trb_proto@latest
	go mod tidy

# Сначала обновляет trb_proto, затем собирает все сервисы
# historicCandleScheduler — пока не реализован (см. README)
build: gene ver historicCandle invest postgre postgre-1c-db test clickhouse nats envoy

make upd: build up

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
