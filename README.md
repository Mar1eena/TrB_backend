# TrB_backend

Go-бэкенд платформы для сбора рыночных данных с **Tinkoff Invest API** (T-Bank), хранения в **ClickHouse** и расчёта технических индикаторов через очередь задач **NATS JetStream**.

Инфраструктура поднимается через **Docker Compose**. Веб-интерфейс живёт в соседнем репозитории [`TrB_frontend`](../TrB_frontend).

## Словарь

| Сокращение | Расшифровка |
|---|---|
| **hct** | historical candles tinkoff — исторические свечи |
| **ct** | candles tinkoff — потоковые (real-time) свечи |
| **sht** | shares — справочник акций |

## Архитектура

```
Tinkoff Invest API
        │
        ▼
┌───────────────────────────────────────────────────┐
│  Go-сервисы                                       │
│  shares │ historicCandle │ invest │ ...                   │
└───────┬─────────────┬──────────────┬────────────┘
        │             │              │
        ▼             ▼              ▼
   ClickHouse      NATS JetStream   Redis
        │             │
        └─────────────┴──► manager_indicators → manager_indicators_executor
```

**Основной поток данных:**

1. `shares` загружает справочник инструментов в ClickHouse (`TrB.sht`).
2. `historicCandle` догружает исторические свечи в `TrB.hct` по заданиям из NATS (`TrB.HistoricCandle.Task.{uid}.{interval}`).
3. `MarketData` (опционально) получает потоковые свечи и пишет в `TrB.Candle` / `TrB.hct`.
4. `manager_indicators` читает задачи из NATS, кэширует свечи в Redis и передаёт задачу исполнителю.
5. `manager_indicators_executor` считает индикаторы (RSI и др.) по данным из ClickHouse.

**Инфраструктура:**

| Компонент | Назначение | Порт (хост) |
|---|---|---|
| ClickHouse / ClickStack | Рыночные данные + логи (OTLP) | 8123, 9000, 4317/4318, **8080** (HyperDX) |
| NATS JetStream | Очередь сообщений | 4222 |
| Redis | Кэш свечей для индикаторов | 6380 |
| Envoy | gRPC/REST-шлюз | 8081, 9901 (admin) |
| Grafana | Дашборды (метрики) | 3001 |
| Prometheus | Метрики | — |
| Vector | Сбор логов контейнеров → OTLP | 8686 (API) |

---

## Требования

- Docker и Docker Compose
- Go 1.26+ (для локальной сборки)
- `make` (опционально)
- Токен [Tinkoff Invest API](https://developer.tbank.ru/invest/intro/intro/)

---

## Быстрый старт

### 1. Настройка окружения

Создайте файл `.env` в корне проекта. Минимальный набор переменных:

```env
# Tinkoff Invest API
INVEST_TOKEN=your_token_here
INVEST_ENDPOINT=sandbox-invest-public-api.tinkoff.ru:443
INVEST_ACCOUNT_ID=
INVEST_APP_NAME=TrB_v3

# ClickHouse
CLICKHOUSE_URL=localhost:9000
CLICKHOUSE_URL_DOCKER=clickhouse-db:9000
CLICKHOUSE_DATABASE=TrB
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default
CLICKHOUSE_DEBUG=0

# NATS
NATS_URL=nats://localhost:4222
NATS_URL_DOCKER=nats://nats-server:4222

# Redis
REDIS_URL=localhost:6380
REDIS_URL_DOCKER=redis:6379
REDIS_PASSWORD=your_redis_password
REDIS_USER=default
REDIS_USER_PASSWORD=your_redis_password

# Сервисы
PORT=9091
```

> Локально сервисы читают `NATS_URL` / `CLICKHOUSE_URL` / `REDIS_URL`.
> В Docker (образ с `APP_RUNTIME=docker`) автоматически берутся `*_DOCKER` варианты.
> Файл `.env` опционален, если переменные уже заданы в окружении.

### 2. Сборка образов

```bash
make shares
make historicCandle
make mng_i
make nats
# опционально:
make marketdata
make envoy
```

Или одной командой с поднятием контейнеров:

```bash
make upd
```

### 3. Запуск

```bash
make up      # поднять без пересборки
make upd     # пересобрать и поднять
make down    # остановить
```

---

## Сервисы

### `shares`

Синхронизирует справочник акций из Tinkoff Instruments API в таблицу `TrB.sht`.

```bash
make shares
```

Запускается в compose по умолчанию. Без актуального справочника `historicCandle` не сможет определить точку начала загрузки для инструмента.

---

### `historicCandle`

Догружает **исторические свечи** в `TrB.hct` через REST API Tinkoff (`GetCandles`).
Задания принимает **только** из JetStream-стрима `historic_candle`.

**Инвариант очереди:** на каждый subject `TrB.HistoricCandle.Task.{uid}.{interval}` в стриме может быть не больше **одного** сообщения. Пока воркер не подтвердил задание (ACK), повторная публикация в тот же subject отклоняется. После ACK слот освобождается.

Это обеспечивается настройками стрима (`maxmsgspersubject: 1`, `discard: New`, `discardnewpersubject: true`) и публикацией с `ExpectLastSequencePerSubject(0)`.

**Переменные окружения:**

```env
HCT_NATS_STREAM=historic_candle                 # JetStream-стрим
HCT_NATS_SUBJECT=TrB.HistoricCandle.Task.*.*    # фильтр subject'ов
HCT_NATS_CONSUMER=historic_candle_cons          # durable pull consumer
INTERVAL_UPDATE_HC=60                           # мин. интервал между догрузками (сек)
```

**Формат NATS-задания** (`HistoricCandleLoadTask`, protobuf):

```protobuf
message HistoricCandleLoadTask {
    repeated string uid = 1;   // достаточно одного UID; источник истины — subject
    int32 interval = 2;        // pb.CandleInterval; 0 = 1 минута
}
```

Subject: `TrB.HistoricCandle.Task.{uid}.{interval}`. Воркер берёт uid и interval из subject; тело сообщения — запасной вариант.

**Пример публикации задания** (Go):

```go
import (
    "fmt"
    format_schemas "github.com/Mar1eena/TrB_V3/configs/clickhouse/format_schemas"
    pb "opensource.tbank.ru/invest/invest-go/proto"
    "google.golang.org/protobuf/proto"
    "github.com/nats-io/nats.go"
)

uid := "8be64b53-a46b-451c-8152-1c871f122d5b"
interval := int32(pb.CandleInterval_CANDLE_INTERVAL_1_MIN)
task := &format_schemas.HistoricCandleLoadTask{
    Uid:      []string{uid},
    Interval: interval,
}
data, _ := proto.Marshal(task)
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()
_, err := js.Publish(
    fmt.Sprintf("TrB.HistoricCandle.Task.%s.%d", uid, interval),
    data,
    nats.ExpectLastSequencePerSubject(0), // не ставить второе задание на тот же subject
)
```

UID инструментов должны присутствовать в `TrB.sht`. Обычно задания публикует `historicCandle_scheduler`.

```bash
make historicCandle
```

---

### `historicCandle_scheduler`

Оркестратор: по тику смотрит отставание свечей в ClickHouse и публикует задания в `TrB.HistoricCandle.Task.{uid}.{interval}`. Если на subject уже есть сообщение, публикация пропускается.

```bash
make historicCandleScheduler
```

---

### `MarketData`

Подписывается на **потоковые свечи** Tinkoff (`MarketDataStream`) и пишет их в ClickHouse.

В `docker-compose.yml` по умолчанию закомментирован. Для включения:

```bash
make marketdata
# раскомментировать сервис marketdata в docker-compose.yml
```

---

### `manager_indicators`

Планировщик расчёта индикаторов:

1. Читает задачи из NATS (`TrB.Indicator.Task`).
2. Загружает историю свечей из `TrB.hct`.
3. Сохраняет свечи в Redis.
4. Публикует задачу исполнителю: `TrB.Indicator.Task.{uid}.{interval}.RSI`.

```bash
make mng_i
```

---

### `manager_indicators_executor`

Исполнитель задач на расчёт индикаторов. Читает `TrB.Indicator.Task.>`, считает RSI (TA-Lib) по данным из ClickHouse.

В compose по умолчанию не поднят; запускается локально:

```bash
go run ./internal/services/manager_indicators_executor/cmd/
```

---

### `api/invest` (T-Bank Invest API)

Единый gRPC-прокси на основе `opensource.tbank.ru/invest/invest-go/proto` в `internal/services/api/invest/`. Один процесс регистрирует все T-Invest сервисы:

- `UsersService` — счета, тариф, маржинальные показатели, инфо, переводы
- `InstrumentsService` — справочники акций, облигаций, валют, ETF, фьючерсов, опционов
- `MarketDataService` / `MarketDataStreamService` — свечи, цены, стакан, стримы
- `OperationsService` / `OperationsStreamService` — портфель, позиции, операции
- `OrdersService` / `OrdersStreamService` — торговые поручения и стримы сделок
- `SandboxService` — песочница
- `SignalService` — торговые сигналы
- `StopOrdersService` — стоп-заявки

```bash
make invest
make envoy
make upd
```

Локально:

```bash
go run ./internal/services/api/invest/cmd/
```

---

### `gateway`

Универсальный WebSocket-шлюз к NATS. UI открывает `/ws` и шлёт:

```json
{ "id": "...", "action": "request", "service": "historicCandle", "method": "...", "params": {} }
```

Конвенция subject'ов (новый сервис подключается ею, без правок шлюза, если тело JSON):

| Назначение | Subject | Транспорт |
|---|---|---|
| Задание | `TrB.{Service}.Task.{Method}` | JetStream |
| Статус (push) | `TrB.{Service}.Status.{task_id}.{kind}` | Core NATS |
| Данные | `TrB.{Service}.Data.{Method}.{task_id}` | JetStream + live subscribe |

Шлюз отдаёт в сокет `push` (accepted / ready / done / failed), затем `data`, затем `complete`. Тело по умолчанию — JSON.

- Порт: `9092` (`GATEWAY_PORT`)
- В dev UI (`TrB_frontend`) ходит на `ws://127.0.0.1:9092/ws`

```bash
make gateway
go run ./internal/services/gateway/cmd/
```

---

### `data`

gRPC API веб-клиента к PostgreSQL: цели планировщика. Браузер ходит через Envoy (gRPC-Web / JSON), как к `nats`. Произвольный SQL с клиента не принимается. Справочник акций и история догрузок — сервис `clickhouse`.

Контракт: `trb.postgresql.v1.PostgreSQL` в [TrB_proto](https://github.com/Mar1eena/TrB_proto) (`services/postgresql/postgresql.proto`).

| RPC | Назначение |
|---|---|
| `ListSchedulerTargets` | Цели догрузки свечей |
| `SyncSchedulerTargets` | Замена набора целей |

Новый экран: добавить RPC в proto, `make gene` в TrB_proto, опубликовать модуль, обновить `github.com/Mar1eena/trb_proto` в `go.mod`, обработчик в `internal/services/api/data/server`. Envoy уже проксирует весь префикс сервиса.

```bash
make data
go run ./internal/services/api/data/cmd/
```

JSON (через Envoy :8081): `GET /v1/scheduler/targets`, `PUT /v1/scheduler/targets`.

---

### `clickhouse`

gRPC API ClickHouse: админка схемы (DDL) и бизнес-логика (`TrB.sht`, `hct_last_download`). Произвольный SQL с клиента в админке не принимается — идентификаторы и выражения проверяются на сервере. Native протокол ClickHouse — `clickhouse.grpc.ClickHouse`.

Контракты в [TrB_proto](https://github.com/Mar1eena/TrB_proto): `trb.clickhouse.v1.ClickHouse_Admin` (`services/clickhouse/admin.proto`) и `trb.clickhouse.v1.ClickHouse` (`services/clickhouse/clickhouse.proto`).

| RPC (`ClickHouse`) | Назначение |
|---|---|
| `ListInstruments` | Справочник акций |
| `ListInstrumentVersions` | История версий инструмента |
| `UpsertInstruments` | Запись акций в `TrB.sht` |
| `ListLastDownloads` | История догрузок |

```bash
make clickhouse
go run ./internal/services/api/clickhouse/cmd/
```

JSON (через Envoy :8081): `GET /v1/clickhouse/ping`, `GET /v1/instruments`, `GET /v1/historic-candles/last-downloads`.

---

### `nats`

gRPC-сервис управления JetStream: создание/обновление стримов, consumer'ов, списки, purge, удаление сообщений.

- Контракт: `trb_proto` (`trb.nats.v1.Nats_Admin`). Списки стримов/консьюмеров — unary RPC (`StreamList`, `ConsumerList`), не server-streaming.
- Доступен через Envoy: gRPC `/trb.nats.v1.Nats_Admin` и REST `/v1/nats/...`.

```bash
make nats
```

---

## NATS JetStream

Основные стримы (см. `configs/nats-server/nats-settings.json`):

| Стрим | Subject | Назначение |
|---|---|---|
| `historic_candle` | `TrB.HistoricCandle.Task.*.*` | Задания на догрузку свечей (1 сообщение на uid+interval) |
| `manager_indicators` | `TrB.Indicator.Task` | Задачи на расчёт индикаторов |
| `manager_indicators_executor` | `TrB.Indicator.Task.*.*.*` | Задачи для исполнителя |
| `manager_trades` | `TrB.Trade` | Обезличенные сделки |

Стримы и consumer'ы создаются через API `nats` (JetStream manager). YAML в `configs/nats-server/` — справочный шаблон, сервис его при старте не применяет.

---

## ClickHouse

Схемы таблиц: `configs/clickhouse/init.d/`.

| Таблица | Описание |
|---|---|
| `TrB.sht` | Справочник акций |
| `TrB.hct` | Исторические свечи (ReplacingMergeTree) |
| `TrB.Candle` | Потоковые свечи |
| `TrB.hct_last_download` | Метаданные последней догрузки |

Часть таблиц подключена к NATS через engine `NATS` — сообщения из JetStream автоматически попадают в ClickHouse через materialized views.

Подключение к ClickHouse:

```bash
clickhouse-client --host localhost --port 9000 --database TrB
```

HTTP-интерфейс: `http://localhost:8123`.

---

## Envoy (API Gateway)

Envoy проксирует gRPC-сервисы и предоставляет JSON transcoding. Descriptor set для grpc-json-transcoder скачивается при сборке образа из [TrB_proto](https://github.com/Mar1eena/TrB_proto) (`gen/desc/trb_protos.pb`, ветка/тег `TRB_PROTO_REF`, по умолчанию `main`).

| Маршрут | Сервис |
|---|---|
| `/tinkoff.public.invest.api.contract.v1.*` | invest |
| `/trb.nats.v1.Nats_Admin` | nats (gRPC) |
| `/v1/nats` | nats (JSON REST, grpc-json-transcoder) |
| `/trb.postgresql.v1.PostgreSQL` | data (gRPC) |
| `/v1/scheduler` | data (JSON REST) |
| `/trb.clickhouse.v1.ClickHouse_Admin` | clickhouse admin (gRPC) |
| `/trb.clickhouse.v1.ClickHouse` | clickhouse (gRPC) |
| `/v1/clickhouse`, `/v1/instruments`, `/v1/historic-candles` | clickhouse (JSON REST) |
| `/trb.test.v1.Test` | test (gRPC) |
| `/clickhouse.grpc.ClickHouse` | ClickHouse gRPC |

Админка Envoy: `http://localhost:9901`.

```bash
make envoy
```

---

## Мониторинг

- **HyperDX (логи):** http://localhost:8080 — без регистрации (`IS_LOCAL_APP_MODE`). Дашборд **Services** — логи по сервисам, ошибки, переподключения.
- **Vector API:** http://localhost:8686 — health/метрики пайплайна логов

### Логи сервисов → ClickHouse (ClickStack)

Пайплайн:

```
Go-сервис (zlog JSON: level/message/error)
    → Docker json-file                    (compose logging driver)
    → Vector (configs/vector/vector.yaml)  (severity + атрибуты uid/dep/…)
    → OTLP HTTP :4318  (или gRPC :4317)
    → ClickStack OTEL collector
    → default.otel_* в ClickHouse
    → HyperDX UI :8080 / clickhouse-client
```

Рыночные данные — БД `TrB`; логи — `default.otel_logs` (схема ClickStack).

**Первый запуск**

1. `docker compose up -d clickhouse vector` (или весь стек).
2. http://localhost:8080 — без регистрации (локальный noauth entrypoint).
3. Search / Logs — источник **Logs** (`otel_logs`). Connection: user `default`, password `default` (как в `.env`).

Прямой OTLP из Go (без Docker/Vector), локально:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=my-go-service
# в noauth-режиме authorization не нужен
```

Дашборды и saved searches HyperDX хранятся в MongoDB контейнера ClickStack (`/data/db`), не в ClickHouse. Том `hyperdx_data` переживает удаление контейнера; `clickhouse_data` — только таблицы. После смены томов: `docker compose up -d clickhouse-db`.

> Если HyperDX пишет `Invalid authentication: ... X-ClickHouse ... Authorization ...` — патч `configs/clickhouse/hyperdx/clickhouseProxy.js` уже в compose. Пересоздайте: `docker compose up -d clickhouse-db --force-recreate`.

**Проверка**

```bash
docker logs trb-vector-1 --tail 50
curl http://localhost:8686/health

docker exec -it trb-clickhouse-1 clickhouse-client --query "
  SELECT Timestamp, ServiceName, SeverityText, Body
  FROM default.otel_logs
  WHERE ScopeName = 'vector.docker_logs'
  ORDER BY Timestamp DESC
  LIMIT 20
"
```

**Полезные запросы**

```sql
SELECT Timestamp, SeverityText, Body
FROM default.otel_logs
WHERE ServiceName = 'historiccandle' AND SeverityText IN ('error', 'ERROR', 'fatal', 'FATAL')
ORDER BY Timestamp DESC
LIMIT 100;

SELECT ServiceName, count() AS c
FROM default.otel_logs
WHERE Timestamp > now() - INTERVAL 1 HOUR
GROUP BY ServiceName
ORDER BY c DESC;
```

HTTP ClickHouse: `http://localhost:8123`. TTL логов — по схеме ClickStack (~30 дней).

Дашборды Grafana: `configs/grafana/provisioning/dashboards/`.

---

## Локальная разработка

Запуск отдельного сервиса без Docker:

```bash
# из корня репозитория, с настроенным .env
go run ./internal/services/historicCandle/cmd/
go run ./internal/services/shares/cmd/
go run ./internal/services/manager_indicators/cmd/
go run ./internal/services/api/invest/cmd/
go run ./internal/services/api/data/cmd/
go run ./internal/services/api/clickhouse/cmd/
```

Генерация protobuf-схем для ClickHouse/NATS:

```bash
make gene
```

Сборка Docker-образа произвольного сервиса:

```bash
docker build -f ./build/docker/services/go/Dockerfile . \
  --build-arg CMD_PATH=./internal/services/historicCandle/cmd/main.go \
  -t historiccandle:latest
```

---

## Структура репозитория

```
TrB_backend/
├── configs/
│   ├── clickhouse/       # SQL-схемы, protobuf format_schemas
│   ├── vector/           # сбор логов контейнеров → ClickStack OTLP
│   ├── nats-server/      # streams.yaml, consumers.yaml
│   ├── envoy/            # конфиг API-шлюза
│   └── grafana/          # дашборды
├── internal/
│   ├── pkg/              # общие библиотеки (investgo, clickhouse, trb_nats, ...)
│   └── services/         # микросервисы
├── build/docker/         # Dockerfile'ы
├── tests/                # интеграционные тесты и утилиты
├── docker-compose.yml
├── Makefile
└── .env                  # переменные окружения (не коммитить)
```

Веб-UI: `../TrB_frontend` (Vite + React, dev-сервер на порту 3002, прокси на Envoy `:8081` и gateway `:9092`).

### Общие пакеты (`internal/pkg/`)

| Пакет | Описание |
|---|---|
| `investgo` | Обёртка над Tinkoff Invest API, вставка в ClickHouse |
| `clickhouse` | Клиент ClickHouse (native + gRPC) |
| `trb_nats` | NATS/JetStream, загрузка YAML-конфигов |
| `trb_redis` | Redis-клиент, кэш свечей |
| `indicators` | Реализация RSI |
| `zlog` | Логирование (zerolog) |

---

## Типичный сценарий использования

1. Заполнить `.env` токеном Tinkoff.
2. Поднять инфраструктуру: `make upd`.
3. Дождаться загрузки справочника акций (`shares` → `TrB.sht`).
4. Дождаться, пока `historicCandle_scheduler` опубликует задания в `TrB.HistoricCandle.Task.{uid}.{interval}` (или опубликовать subject вручную).
5. Проверить данные в ClickHouse: `SELECT * FROM TrB.hct FINAL LIMIT 10`.
6. При необходимости запустить пайплайн индикаторов (`manager_indicators` + `manager_indicators_executor`).

---

## Зависимости

- [invest-go](https://opensource.tbank.ru/invest/invest-go) — SDK Tinkoff Invest API
- [trb_proto](https://github.com/Mar1eena/TrB_proto) — gRPC-контракты проекта
- ClickHouse / ClickStack, NATS, Redis, Envoy, Grafana, Prometheus, Vector
