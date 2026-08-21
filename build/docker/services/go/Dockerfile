# ========== Builder ==========
FROM golang:1.26.3-alpine AS builder

WORKDIR /app

ARG CMD_PATH=main.go
ARG SERVICE_NAME

# Зависимости (с кэшем для ускорения пересборки)
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x && go mod verify

# Только исходники, нужные для сборки (без docs/, build/, docker-compose и т.д.)
COPY internal/ internal/
COPY configs/ configs/

# Сертификаты НУЦ Минцифры в системный trust store (нужны для T-Invest / российских API)
RUN apk add --no-cache ca-certificates && \
    mkdir -p /usr/local/share/ca-certificates/russian-trusted && \
    find configs/CA -type f \( -name '*.crt' -o -name '*.pem' -o -name '*.cer' \) | while read -r f; do \
      if grep -q "BEGIN CERTIFICATE" "$f"; then \
        base=$(basename "$f"); \
        cp "$f" "/usr/local/share/ca-certificates/russian-trusted/${base%.*}.crt"; \
      fi; \
    done && \
    update-ca-certificates

# Статический бинарник без отладочной информации
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s -extldflags '-static'" \
    -trimpath \
    -buildvcs=false \
    -o /app/service "$CMD_PATH"

# ========== Runtime (минимальный образ) ==========
FROM scratch

# CA-сертификаты (включая НУЦ Минцифры) для HTTPS/gRPC
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/service /app/service

WORKDIR /app
ENV APP_RUNTIME=docker
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
USER 65534:65534

ENTRYPOINT ["/app/service"]
