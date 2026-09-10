FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

# TA-Lib C-библиотека нужна для backtrader bt.talib и обёртки TA-Lib.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential wget ca-certificates \
    && wget -q https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb \
    && dpkg -i ta-lib_0.6.4_amd64.deb \
    && rm ta-lib_0.6.4_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

COPY internal/services/strategy/eval-worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Слой сбрасывается, когда на PyPI выходит новая версия trb-proto.
ADD https://pypi.org/pypi/trb-proto/json /tmp/trb-proto.json
RUN pip install --no-cache-dir --upgrade trb-proto && rm -f /tmp/trb-proto.json

# _common раскладывается в тот же /app плоско — импорты остаются без префикса.
COPY internal/services/strategy/_common/ /app/
COPY internal/services/strategy/eval-worker/ /app/
RUN rm -rf /app/tests

# HTTP /metrics, /healthz, /readyz (STRATEGY_METRICS_ADDR, по умолчанию :9106)
EXPOSE 9106

CMD ["python", "/app/main.py"]
