#!/usr/bin/env python3
"""gRPC-сервис расчёта индикаторов (TA-Lib)."""

from __future__ import annotations

import logging
import os
from concurrent import futures

import grpc
from indicators import indicators_pb2_grpc

import envutil
from clickhouse_client import close_client, init_client
from servicer import IndicatorsServicer


def _warmup_libs() -> None:
    try:
        import numpy as np
        import talib

        dummy = np.ones(50, dtype=np.float64)
        talib.RSI(dummy, 14)
        talib.MACD(dummy)
        talib.BBANDS(dummy)
    except Exception as exc:
        logging.warning("Ошибка прогрева TA-Lib: %s", exc)

    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    envutil.load()
    _warmup_libs()
    # Не брать общий PORT из .env (там 9091 для Go API). Envoy ждёт 9093.
    port = (os.environ.get("INDICATORS_PORT") or "9093").strip()
    ch_enabled = False
    ch_client = None
    try:
        ch_client = init_client()
        ch_enabled = True
        logging.info("Постоянное подключение к ClickHouse успешно установлено")
    except Exception as exc:
        logging.warning("ClickHouse недоступен, ComputeForInstrument отключён: %s", exc)

    workers = int(os.environ.get("GRPC_WORKERS", "4"))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        options=[
            ("grpc.max_send_message_length", 32 * 1024 * 1024),
            ("grpc.max_receive_message_length", 32 * 1024 * 1024),
        ],
    )
    indicators_pb2_grpc.add_IndicatorsServicer_to_server(
        IndicatorsServicer(ch_enabled=ch_enabled, client=ch_client),
        server,
    )
    # 0.0.0.0: Envoy ходит сюда через host.docker.internal (IPv4). [::] на Windows не dual-stack.
    listen = f"0.0.0.0:{port}"
    server.add_insecure_port(listen)
    server.start()
    logging.info("indicators слушает gRPC на %s", listen)
    try:
        server.wait_for_termination()
    finally:
        close_client()


if __name__ == "__main__":
    main()
