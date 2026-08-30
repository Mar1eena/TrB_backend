#!/usr/bin/env python3
"""gRPC-сервис расчёта индикаторов (TA-Lib)."""

from __future__ import annotations

import logging
import os
from concurrent import futures

import grpc
from indicators import indicators_pb2_grpc

from clickhouse_client import check_connection
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
    _warmup_libs()
    port = os.environ.get("PORT", "9093")
    ch_enabled = False
    try:
        check_connection()
        ch_enabled = True
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
    indicators_pb2_grpc.add_IndicatorsServicer_to_server(IndicatorsServicer(ch_enabled), server)
    listen = f"[::]:{port}"
    server.add_insecure_port(listen)
    server.start()
    logging.info("indicators слушает gRPC на %s", listen)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
