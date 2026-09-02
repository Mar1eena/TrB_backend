#!/usr/bin/env python3
"""gRPC-сервис расчёта индикаторов (TA-Lib). Чистые вычисления без БД."""

from __future__ import annotations

import logging
import os
from concurrent import futures

import grpc
from indicators import indicators_pb2_grpc

import envutil
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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    envutil.load()
    _warmup_libs()

    port = (os.environ.get("INDICATORS_PORT") or "9093").strip()
    workers = int(os.environ.get("GRPC_WORKERS", "4"))
    max_msg = int(os.environ.get("GRPC_MAX_MESSAGE_MB", "128")) * 1024 * 1024
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        options=[
            ("grpc.max_send_message_length", max_msg),
            ("grpc.max_receive_message_length", max_msg),
        ],
    )
    indicators_pb2_grpc.add_IndicatorsServicer_to_server(
        IndicatorsServicer(),
        server,
    )
    listen = f"0.0.0.0:{port}"
    server.add_insecure_port(listen)
    server.start()
    logging.info("indicators (compute-only) слушает gRPC на %s", listen)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
