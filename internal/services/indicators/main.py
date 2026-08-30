#!/usr/bin/env python3
"""gRPC-сервис расчёта индикаторов (TA-Lib)."""

from __future__ import annotations

import logging
import os
from concurrent import futures

import grpc
from indicators import indicators_pb2_grpc

from clickhouse_client import get_client
from servicer import IndicatorsServicer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    port = os.environ.get("PORT", "9093")
    ch_client = None
    try:
        ch_client = get_client()
    except Exception as exc:
        logging.warning("ClickHouse недоступен, ComputeForInstrument отключён: %s", exc)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2),
        options=[
            ("grpc.max_send_message_length", 16 * 1024 * 1024),
            ("grpc.max_receive_message_length", 32 * 1024 * 1024),
        ],
    )
    indicators_pb2_grpc.add_IndicatorsServicer_to_server(IndicatorsServicer(ch_client), server)
    listen = f"[::]:{port}"
    server.add_insecure_port(listen)
    server.start()
    logging.info("indicators слушает gRPC на %s", listen)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
