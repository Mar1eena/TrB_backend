#!/usr/bin/env python3
"""Пример gRPC-клиента."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb
from indicators import indicators_pb2_grpc


def _ts(dt: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(dt.replace(tzinfo=None))
    return t


def main() -> None:
    addr = sys.argv[1] if len(sys.argv) > 1 else "localhost:9093"
    stub = indicators_pb2_grpc.IndicatorsStub(grpc.insecure_channel(addr))

    supported = stub.ListSupported(pb.ListSupportedRequest())
    print("supported:", [i.name for i in supported.indicators])

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    candles = [
        pb.Candle(
            time=_ts(start + timedelta(hours=i)),
            open=100 + i * 0.3,
            high=101 + i * 0.3,
            low=99 + i * 0.3,
            close=100 + i * 0.3,
            volume=1000,
        )
        for i in range(30)
    ]

    resp = stub.Compute(pb.ComputeRequest(type=pb.INDICATOR_TYPE_RSI, candles=candles))
    print(f"RSI points: {len(resp.points)}")
    if resp.points:
        last = resp.points[-1]
        print(f"last RSI @ {last.time.ToDatetime()}: {last.values.get('value'):.2f}")


if __name__ == "__main__":
    main()
