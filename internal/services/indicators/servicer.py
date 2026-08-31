"""gRPC servicer для сервиса Indicators."""

from __future__ import annotations

from typing import TYPE_CHECKING

import grpc
from indicators import indicators_pb2 as pb
from indicators import indicators_pb2_grpc

from calc import ComputeError, compute
from clickhouse_client import get_client
from instrument import compute_for_instrument
from loader import list_indicator_values
from registry import REGISTRY

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client


class IndicatorsServicer(indicators_pb2_grpc.IndicatorsServicer):
    def __init__(self, ch_enabled: bool = False, client: Client | None = None) -> None:
        self._ch_enabled = ch_enabled
        self._client = client

    def _get_ch_client(self) -> Client:
        if self._client is not None:
            return self._client
        return get_client()

    def Compute(self, request: pb.ComputeRequest, context: grpc.ServicerContext) -> pb.ComputeResponse:
        try:
            return compute(request)
        except ComputeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb.ComputeResponse()

    def ComputeForInstrument(
        self,
        request: pb.ComputeForInstrumentRequest,
        context: grpc.ServicerContext,
    ) -> pb.ComputeResponse:
        if not self._ch_enabled:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("ClickHouse не настроен (CLICKHOUSE_URL)")
            return pb.ComputeResponse()
        try:
            return compute_for_instrument(self._get_ch_client(), request)
        except ComputeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb.ComputeResponse()
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return pb.ComputeResponse()

    def ListSupported(
        self,
        request: pb.ListSupportedRequest,
        context: grpc.ServicerContext,
    ) -> pb.ListSupportedResponse:
        items = []
        for spec in REGISTRY.values():
            info = pb.IndicatorInfo(
                type=spec.type,
                name=spec.name,
                min_bars=spec.min_bars,
            )
            info.default_params.update({k: float(v) for k, v in spec.default_params.items()})
            items.append(info)
        items.sort(key=lambda x: x.type)
        return pb.ListSupportedResponse(indicators=items)

    def ListIndicatorValues(
        self,
        request: pb.ListIndicatorValuesRequest,
        context: grpc.ServicerContext,
    ) -> pb.ListIndicatorValuesResponse:
        if not self._ch_enabled:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("ClickHouse не настроен (CLICKHOUSE_URL)")
            return pb.ListIndicatorValuesResponse()
        try:
            return list_indicator_values(self._get_ch_client(), request)
        except ComputeError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb.ListIndicatorValuesResponse()
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return pb.ListIndicatorValuesResponse()
