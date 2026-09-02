"""gRPC servicer для сервиса Indicators (только вычисления, без БД)."""

from __future__ import annotations

import grpc
from indicators import indicators_pb2 as pb
from indicators import indicators_pb2_grpc

from calc import ComputeError, compute
from registry import REGISTRY


class IndicatorsServicer(indicators_pb2_grpc.IndicatorsServicer):
    """Stateless servicer: расчёт технических индикаторов по переданным свечам."""

    def Compute(self, request: pb.ComputeRequest, context: grpc.ServicerContext) -> pb.ComputeResponse:
        try:
            return compute(request)
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

    def ComputeForInstrument(
        self,
        request: pb.ComputeForInstrumentRequest,
        context: grpc.ServicerContext,
    ) -> pb.ComputeResponse:
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("ComputeForInstrument обрабатывается сервисом ClickHouse")
        return pb.ComputeResponse()

    def ListIndicatorValues(
        self,
        request: pb.ListIndicatorValuesRequest,
        context: grpc.ServicerContext,
    ) -> pb.ListIndicatorValuesResponse:
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("ListIndicatorValues обрабатывается сервисом ClickHouse")
        return pb.ListIndicatorValuesResponse()
