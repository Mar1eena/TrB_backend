import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UsersMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    USERS_METHOD_UNSPECIFIED: _ClassVar[UsersMethod]
    GET_ACCOUNTS: _ClassVar[UsersMethod]
    GET_MARGIN_ATTRIBUTES: _ClassVar[UsersMethod]
    GET_USER_TARIFF: _ClassVar[UsersMethod]
    GET_INFO: _ClassVar[UsersMethod]
    GET_BANK_ACCOUNTS: _ClassVar[UsersMethod]
    GET_ACCOUNT_VALUES: _ClassVar[UsersMethod]

class UsersStatusKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    USERS_STATUS_UNSPECIFIED: _ClassVar[UsersStatusKind]
    ACCEPTED: _ClassVar[UsersStatusKind]
    DONE: _ClassVar[UsersStatusKind]
    READY: _ClassVar[UsersStatusKind]
    FAILED: _ClassVar[UsersStatusKind]
USERS_METHOD_UNSPECIFIED: UsersMethod
GET_ACCOUNTS: UsersMethod
GET_MARGIN_ATTRIBUTES: UsersMethod
GET_USER_TARIFF: UsersMethod
GET_INFO: UsersMethod
GET_BANK_ACCOUNTS: UsersMethod
GET_ACCOUNT_VALUES: UsersMethod
USERS_STATUS_UNSPECIFIED: UsersStatusKind
ACCEPTED: UsersStatusKind
DONE: UsersStatusKind
READY: UsersStatusKind
FAILED: UsersStatusKind

class IndicatorTask(_message.Message):
    __slots__ = ("uid", "interval", "time", "open", "high", "low", "close", "volume", "volume_buy", "volume_sell", "candle_source", "is_complete")
    UID_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    VOLUME_BUY_FIELD_NUMBER: _ClassVar[int]
    VOLUME_SELL_FIELD_NUMBER: _ClassVar[int]
    CANDLE_SOURCE_FIELD_NUMBER: _ClassVar[int]
    IS_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    uid: str
    interval: int
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    volume_buy: int
    volume_sell: int
    candle_source: int
    is_complete: bool
    def __init__(self, uid: _Optional[str] = ..., interval: _Optional[int] = ..., time: _Optional[int] = ..., open: _Optional[float] = ..., high: _Optional[float] = ..., low: _Optional[float] = ..., close: _Optional[float] = ..., volume: _Optional[int] = ..., volume_buy: _Optional[int] = ..., volume_sell: _Optional[int] = ..., candle_source: _Optional[int] = ..., is_complete: _Optional[bool] = ...) -> None: ...

class HistoricCandleLoadTask(_message.Message):
    __slots__ = ("uid", "interval")
    UID_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    uid: _containers.RepeatedScalarFieldContainer[str]
    interval: int
    def __init__(self, uid: _Optional[_Iterable[str]] = ..., interval: _Optional[int] = ...) -> None: ...

class Trade(_message.Message):
    __slots__ = ("uid", "time", "figi", "direction", "price", "quantity", "trade_source", "ticker", "class_code")
    UID_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    FIGI_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    TRADE_SOURCE_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    CLASS_CODE_FIELD_NUMBER: _ClassVar[int]
    uid: str
    time: _timestamp_pb2.Timestamp
    figi: str
    direction: int
    price: float
    quantity: int
    trade_source: int
    ticker: str
    class_code: str
    def __init__(self, uid: _Optional[str] = ..., time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., figi: _Optional[str] = ..., direction: _Optional[int] = ..., price: _Optional[float] = ..., quantity: _Optional[int] = ..., trade_source: _Optional[int] = ..., ticker: _Optional[str] = ..., class_code: _Optional[str] = ...) -> None: ...

class UsersTask(_message.Message):
    __slots__ = ("task_id", "method", "account_status", "account_id", "accounts", "values")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_STATUS_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNTS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    method: UsersMethod
    account_status: int
    account_id: str
    accounts: _containers.RepeatedScalarFieldContainer[str]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, task_id: _Optional[str] = ..., method: _Optional[_Union[UsersMethod, str]] = ..., account_status: _Optional[int] = ..., account_id: _Optional[str] = ..., accounts: _Optional[_Iterable[str]] = ..., values: _Optional[_Iterable[int]] = ...) -> None: ...

class UsersStatus(_message.Message):
    __slots__ = ("task_id", "method", "kind", "error")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    method: UsersMethod
    kind: UsersStatusKind
    error: str
    def __init__(self, task_id: _Optional[str] = ..., method: _Optional[_Union[UsersMethod, str]] = ..., kind: _Optional[_Union[UsersStatusKind, str]] = ..., error: _Optional[str] = ...) -> None: ...

class UsersData(_message.Message):
    __slots__ = ("task_id", "method", "payload")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    method: UsersMethod
    payload: bytes
    def __init__(self, task_id: _Optional[str] = ..., method: _Optional[_Union[UsersMethod, str]] = ..., payload: _Optional[bytes] = ...) -> None: ...
