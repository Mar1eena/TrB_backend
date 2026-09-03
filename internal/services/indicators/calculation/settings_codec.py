"""Protobuf Settings: байты колонки `request` ↔ сообщение indicators.Settings."""

from __future__ import annotations

from indicators import indicators_pb2 as pb


class SettingsCodecError(Exception):
    """Не удалось разобрать protobuf Settings из колонки request."""


def decode_request(raw: bytes | str | bytearray) -> pb.Settings:
    """Разбирает protobuf-строку `request` в Settings (indicators.proto)."""
    data = _to_bytes(raw)
    if not data:
        raise SettingsCodecError("request пуст")
    msg = pb.Settings()
    try:
        msg.ParseFromString(data)
    except Exception as exc:
        raise SettingsCodecError(f"не удалось разобрать request как Settings: {exc}") from exc
    if not msg.HasField("settings"):
        raise SettingsCodecError("в request нет поля settings")
    if msg.settings.WhichOneof("indicator_type") is None:
        raise SettingsCodecError("в request.settings не выбран indicator_type")
    return msg


def encode_request(settings: pb.Settings) -> bytes:
    return settings.SerializeToString()


def indicator_type_name(settings: pb.Settings) -> str:
    if not settings.HasField("settings"):
        return ""
    return settings.settings.WhichOneof("indicator_type") or ""


def _to_bytes(raw: bytes | str | bytearray) -> bytes:
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    text = raw.strip()
    if not text:
        return b""
    if _looks_like_hex(text):
        try:
            return bytes.fromhex(text)
        except ValueError as exc:
            raise SettingsCodecError(f"некорректный hex(request): {exc}") from exc
    return text.encode("latin-1")


def _looks_like_hex(text: str) -> bool:
    if len(text) < 2 or len(text) % 2 != 0:
        return False
    return all(ch in "0123456789abcdefABCDEF" for ch in text)
