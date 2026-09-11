"""Опциональное зеркалирование трайлов/study в ClickHouse (StudyConfig.storage.clickhouse).

Postgres (strategysearch_run/strategysearch_trial) остаётся источником истины
для GetSearchProgress/GetBestTrials — эта запись чисто аналитическая надстройка,
включается флагом ClickHouseTrialSink.enabled. Self-provisions свою
БД/таблицы при первой записи (manage о них ничего не знает и не должен).
"""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)

_ENSURED: set[tuple[str, str, str]] = set()


def _ensure_tables(ch_client: Any, database: str, trials_table: str, studies_table: str) -> None:
    key = (database, trials_table, studies_table)
    if key in _ENSURED:
        return
    ch_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    ch_client.command(f"""
        CREATE TABLE IF NOT EXISTS {database}.{trials_table} (
            search_id String, trial_id String, trial_number UInt32, state String,
            params String, values String, metrics String,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree ORDER BY (search_id, trial_number)
    """)
    ch_client.command(f"""
        CREATE TABLE IF NOT EXISTS {database}.{studies_table} (
            search_id String, status String, completed_trials UInt32, best_values String,
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY search_id
    """)
    _ENSURED.add(key)


class ClickHouseTrialSink:
    """Буферизованная запись трайлов; flush() — периодически координатором и в конце run_search."""

    def __init__(self, ch_client: Any, cfg: Any) -> None:
        self.enabled = bool(cfg is not None and cfg.enabled)
        self.ch = ch_client
        self.database = cfg.database if (cfg and cfg.database) else "TrB_strategysearch"
        self.trials_table = cfg.trials_table if (cfg and cfg.trials_table) else "trials"
        self.studies_table = cfg.studies_table if (cfg and cfg.studies_table) else "studies"
        self.batch_size = cfg.flush_batch_size if (cfg and cfg.flush_batch_size) else 200
        self._rows: list[list] = []
        if self.enabled:
            try:
                _ensure_tables(self.ch, self.database, self.trials_table, self.studies_table)
            except Exception as exc:  # noqa: BLE001
                log.warning("ClickHouse trial sink: не удалось подготовить таблицы (%s) — выключаю", exc)
                self.enabled = False

    def add_trial(self, *, search_id: str, trial_id: str, number: int, state: str,
                  params: dict, values: dict, metrics: dict) -> None:
        if not self.enabled:
            return
        self._rows.append([
            search_id, trial_id, int(number), state,
            json.dumps(params or {}), json.dumps(values or {}), json.dumps(metrics or {}),
        ])
        if len(self._rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.enabled or not self._rows:
            return
        rows, self._rows = self._rows, []
        try:
            self.ch.insert(
                f"{self.database}.{self.trials_table}", rows,
                column_names=["search_id", "trial_id", "trial_number", "state", "params", "values", "metrics"],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("ClickHouse trial sink: вставка %d строк не удалась (%s)", len(rows), exc)

    def write_study(self, *, search_id: str, status: str, completed_trials: int, best_values: dict) -> None:
        if not self.enabled:
            return
        try:
            self.ch.insert(
                f"{self.database}.{self.studies_table}",
                [[search_id, status, int(completed_trials), json.dumps(best_values or {})]],
                column_names=["search_id", "status", "completed_trials", "best_values"],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("ClickHouse trial sink: обновление study не удалось (%s)", exc)
