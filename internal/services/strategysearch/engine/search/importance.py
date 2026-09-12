"""Post-hoc важность параметров (optuna fANOVA) для завершённого/идущего поиска.

Реконструирует optuna.Study из RDB-хранилища конкретного search_id
(StudyConfig.storage.storage_url, см. runner.py:create_study) и вызывает
optuna.importance.get_param_importances. Ничего не пишет обратно в Postgres —
чистый read-only расчёт по требованию, инициируется через NATS
(TrB.strategysearch.importance.request, см. consumers.py:consume_importance).

Без storage_url (поиск запущен без RDB-хранилища, например старый search до
включения дефолта в manage) Study недоступна — возвращаем пустой результат,
а не ошибку: фронт просто не покажет график важности параметров.
"""

from __future__ import annotations

import logging

import optuna

import pg
from specmod import load as specload

log = logging.getLogger(__name__)

optuna.logging.set_verbosity(optuna.logging.WARNING)


def compute_param_importances(search_id: str, metric: str = "") -> dict[str, float]:
    row = pg.fetch_search_run(search_id)
    if row is None:
        return {}
    try:
        study_cfg = specload.parse_study(row["study"])
    except specload.SpecError:
        return {}

    storage_url = study_cfg.storage.storage_url
    if not storage_url:
        return {}

    study_name = study_cfg.study_name or search_id
    try:
        study = optuna.load_study(study_name=study_name, storage=storage_url)
    except KeyError:
        # study ещё не создан движком (поиск в очереди) либо storage пуст.
        return {}

    target = None
    metrics = [m.metric for m in study_cfg.objective.metrics]
    if len(metrics) > 1:
        idx = metrics.index(metric) if metric in metrics else 0
        target = lambda t, i=idx: t.values[i]  # noqa: E731

    try:
        importances = optuna.importance.get_param_importances(study, target=target)
    except (ValueError, RuntimeError) as exc:
        # Недостаточно завершённых трайлов для fANOVA и т.п. — пустой
        # результат, это не ошибка запроса.
        log.info("search %s: importances недоступны: %s", search_id, exc)
        return {}
    return {path: float(v) for path, v in importances.items()}
