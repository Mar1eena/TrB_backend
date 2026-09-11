"""PrunerConfig (oneof) -> optuna.pruners.*."""

from __future__ import annotations

import optuna

from strategysearch import search_pb2


def build(cfg: search_pb2.PrunerConfig | None) -> optuna.pruners.BasePruner:
    if cfg is None:
        return optuna.pruners.NopPruner()
    kind = cfg.WhichOneof("pruner")
    if kind is None or kind == "none":
        return optuna.pruners.NopPruner()
    if kind == "median":
        p = cfg.median
        kwargs: dict = {}
        if p.n_startup_trials:
            kwargs["n_startup_trials"] = p.n_startup_trials
        if p.n_warmup_steps:
            kwargs["n_warmup_steps"] = p.n_warmup_steps
        if p.interval_steps:
            kwargs["interval_steps"] = p.interval_steps
        if p.n_min_trials:
            kwargs["n_min_trials"] = p.n_min_trials
        return optuna.pruners.MedianPruner(**kwargs)
    if kind == "percentile":
        p = cfg.percentile
        kwargs = {"percentile": p.percentile or 25.0}
        if p.n_startup_trials:
            kwargs["n_startup_trials"] = p.n_startup_trials
        if p.n_warmup_steps:
            kwargs["n_warmup_steps"] = p.n_warmup_steps
        if p.interval_steps:
            kwargs["interval_steps"] = p.interval_steps
        if p.n_min_trials:
            kwargs["n_min_trials"] = p.n_min_trials
        return optuna.pruners.PercentilePruner(**kwargs)
    if kind == "successive_halving":
        p = cfg.successive_halving
        kwargs = {}
        if p.min_resource:
            kwargs["min_resource"] = int(p.min_resource)
        if p.reduction_factor:
            kwargs["reduction_factor"] = int(p.reduction_factor)
        if p.min_early_stopping_rate:
            kwargs["min_early_stopping_rate"] = int(p.min_early_stopping_rate)
        return optuna.pruners.SuccessiveHalvingPruner(**kwargs)
    if kind == "hyperband":
        p = cfg.hyperband
        kwargs = {}
        if p.min_resource:
            kwargs["min_resource"] = p.min_resource
        if p.max_resource:
            kwargs["max_resource"] = p.max_resource
        if p.reduction_factor:
            kwargs["reduction_factor"] = p.reduction_factor
        return optuna.pruners.HyperbandPruner(**kwargs)
    if kind == "patient":
        p = cfg.patient
        return optuna.pruners.PatientPruner(
            optuna.pruners.NopPruner(), patience=p.patience or 1, min_delta=p.min_delta or 0.0,
        )
    if kind == "threshold":
        p = cfg.threshold
        # lower/upper: 0.0 в proto3 неотличим от "не задано" (нет presence-трекинга
        # для plain double) — трактуем 0.0 как unset, как и везде в этом API.
        # ThresholdPruner требует хотя бы одну границу — если обе оставлены нулём,
        # Optuna сама кинет TypeError при построении (осознанная ошибка конфигурации).
        kwargs = {}
        if p.lower:
            kwargs["lower"] = p.lower
        if p.upper:
            kwargs["upper"] = p.upper
        if p.n_warmup_steps:
            kwargs["n_warmup_steps"] = p.n_warmup_steps
        if p.interval_steps:
            kwargs["interval_steps"] = p.interval_steps
        return optuna.pruners.ThresholdPruner(**kwargs)
    raise ValueError(f"неизвестный pruner: {kind}")
