"""SamplerConfig (oneof) -> optuna.samplers.*.

Пустой числовой/bool-параметр (0/False — proto3-дефолт) => опускаем kwarg,
Optuna сама подставляет свой дефолт. Из-за этого через прото нельзя явно
затребовать False там, где дефолт Optuna — True (напр. CmaEs
warn_independent_sampling); тот же компромисс уже принят в genetic-протоколе
(StructureSpace.mutate_structure) — задокументировано, не баг.
"""

from __future__ import annotations

import optuna

from strategysearch import search_pb2


def _tpe(p: search_pb2.TpeSamplerParams) -> optuna.samplers.TPESampler:
    kwargs: dict = {}
    if p.n_startup_trials:
        kwargs["n_startup_trials"] = p.n_startup_trials
    if p.n_ei_candidates:
        kwargs["n_ei_candidates"] = p.n_ei_candidates
    if p.multivariate:
        kwargs["multivariate"] = True
    if p.group:
        kwargs["group"] = True
    if p.constant_liar:
        kwargs["constant_liar"] = True
    if p.prior_weight:
        kwargs["prior_weight"] = p.prior_weight
    if p.seed:
        kwargs["seed"] = p.seed
    return optuna.samplers.TPESampler(**kwargs)


def _cmaes(p: search_pb2.CmaEsSamplerParams) -> optuna.samplers.CmaEsSampler:
    kwargs: dict = {}
    if p.n_startup_trials:
        kwargs["n_startup_trials"] = p.n_startup_trials
    if p.sigma0:
        kwargs["sigma0"] = p.sigma0
    if p.warn_independent_sampling:
        kwargs["warn_independent_sampling"] = True
    if p.restart_strategy_ipop:
        kwargs["restart_strategy"] = "ipop"
    if p.use_separable_cma:
        kwargs["use_separable_cma"] = True
    if p.seed:
        kwargs["seed"] = p.seed
    return optuna.samplers.CmaEsSampler(**kwargs)


def _random(p: search_pb2.RandomSamplerParams) -> optuna.samplers.RandomSampler:
    return optuna.samplers.RandomSampler(seed=p.seed or None)


def _grid_space(space: list[search_pb2.ParamRange]) -> dict[str, list]:
    """optuna.samplers.GridSampler требует явный конечный search_space:
    {param_name: [значения]}. LogFloatRange и FloatRange без step с
    перебором несовместимы.
    """
    grid: dict[str, list] = {}
    for pr in space:
        kind = pr.WhichOneof("range")
        if kind == "ints":
            step = pr.ints.step or 1
            grid[pr.path] = list(range(int(pr.ints.min), int(pr.ints.max) + 1, step))
        elif kind == "floats" and pr.floats.step:
            n = int(round((pr.floats.max - pr.floats.min) / pr.floats.step))
            grid[pr.path] = [pr.floats.min + i * pr.floats.step for i in range(n + 1)]
        elif kind == "choice":
            grid[pr.path] = list(pr.choice.values)
        elif kind == "categorical":
            grid[pr.path] = list(pr.categorical.values)
        else:
            raise ValueError(f"{pr.path}: GridSampler требует ints/floats(step)/choice/categorical")
    return grid


def _nsga2(p: search_pb2.NsgaIiSamplerParams) -> optuna.samplers.NSGAIISampler:
    kwargs: dict = {}
    if p.population_size:
        kwargs["population_size"] = p.population_size
    if p.mutation_prob:
        kwargs["mutation_prob"] = p.mutation_prob
    if p.crossover_prob:
        kwargs["crossover_prob"] = p.crossover_prob
    if p.seed:
        kwargs["seed"] = p.seed
    return optuna.samplers.NSGAIISampler(**kwargs)


def _qmc(p: search_pb2.QmcSamplerParams) -> optuna.samplers.QMCSampler:
    kwargs: dict = {}
    if p.scramble:
        kwargs["scramble"] = True
    if p.seed:
        kwargs["seed"] = p.seed
    return optuna.samplers.QMCSampler(**kwargs)


def _gp(p: search_pb2.GpSamplerParams) -> optuna.samplers.GPSampler:
    kwargs: dict = {}
    if p.n_startup_trials:
        kwargs["n_startup_trials"] = p.n_startup_trials
    if p.seed:
        kwargs["seed"] = p.seed
    return optuna.samplers.GPSampler(**kwargs)


def build(cfg: search_pb2.SamplerConfig | None, space: list[search_pb2.ParamRange]) -> optuna.samplers.BaseSampler:
    if cfg is None:
        return _tpe(search_pb2.TpeSamplerParams())
    kind = cfg.WhichOneof("sampler")
    if kind is None or kind == "tpe":
        return _tpe(cfg.tpe)
    if kind == "cmaes":
        return _cmaes(cfg.cmaes)
    if kind == "random":
        return _random(cfg.random)
    if kind == "grid":
        return optuna.samplers.GridSampler(_grid_space(space))
    if kind == "nsga2":
        return _nsga2(cfg.nsga2)
    if kind == "qmc":
        return _qmc(cfg.qmc)
    if kind == "gp":
        return _gp(cfg.gp)
    raise ValueError(f"неизвестный sampler: {kind}")
