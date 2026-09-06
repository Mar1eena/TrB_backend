"""Генетический алгоритм над StrategySpec: отбор турниром, кроссовер, мутация."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Iterable

from strategy import search_pb2, spec_pb2

from . import genome


@dataclass
class Individual:
    spec: spec_pb2.StrategySpec
    params: dict[str, float]
    score: float = float("-inf")


@dataclass
class GeneticSearch:
    base_spec: spec_pb2.StrategySpec
    space: list[search_pb2.ParamRange]
    structure: search_pb2.StructureSpace
    population_size: int
    rng: random.Random
    elite: int = 2
    tournament: int = 3
    mutation_rate: float = 0.3
    _generation: int = field(default=0, init=False)

    def initial_population(self) -> list[Individual]:
        pop: list[Individual] = []
        for i in range(self.population_size):
            spec = spec_pb2.StrategySpec()
            spec.CopyFrom(self.base_spec)
            params = {} if i == 0 else genome.random_params(self.space, self.rng)
            genome.apply_params(spec, params)
            if i > 0 and self.structure.mutate_structure and self.rng.random() < 0.5:
                genome.mutate_structure(spec, self.structure, self.rng)
            pop.append(Individual(spec=spec, params=params))
        return pop

    def next_generation(self, scored: list[Individual]) -> list[Individual]:
        self._generation += 1
        ranked = sorted(scored, key=lambda ind: ind.score, reverse=True)
        survivors = [ind for ind in ranked if ind.score != float("-inf")] or ranked

        nxt: list[Individual] = []
        for ind in ranked[: self.elite]:
            clone = spec_pb2.StrategySpec()
            clone.CopyFrom(ind.spec)
            nxt.append(Individual(spec=clone, params=dict(ind.params)))

        while len(nxt) < self.population_size:
            p1 = self._tournament(survivors)
            p2 = self._tournament(survivors)
            child_params = genome.crossover_params(p1.params, p2.params, self.rng)
            child_params = genome.mutate_params(child_params, self.space, self.rng, self.mutation_rate)

            child = spec_pb2.StrategySpec()
            child.CopyFrom(p1.spec if self.rng.random() < 0.5 else p2.spec)
            genome.apply_params(child, child_params)
            if self.rng.random() < self.mutation_rate:
                genome.mutate_structure(child, self.structure, self.rng)
            nxt.append(Individual(spec=child, params=child_params))
        return nxt

    def _tournament(self, pool: list[Individual]) -> Individual:
        k = min(self.tournament, len(pool))
        return max(self.rng.sample(pool, k), key=lambda ind: ind.score)

    @property
    def generation(self) -> int:
        return self._generation


def iter_generations(gs: GeneticSearch, generations: int) -> Iterable[list[Individual]]:
    """Служебный генератор для тестов: без оценки просто прогоняет структуру поколений."""
    pop = gs.initial_population()
    for _ in range(max(generations, 1)):
        yield pop
        for ind in pop:
            ind.score = gs.rng.random()
        pop = gs.next_generation(pop)
