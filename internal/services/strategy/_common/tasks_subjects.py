"""NATS subjects/streams/consumers домена стратегий — зеркало Go
internal/services/strategy/manage/pkg/tasks/tasks.go.
"""

from __future__ import annotations

STREAM_STRATEGY_TASKS = "strategy_tasks"
SUBJ_BACKTEST_TASKS = "TrB.strategy.backtest.tasks"
SUBJ_SEARCH_TASKS = "TrB.strategy.search.tasks"
CONSUMER_BACKTEST = "strategy_backtest_engine"
CONSUMER_SEARCH = "strategy_search_engine"

# Fan-out оценки кандидатов генетического поиска.
STREAM_STRATEGY_EVAL = "strategy_eval"
SUBJ_EVAL_TASKS = "TrB.strategy.eval.tasks"
SUBJ_EVAL_RESULTS_PREFIX = "TrB.strategy.eval.results."
SUBJ_EVAL_PING = "TrB.strategy.eval.ping"  # core-NATS проба наличия воркеров
CONSUMER_EVAL_WORKER = "strategy_eval_worker"
