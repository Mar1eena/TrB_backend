"""NATS subjects/streams/consumers домена strategysearch — зеркало Go
internal/services/strategysearch/manage/pkg/tasks/tasks.go.
"""

from __future__ import annotations

STREAM_STRATEGYSEARCH_TASKS = "strategysearch_tasks"
SUBJ_SEARCH_TASKS = "TrB.strategysearch.search.tasks"
CONSUMER_SEARCH = "strategysearch_search_engine"

# Fan-out оценки трайлов Optuna-поиска.
STREAM_STRATEGYSEARCH_EVAL = "strategysearch_eval"
SUBJ_TRIAL_TASKS = "TrB.strategysearch.trial.tasks"
SUBJ_TRIAL_RESULTS_PREFIX = "TrB.strategysearch.trial.results."
SUBJ_TRIAL_PING = "TrB.strategysearch.trial.ping"  # core-NATS проба наличия воркеров
CONSUMER_TRIAL_WORKER = "strategysearch_trial_worker"

# manage просит engine посчитать param importances (fANOVA) по конкретному
# search_id — core-NATS request/reply, не JetStream.
SUBJ_IMPORTANCE_REQUEST = "TrB.strategysearch.importance.request"
