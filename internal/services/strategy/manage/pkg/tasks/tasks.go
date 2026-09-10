// Package tasks — публикация задач бэктеста/поиска в NATS JetStream.
package tasks

import (
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	StreamStrategyTasks = "strategy_tasks"
	SubjBacktestTasks   = "TrB.strategy.backtest.tasks"
	SubjSearchTasks     = "TrB.strategy.search.tasks"

	ConsumerBacktest = "strategy_backtest_engine"
	ConsumerSearch   = "strategy_search_engine"

	// Fan-out оценки кандидатов генетического поиска: координатор
	// (strategy-engine) публикует EvalTask, пул stateless-воркеров разбирает.
	// Ответы (EvalResult) идут по core-NATS на SubjEvalResultsPrefix+<search_id>.
	StreamStrategyEval    = "strategy_eval"
	SubjEvalTasks         = "TrB.strategy.eval.tasks"
	SubjEvalResultsPrefix = "TrB.strategy.eval.results."
	ConsumerEvalWorker    = "strategy_eval_worker"
)

// PublishBacktest кладёт BacktestTask{run_id} в стрим (MsgId=run_id для дедупа).
func PublishBacktest(js *trb_nats.Nats, runID string) error {
	b, err := proto.Marshal(&strategypb.BacktestTask{RunId: runID})
	if err != nil {
		return err
	}
	_, err = js.Jsc.Publish(SubjBacktestTasks, b, nats.MsgId(runID))
	return err
}

// PublishSearch кладёт SearchTask{search_id} в стрим.
func PublishSearch(js *trb_nats.Nats, searchID string) error {
	b, err := proto.Marshal(&strategypb.SearchTask{SearchId: searchID})
	if err != nil {
		return err
	}
	_, err = js.Jsc.Publish(SubjSearchTasks, b, nats.MsgId(searchID))
	return err
}
