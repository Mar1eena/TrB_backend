// Package tasks — публикация задач Optuna-поиска в NATS JetStream.
package tasks

import (
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	StreamStrategySearchTasks = "strategysearch_tasks"
	SubjSearchTasks           = "TrB.strategysearch.search.tasks"
	ConsumerSearch            = "strategysearch_search_engine"

	// Fan-out оценки трайлов Optuna-поиска: координатор (strategysearch-engine)
	// публикует TrialTask, пул stateless-воркеров разбирает. Ответы (TrialResult)
	// идут по core-NATS на SubjTrialResultsPrefix+<search_id>.
	StreamStrategySearchEval = "strategysearch_eval"
	SubjTrialTasks           = "TrB.strategysearch.trial.tasks"
	SubjTrialResultsPrefix   = "TrB.strategysearch.trial.results."
	SubjTrialPing            = "TrB.strategysearch.trial.ping"
	ConsumerTrialWorker      = "strategysearch_trial_worker"
)

// PublishSearch кладёт SearchTask{search_id} в стрим (MsgId=search_id для дедупа).
func PublishSearch(js *trb_nats.Nats, searchID string) error {
	b, err := proto.Marshal(&strategysearchpb.SearchTask{SearchId: searchID})
	if err != nil {
		return err
	}
	_, err = js.Jsc.Publish(SubjSearchTasks, b, nats.MsgId(searchID))
	return err
}
