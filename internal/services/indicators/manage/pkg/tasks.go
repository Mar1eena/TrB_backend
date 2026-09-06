package pkg

import (
	"encoding/json"
	"fmt"

	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
)

const (
	// IndicatorTasksSubject — очередь задач расчёта (JSONEachRow с param_hash).
	IndicatorTasksSubject = "TrB.indicators.tasks"
	// IndicatorStatusPrefix — статус расчёта по хэшу: <prefix><param_hash>.
	// Сервис calculation публикует сюда {"status":"done"|"error", ...},
	// движок стратегий подписывается по хэшу вместо опроса ClickHouse.
	IndicatorStatusPrefix = "TrB.indicators.status."
)

// PublishCalcTask ставит задачу расчёта индикатора по param_hash в JetStream-стрим.
// Без MsgId: дедуп не нужен — calculation сам проверяет end_after_max_time и
// либо продлевает ряд вперёд, либо делает noop.
func PublishCalcTask(js *trb_nats.Nats, paramHash uint64) error {
	if js == nil || js.Jsc == nil {
		return fmt.Errorf("nats не инициализирован")
	}
	line, err := json.Marshal(map[string]uint64{"param_hash": paramHash})
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = js.Jsc.Publish(IndicatorTasksSubject, line)
	return err
}
