package investgo

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OperationsStream struct {
	stream           pb.OperationsStreamService_OperationsStreamClient
	operationsClient *OperationsStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	operations chan *pb.OperationData
}

// Operations - Метод возвращает канал для чтения обновлений операций
func (p *OperationsStream) Operations() <-chan *pb.OperationData {
	return p.operations
}

// Listen - метод начинает слушать стрим и отправлять информацию в канал, для получения канала: Operations()
func (p *OperationsStream) Listen() error {
	defer p.shutdown()
	for {
		select {
		case <-p.ctx.Done():
			return nil
		default:
			resp, err := p.stream.Recv()
			if err != nil {
				switch {
				case status.Code(err) == codes.Canceled:
					p.operationsClient.logger.Infof("остановка прослушивания операций")
					return nil
				default:
					return err
				}
			} else {
				switch resp.GetPayload().(type) {
				case *pb.OperationsStreamResponse_Operation:
					p.operations <- resp.GetOperation()
				default:
					p.operationsClient.logger.Infof("информация из стрима операций: %v", resp.String())
				}
			}
		}
	}
}

func (p *OperationsStream) restart(_ context.Context, attempt uint, err error) {
	p.operationsClient.logger.Infof("попытка перезапуска стрима операций, ошибка = %v, попытка = %v", err.Error(), attempt)
}

func (p *OperationsStream) shutdown() {
	p.operationsClient.logger.Infof("закрытие стрима операций")
	close(p.operations)
}

// Stop - Завершение работы стрима
func (p *OperationsStream) Stop() {
	p.cancel()
}
