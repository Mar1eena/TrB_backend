package investgo

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OrderStateStream struct {
	stream       pb.OrdersStreamService_OrderStateStreamClient
	ordersClient *OrdersStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	states chan *pb.OrderStateStreamResponse_OrderState
}

// OrderState - Метод возвращает канал для чтения информации о состоянии поручений
func (s *OrderStateStream) OrderState() <-chan *pb.OrderStateStreamResponse_OrderState {
	return s.states
}

// Listen - метод начинает слушать стрим и отправлять информацию в канал, для получения канала: OrderState()
func (s *OrderStateStream) Listen() error {
	defer s.shutdown()
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			resp, err := s.stream.Recv()
			if err != nil {
				switch {
				case status.Code(err) == codes.Canceled:
					s.ordersClient.logger.Infof("остановка прослушивания стрима состояний заявок")
					return nil
				default:
					return err
				}
			} else {
				switch resp.GetPayload().(type) {
				case *pb.OrderStateStreamResponse_OrderState_:
					s.states <- resp.GetOrderState()
				default:
					s.ordersClient.logger.Infof("информация из стрима состояний заявок: %v", resp.String())
				}
			}
		}
	}
}

func (s *OrderStateStream) restart(_ context.Context, attempt uint, err error) {
	s.ordersClient.logger.Infof("попытка перезапуска стрима состояний заявок, ошибка = %v, попытка = %v", err.Error(), attempt)
}

func (s *OrderStateStream) shutdown() {
	s.ordersClient.logger.Infof("закрытие стрима состояний заявок")
	close(s.states)
}

// Stop - Завершение работы стрима
func (s *OrderStateStream) Stop() {
	s.cancel()
}
