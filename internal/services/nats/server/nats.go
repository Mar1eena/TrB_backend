package server

import (
	"context"

	natspkg "github.com/Mar1eena/TrB_V3/internal/services/nats/pkg"
	nats_admin "github.com/Mar1eena/trb_proto/gen/go/nats"
)

// STREAMS
// STREAMS

func (s *natsService) AddStream(context context.Context, req *nats_admin.StreamConfig) (*nats_admin.StreamInfos, error) {
	r, e := s.js.AddStream(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Msg("не удалось добавить стрим")
	} else {
		s.js.L.Info().Str("stream", req.Name).Msg("стрим успешно добавлен")
	}
	return r, e
}

func (s *natsService) UpdateStream(context context.Context, req *nats_admin.StreamConfig) (*nats_admin.StreamInfos, error) {
	r, e := s.js.UpdateStream(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Msg("не удалось обновить стрим")
	} else {
		s.js.L.Info().Str("stream", req.Name).Msg("стрим успешно обновлён")
	}
	return r, e
}

func (s *natsService) DeleteStream(context context.Context, req *nats_admin.StreamName) (*nats_admin.Response, error) {
	r, e := s.js.DeleteStream(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Msg("не удалось удалить стрим")
	} else {
		s.js.L.Info().Str("stream", req.Name).Msg("стрим успешно удалён")
	}
	return r, e
}

func (s *natsService) StreamInfo(context context.Context, req *nats_admin.StreamName) (*nats_admin.StreamInfos, error) {
	r, e := s.js.StreamInfo(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Msg("не удалось получить информацию о стриме")
	} else {
		s.js.L.Info().Str("stream", req.Name).Msg("информация о стриме получена")
	}
	return r, e
}

func (s *natsService) PurgeStream(context context.Context, req *nats_admin.StreamName) (*nats_admin.Response, error) {
	r, e := s.js.PurgeStream(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Msg("не удалось очистить стрим")
	} else {
		s.js.L.Info().Str("stream", req.Name).Msg("стрим успешно очищен")
	}
	return r, e
}

func (s *natsService) StreamsInfo(ctx context.Context, req *nats_admin.JsOpts) (*nats_admin.StreamList, error) {
	r, e := s.js.StreamsInfo(ctx, req)
	if e != nil {
		s.js.L.Err(e).Msg("не удалось получить информацию о стримах")
	} else {
		s.js.L.Info().Int("count", len(r.GetItems())).Msg("информация о стримах получена")
	}
	return r, e
}

func (s *natsService) Streams(ctx context.Context, req *nats_admin.JsOpts) (*nats_admin.StreamList, error) {
	r, e := s.js.Streams(ctx, req)
	if e != nil {
		s.js.L.Err(e).Msg("не удалось получить список стримов")
	} else {
		s.js.L.Info().Int("count", len(r.GetItems())).Msg("список стримов получен")
	}
	return r, e
}

func (s *natsService) StreamNames(ctx context.Context, req *nats_admin.JsOpts) (*nats_admin.StreamNameList, error) {
	r, e := s.js.StreamNames(ctx, req)
	if e != nil {
		s.js.L.Err(e).Msg("не удалось получить имена стримов")
	} else {
		s.js.L.Info().Int("count", len(r.GetNames())).Msg("имена стримов получены")
	}
	return r, e
}

// MESSAGES

func (s *natsService) GetMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.RawStreamMsg, error) {
	r, e := s.js.GetMsg(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Uint64("seq", req.Seq).Msg("не удалось получить сообщение")
	} else {
		s.js.L.Info().Str("stream", req.Name).Uint64("seq", req.Seq).Msg("сообщение получено")
	}
	return r, e
}

func (s *natsService) GetMsgs(ctx context.Context, req *nats_admin.MsgRange) (*nats_admin.MsgList, error) {
	r, e := s.js.GetMsgs(ctx, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Uint64("from", req.GetFromSeq()).Uint64("to", req.GetToSeq()).Msg("не удалось получить сообщения")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Uint64("from", req.GetFromSeq()).Uint64("to", req.GetToSeq()).Int("count", len(r.GetItems())).Msg("сообщения получены")
	}
	return r, e
}

func (s *natsService) GetLastMsg(context context.Context, req *nats_admin.LastMsg) (*nats_admin.RawStreamMsg, error) {
	r, e := s.js.GetLastMsg(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Str("subject", req.Subject).Msg("не удалось получить последнее сообщение")
	} else {
		s.js.L.Info().Str("stream", req.Name).Str("subject", req.Subject).Msg("последнее сообщение получено")
	}
	return r, e
}

func (s *natsService) DeleteMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.Response, error) {
	r, e := s.js.DeleteMsg(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Uint64("seq", req.Seq).Msg("не удалось удалить сообщение")
	} else {
		s.js.L.Info().Str("stream", req.Name).Uint64("seq", req.Seq).Msg("сообщение удалено")
	}
	return r, e
}

func (s *natsService) SecureDeleteMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.Response, error) {
	r, e := s.js.SecureDeleteMsg(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.Name).Uint64("seq", req.Seq).Msg("не удалось безопасно удалить сообщение")
	} else {
		s.js.L.Info().Str("stream", req.Name).Uint64("seq", req.Seq).Msg("сообщение безопасно удалено")
	}
	return r, e
}

// CONSUMERS

func (s *natsService) AddConsumer(context context.Context, req *nats_admin.Consumer) (*nats_admin.ConsumerInfos, error) {
	r, e := s.js.AddConsumer(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Str("consumer", natspkg.ConsumerName(req)).Msg("не удалось добавить консьюмер")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Str("consumer", natspkg.ConsumerName(req)).Msg("консьюмер успешно добавлен")
	}
	return r, e
}

func (s *natsService) UpdateConsumer(context context.Context, req *nats_admin.Consumer) (*nats_admin.ConsumerInfos, error) {
	r, e := s.js.UpdateConsumer(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Str("consumer", natspkg.ConsumerName(req)).Msg("не удалось обновить консьюмер")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Str("consumer", natspkg.ConsumerName(req)).Msg("консьюмер успешно обновлён")
	}
	return r, e
}

func (s *natsService) DeleteConsumer(context context.Context, req *nats_admin.ConsumerName) (*nats_admin.Response, error) {
	r, e := s.js.DeleteConsumer(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetStream()).Str("consumer", req.GetName()).Msg("не удалось удалить консьюмер")
	} else {
		s.js.L.Info().Str("stream", req.GetStream()).Str("consumer", req.GetName()).Msg("консьюмер успешно удалён")
	}
	return r, e
}

func (s *natsService) ConsumerInfo(context context.Context, req *nats_admin.ConsumerName) (*nats_admin.ConsumerInfos, error) {
	r, e := s.js.ConsumerInfo(context, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetStream()).Str("consumer", req.GetName()).Msg("не удалось получить информацию о консьюмере")
	} else {
		s.js.L.Info().Str("stream", req.GetStream()).Str("consumer", req.GetName()).Msg("информация о консьюмере получена")
	}
	return r, e
}

func (s *natsService) ConsumersInfo(ctx context.Context, req *nats_admin.StreamName) (*nats_admin.ConsumerList, error) {
	r, e := s.js.ConsumersInfo(ctx, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Msg("не удалось получить информацию о консьюмерах")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Int("count", len(r.GetItems())).Msg("информация о консьюмерах получена")
	}
	return r, e
}

func (s *natsService) Consumers(ctx context.Context, req *nats_admin.StreamName) (*nats_admin.ConsumerList, error) {
	r, e := s.js.Consumers(ctx, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Msg("не удалось получить список консьюмеров")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Int("count", len(r.GetItems())).Msg("список консьюмеров получен")
	}
	return r, e
}

func (s *natsService) ConsumerNames(ctx context.Context, req *nats_admin.StreamName) (*nats_admin.ConsumerNameList, error) {
	r, e := s.js.ConsumerNames(ctx, req)
	if e != nil {
		s.js.L.Err(e).Str("stream", req.GetName()).Msg("не удалось получить имена консьюмеров")
	} else {
		s.js.L.Info().Str("stream", req.GetName()).Int("count", len(r.GetNames())).Msg("имена консьюмеров получены")
	}
	return r, e
}

// Account
func (s *natsService) AccountInfo(context context.Context, req *nats_admin.JsOpts) (*nats_admin.AccountInfos, error) {
	r, e := s.js.AccountInfo(context, req)
	if e != nil {
		s.js.L.Err(e).Msg("не удалось получить информацию об аккаунте")
	} else {
		s.js.L.Info().Msg("информация об аккаунте получена")
	}
	return r, e
}

func (s *natsService) StreamNameBySubject(ctx context.Context, req *nats_admin.SubjectQuery) (*nats_admin.StreamName, error) {
	r, e := s.js.StreamNameBySubject(ctx, req)
	if e != nil {
		s.js.L.Err(e).Str("subject", req.GetSubject()).Msg("не удалось получить имя стрима по subject")
	} else {
		s.js.L.Info().Str("subject", req.GetSubject()).Str("stream", r.GetName()).Msg("имя стрима по subject получено")
	}
	return r, e
}

func (s *natsService) Publish(context context.Context, req *nats_admin.PublishRequest) (*nats_admin.PublishResponse, error) {
	_, err := s.js.Jsc.Publish(req.Subject, []byte(req.Data))
	if err != nil {
		s.js.L.Err(err).Str("subject", req.GetSubject()).Msg("не удалось опубликовать сообщение")
		return nil, err
	}
	return &nats_admin.PublishResponse{Response: req.Data}, nil
}
