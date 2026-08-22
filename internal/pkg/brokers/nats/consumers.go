package trb_nats

import (
	"context"
	"fmt"
	"time"

	nats_admin "github.com/Mar1eena/trb_proto/gen/go/nats"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Nats) AddConsumer(context context.Context, req *nats_admin.Consumer) (*nats_admin.ConsumerInfos, error) {
	cfg := convertConsumerInfo(req.Config)

	consInfo, err := s.Jsc.AddConsumer(req.Name, cfg)
	if err != nil {
		return nil, err
	}

	return convertConsumerInfos(consInfo), nil
}

func (s *Nats) UpdateConsumer(context context.Context, req *nats_admin.Consumer) (*nats_admin.ConsumerInfos, error) {
	cfg := convertConsumerInfo(req.Config)

	consInfo, err := s.Jsc.UpdateConsumer(req.Name, cfg)
	if err != nil {
		return nil, err
	}

	return convertConsumerInfos(consInfo), nil
}

func (s *Nats) DeleteConsumer(context context.Context, req *nats_admin.ConsumerName) (*nats_admin.Response, error) {

	err := s.Jsc.DeleteConsumer(req.Stream, req.Name)
	if err != nil {
		return nil, err
	}

	return &nats_admin.Response{Response: "OK"}, nil
}

func (s *Nats) ConsumerInfo(context context.Context, req *nats_admin.ConsumerName) (*nats_admin.ConsumerInfos, error) {
	consInfo, err := s.Jsc.ConsumerInfo(req.Stream, req.Name)
	if err != nil {
		return nil, err
	}

	return convertConsumerInfos(consInfo), nil
}

func (s *Nats) ConsumersInfo(ctx context.Context, streamName *nats_admin.StreamName) (*nats_admin.ConsumerList, error) {
	return s.Consumers(ctx, streamName)
}

func (s *Nats) Consumers(ctx context.Context, streamName *nats_admin.StreamName) (*nats_admin.ConsumerList, error) {
	if streamName == nil || streamName.Name == "" {
		return nil, fmt.Errorf("имя стрима обязательно")
	}
	ch := s.Jsc.Consumers(streamName.Name, nats.Context(ctx))
	if ch == nil {
		return nil, fmt.Errorf("не удалось получить список консьюмеров")
	}
	items := make([]*nats_admin.ConsumerInfos, 0)
	for info := range ch {
		if info == nil {
			continue
		}
		items = append(items, convertConsumerInfos(info))
	}
	return &nats_admin.ConsumerList{Items: items}, nil
}

func (s *Nats) ConsumerNames(ctx context.Context, streamName *nats_admin.StreamName) (*nats_admin.ConsumerNameList, error) {
	if streamName == nil || streamName.Name == "" {
		return nil, fmt.Errorf("имя стрима обязательно")
	}
	ch := s.Jsc.ConsumerNames(streamName.Name, nats.Context(ctx))
	if ch == nil {
		return nil, fmt.Errorf("не удалось получить имена консьюмеров")
	}
	names := make([]string, 0)
	for name := range ch {
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return &nats_admin.ConsumerNameList{Names: names}, nil
}

func convertConsumerInfo(c *nats_admin.ConsumerConfig) *nats.ConsumerConfig {
	if c == nil {
		return nil
	}

	backOff := make([]time.Duration, len(c.Backoff))
	for i, n := range c.Backoff {
		backOff[i] = time.Duration(n)
	}

	consCfg := nats.ConsumerConfig{
		Durable:            c.Durable,
		Name:               c.Name,
		Description:        c.Description,
		DeliverPolicy:      nats.DeliverPolicy(c.DeliverPolicy),
		AckPolicy:          nats.AckPolicy(c.AckPolicy),
		AckWait:            time.Duration(c.AckWait),
		MaxDeliver:         int(c.MaxDeliver),
		BackOff:            backOff,
		FilterSubject:      c.FilterSubject,
		FilterSubjects:     c.FilterSubjects,
		ReplayPolicy:       nats.ReplayPolicy(c.ReplayPolicy),
		RateLimit:          c.RateLimitBps,
		SampleFrequency:    c.SampleFreq,
		MaxWaiting:         int(c.MaxWaiting),
		MaxAckPending:      int(c.MaxAckPending),
		FlowControl:        c.FlowControl,
		Heartbeat:          time.Duration(c.IdleHeartbeat),
		HeadersOnly:        c.HeadersOnly,
		MaxRequestBatch:    int(c.MaxRequestBatch),
		MaxRequestExpires:  time.Duration(c.MaxRequestExpires),
		MaxRequestMaxBytes: int(c.MaxRequestMaxBytes),
		DeliverSubject:     c.DeliverSubject,
		DeliverGroup:       c.DeliverGroup,
		InactiveThreshold:  time.Duration(c.InactiveThreshold),
		Replicas:           int(c.Replicas),
		MemoryStorage:      c.MemoryStorage,
		Metadata:           c.Metadata,
	}

	if c.OptStartTime != nil {
		optStartTime := c.OptStartTime.AsTime()
		consCfg.OptStartTime = &optStartTime
	}
	consCfg.OptStartSeq = c.OptStartSeq

	return &consCfg
}

func convertConsumerConfig(c nats.ConsumerConfig) *nats_admin.ConsumerConfig {
	backOff := make([]int64, len(c.BackOff))
	for i, d := range c.BackOff {
		backOff[i] = int64(d)
	}

	cfg := &nats_admin.ConsumerConfig{
		Durable:            c.Durable,
		Name:               c.Name,
		Description:        c.Description,
		DeliverPolicy:      int32(c.DeliverPolicy),
		OptStartSeq:        c.OptStartSeq,
		AckPolicy:          int32(c.AckPolicy),
		AckWait:            int64(c.AckWait),
		MaxDeliver:         int32(c.MaxDeliver),
		Backoff:            backOff,
		FilterSubject:      c.FilterSubject,
		FilterSubjects:     c.FilterSubjects,
		ReplayPolicy:       int32(c.ReplayPolicy),
		RateLimitBps:       c.RateLimit,
		SampleFreq:         c.SampleFrequency,
		MaxWaiting:         int32(c.MaxWaiting),
		MaxAckPending:      int32(c.MaxAckPending),
		FlowControl:        c.FlowControl,
		IdleHeartbeat:      int64(c.Heartbeat),
		HeadersOnly:        c.HeadersOnly,
		MaxRequestBatch:    int32(c.MaxRequestBatch),
		MaxRequestExpires:  int64(c.MaxRequestExpires),
		MaxRequestMaxBytes: int32(c.MaxRequestMaxBytes),
		DeliverSubject:     c.DeliverSubject,
		DeliverGroup:       c.DeliverGroup,
		InactiveThreshold:  int64(c.InactiveThreshold),
		Replicas:           int32(c.Replicas),
		MemoryStorage:      c.MemoryStorage,
		Metadata:           c.Metadata,
	}
	if c.OptStartTime != nil {
		cfg.OptStartTime = timestamppb.New(*c.OptStartTime)
	}
	return cfg
}

func convertConsumerInfos(c *nats.ConsumerInfo) *nats_admin.ConsumerInfos {
	if c == nil {
		return nil
	}

	return &nats_admin.ConsumerInfos{
		StreamName:     c.Stream,
		Name:           c.Name,
		Created:        timestamppb.New(c.Created),
		Delivered:      convertSequenceInfo(c.Delivered),
		AckFloor:       convertSequenceInfo(c.AckFloor),
		NumAckPendin:   int32(c.NumAckPending),
		NumRedelivered: int32(c.NumRedelivered),
		NumWaiting:     int32(c.NumWaiting),
		NumPending:     c.NumPending,
		Cluster:        convertClusterInfo(c.Cluster),
		PushBound:      c.PushBound,
		Config:         convertConsumerConfig(c.Config),
	}
}

func convertSequenceInfo(s nats.SequenceInfo) *nats_admin.SequenceInfo {
	LastActive := timestamppb.New(time.Time{})
	if s.Last != nil {
		LastActive = timestamppb.New(*s.Last)
	}

	return &nats_admin.SequenceInfo{
		ConsumerSeq: s.Consumer,
		StreamSeq:   s.Stream,
		LastActive:  LastActive,
	}
}
