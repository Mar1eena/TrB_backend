package trb_nats

import (
	"context"
	"fmt"

	nats_admin "github.com/Mar1eena/trb_proto/gen/go/nats"
	"github.com/nats-io/nats.go"
)

func (s *Nats) AccountInfo(context context.Context, req *nats_admin.JsOpts) (*nats_admin.AccountInfos, error) {
	accInfo, err := s.Jsc.AccountInfo()
	if err != nil {
		return nil, err
	}

	tiers := make(map[string]*nats_admin.Tier)
	for k, v := range accInfo.Tiers {
		tiers[k] = convertTier(&v)
	}

	return &nats_admin.AccountInfos{
		Tier:   convertTier(&accInfo.Tier),
		Domain: accInfo.Domain,
		Api: &nats_admin.APIStats{
			Total:  accInfo.API.Total,
			Errors: accInfo.API.Errors,
		},
		Tiers: tiers,
	}, nil
}

func (s *Nats) StreamNameBySubject(ctx context.Context, req *nats_admin.SubjectQuery) (*nats_admin.StreamName, error) {
	if req == nil || req.Subject == "" {
		return nil, fmt.Errorf("subject обязателен")
	}
	response, err := s.Jsc.StreamNameBySubject(req.Subject, nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	return &nats_admin.StreamName{Name: response}, nil
}

func convertTier(t *nats.Tier) *nats_admin.Tier {
	return &nats_admin.Tier{
		Memory:          t.Memory,
		Storage:         t.Store,
		ReservedMemory:  t.ReservedMemory,
		ReservedStorage: t.ReservedStore,
		Stream:          int32(t.Streams),
		Consumers:       int32(t.Consumers),
		Limits: &nats_admin.AccountLimits{
			MaxMemory:             t.Limits.MaxMemory,
			MaxStorage:            t.Limits.MaxStore,
			MaxStreams:            int32(t.Limits.MaxStreams),
			MaxConsumers:          int32(t.Limits.MaxConsumers),
			MaxAckPending:         int32(t.Limits.MaxAckPending),
			MemoryMaxStreamBytes:  t.Limits.MemoryMaxStreamBytes,
			StorageMaxStreamBytes: t.Limits.StoreMaxStreamBytes,
			MaxBytesRequired:      t.Limits.MaxBytesRequired,
		},
	}
}
