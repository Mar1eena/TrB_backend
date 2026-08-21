package trb_nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	nats_admin "github.com/Mar1eena/trb_proto/gen/go/api/nats"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Nats) GetMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.RawStreamMsg, error) {
	rawStreamMsg, err := s.Jsc.GetMsg(req.Name, req.Seq)
	if err != nil {
		return nil, err
	}

	return ConvertRawStreamMsg(rawStreamMsg), nil
}

const maxGetMsgs = 256

func (s *Nats) GetMsgs(ctx context.Context, req *nats_admin.MsgRange) (*nats_admin.MsgList, error) {
	name := req.GetName()
	from := req.GetFromSeq()
	to := req.GetToSeq()
	if name == "" {
		return nil, fmt.Errorf("не задано имя стрима")
	}
	if to < from {
		return &nats_admin.MsgList{}, nil
	}
	n := to - from + 1
	if n > maxGetMsgs {
		return nil, fmt.Errorf("диапазон сообщений слишком большой: %d (макс. %d)", n, maxGetMsgs)
	}

	found := make([]*nats_admin.RawStreamMsg, n)
	var firstErr error
	var mu sync.Mutex
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for i := uint64(0); i < n; i++ {
		seq := from + i
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, seq uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			raw, err := s.Jsc.GetMsg(name, seq, nats.Context(ctx))
			if err != nil {
				if isAbsentMsg(err) {
					return
				}
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			found[idx] = ConvertRawStreamMsg(raw)
		}(int(i), seq)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	items := make([]*nats_admin.RawStreamMsg, 0, n)
	for _, item := range found {
		if item != nil {
			items = append(items, item)
		}
	}
	return &nats_admin.MsgList{Items: items}, nil
}

func isAbsentMsg(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrMsgNotFound) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not found") ||
		strings.Contains(text, "no message") ||
		strings.Contains(text, "deleted")
}

func (s *Nats) GetLastMsg(context context.Context, req *nats_admin.LastMsg) (*nats_admin.RawStreamMsg, error) {
	rawStreamMsg, err := s.Jsc.GetLastMsg(req.Name, req.Subject)
	if err != nil {
		return nil, err
	}

	return ConvertRawStreamMsg(rawStreamMsg), nil
}

func (s *Nats) DeleteMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.Response, error) {
	err := s.Jsc.DeleteMsg(req.Name, req.Seq)
	if err != nil {
		return nil, err
	}

	return &nats_admin.Response{Response: "OK"}, nil
}

func (s *Nats) SecureDeleteMsg(context context.Context, req *nats_admin.Msg) (*nats_admin.Response, error) {
	err := s.Jsc.SecureDeleteMsg(req.Name, req.Seq)
	if err != nil {
		return nil, err
	}

	return &nats_admin.Response{Response: "OK"}, nil
}

func ConvertRawStreamMsg(msg *nats.RawStreamMsg) *nats_admin.RawStreamMsg {
	headers := convertToProtoHeaders(msg.Header)
	return &nats_admin.RawStreamMsg{
		Subject: msg.Subject,
		Seq:     msg.Sequence,
		Hdrs:    headers,
		Data:    msg.Data,
		Time:    timestamppb.New(msg.Time),
	}
}

func convertToProtoHeaders(headers map[string][]string) map[string]*nats_admin.Strings {
	protoHeaders := make(map[string]*nats_admin.Strings, len(headers))
	for k, values := range headers {
		protoHeaders[k] = &nats_admin.Strings{
			Values: values,
		}
	}
	return protoHeaders
}
