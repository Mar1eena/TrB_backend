package server

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func PbTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() || t.Unix() <= 0 || t.Year() < 1971 {
		return nil
	}
	return timestamppb.New(t.UTC())
}
