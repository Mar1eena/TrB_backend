package pkg

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/protobuf/proto"
)

var (
	ErrSettingsRequired      = errors.New("settings обязателен")
	ErrIndicatorTypeRequired = errors.New("indicator_type обязателен")
)

var deterministicMarshal = proto.MarshalOptions{Deterministic: true}

// Hash64 — SHA-256(канонический Settings без End)[:8] little-endian как uint64.
func Hash64(settings *indpb.Settings) (uint64, error) {
	payload, err := canonicalBytes(settings, false)
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(payload)
	return binary.LittleEndian.Uint64(sum[:8]), nil
}

// CanonicalBytes сериализует Settings с тем же набором полей, что хранится в request.
func CanonicalBytes(settings *indpb.Settings) ([]byte, error) {
	return canonicalBytes(settings, true)
}

func canonicalBytes(settings *indpb.Settings, includeEnd bool) ([]byte, error) {
	if settings == nil || settings.GetSettings() == nil {
		return nil, ErrSettingsRequired
	}
	if settings.GetSettings().GetIndicatorType() == nil {
		return nil, ErrIndicatorTypeRequired
	}
	canonical := &indpb.Settings{
		Interval: settings.GetInterval(),
		Uid:      settings.GetUid(),
		Settings: settings.GetSettings(),
	}
	if settings.Start != nil {
		canonical.Start = settings.Start
	}
	if includeEnd && settings.End != nil {
		canonical.End = settings.End
	}
	return deterministicMarshal.Marshal(canonical)
}
