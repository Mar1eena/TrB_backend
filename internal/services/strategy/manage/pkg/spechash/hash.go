// Package spechash — канонический хэш StrategySpec (дедуп прогонов и кандидатов).
package spechash

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/protobuf/proto"
)

// ErrSpecRequired — spec пустой.
var ErrSpecRequired = errors.New("spec обязателен")

var deterministicMarshal = proto.MarshalOptions{Deterministic: true}

// Hash64 — SHA-256(детерминированный marshal spec)[:8] little-endian как uint64.
// Совпадает по способу вычисления с indicators/manage/pkg/hash.go.
func Hash64(spec *strategypb.StrategySpec) (uint64, error) {
	if spec == nil {
		return 0, ErrSpecRequired
	}
	payload, err := deterministicMarshal.Marshal(spec)
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(payload)
	return binary.LittleEndian.Uint64(sum[:8]), nil
}

// Signed — представление uint64-хэша для колонки Postgres bigint (без потерь).
func Signed(h uint64) int64 { return int64(h) }
