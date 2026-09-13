// Package spechash — канонический хэш StrategySearchSpec (дедуп прогонов бэктеста).
package spechash

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/protobuf/proto"
)

// ErrSpecRequired — spec пустой.
var ErrSpecRequired = errors.New("spec обязателен")

var deterministicMarshal = proto.MarshalOptions{Deterministic: true}

// Hash64 — SHA-256(детерминированный marshal spec)[:8] little-endian как uint64.
// Совпадает по способу вычисления с _common/specmod/hash.py::spec_hash_u64 (Python-сторона движка).
func Hash64(spec *strategysearchpb.StrategySearchSpec) (uint64, error) {
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
