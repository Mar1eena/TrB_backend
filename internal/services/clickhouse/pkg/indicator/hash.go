package indicator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// ParamsJSON сериализует параметры как Python json.dumps(..., separators=(",", ":"))
// с float в виде 14.0 — тот же формат, что param_hash в Python-сервисе.
func ParamsJSON(params map[string]float64) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		b.WriteString(formatPyFloat(params[k]))
	}
	b.WriteByte('}')
	return b.String()
}

func formatPyFloat(v float64) string {
	s := strconv.FormatFloat(v, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		return s + ".0"
	}
	return s
}

// ParamHash64 — детерминированный uint64: SHA-256("{indicator}:{paramsJSON}")[:8] little-endian.
func ParamHash64(indicator, paramsJSON string) uint64 {
	sum := sha256.Sum256([]byte(indicator + ":" + paramsJSON))
	return binary.LittleEndian.Uint64(sum[:8])
}

func copyParams(params map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(params))
	for k, v := range params {
		out[k] = v
	}
	return out
}
