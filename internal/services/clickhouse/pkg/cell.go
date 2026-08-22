package pkg

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func FormatCellValue(val any) string {
	if val == nil {
		return "NULL"
	}
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return "NULL"
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return "NULL"
	}
	val = rv.Interface()

	switch v := val.(type) {
	case time.Time:
		if v.IsZero() {
			return ""
		}
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return v.Format("2006-01-02")
		}
		if v.Nanosecond() > 0 {
			return v.Format("2006-01-02 15:04:05.000000")
		}
		return v.Format("2006-01-02 15:04:05")
	case []byte:
		return string(v)
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])
	case net.IP:
		return v.String()
	case fmt.Stringer:
		return v.String()
	default:
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			var b strings.Builder
			b.WriteByte('[')
			for i := 0; i < rv.Len(); i++ {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(FormatCellValue(rv.Index(i).Interface()))
			}
			b.WriteByte(']')
			return b.String()
		}
		if rv.Kind() == reflect.Map {
			b, err := json.Marshal(v)
			if err == nil {
				return string(b)
			}
		}
		return fmt.Sprintf("%v", v)
	}
}
