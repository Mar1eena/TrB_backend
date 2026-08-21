package clickhouse

import (
	"bytes"
	"encoding/json"
)

// OutputAsString возвращает сырой вывод ClickHouse ([]byte) как строку.
// Удобно, когда ответ нужно отдать клиенту в виде строки (например, в gRPC/JSON поле типа string).
func OutputAsString(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return string(data)
}

// RowsToJSONString сериализует строки ([]map[string]interface{}) в одну JSON-строку.
// Формат: массив объектов [{"col1": "val1", ...}, ...]. Для пустого среза возвращает "[]".
func RowsToJSONString(rows []map[string]interface{}) (string, error) {
	if len(rows) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ClickHouseJSONResponse — формат ответа ClickHouse FORMAT JSON (одним объектом).
// См. https://clickhouse.com/docs/en/interfaces/formats#json
type ClickHouseJSONResponse struct {
	Meta        []struct{ Name string `json:"name"`; Type string `json:"type"` } `json:"meta"`
	Data        []map[string]interface{}                                       `json:"data"`
	Rows        int                                                             `json:"rows"`
	RowsRead    int64                                                           `json:"rows_read"`
	BytesRead   int64                                                           `json:"bytes_read"`
	Statistics  map[string]interface{}                                          `json:"statistics"`
	Elapsed     float64                                                         `json:"elapsed,omitempty"`
}

// ParseJSON разбирает []byte с JSON в структуру ответа ClickHouse (meta, data, rows, ...).
// Удобно, когда запрос выполнен с FORMAT JSON.
func ParseJSON(data []byte) (*ClickHouseJSONResponse, error) {
	var out ClickHouseJSONResponse
	err := json.Unmarshal(data, &out)
	return &out, err
}

// ParseJSONToMaps разбирает []byte с JSON в срез карт (каждая строка — map[имя_колонки]значение).
// Подходит для ответа ClickHouse с полем "data" или для массива объектов.
func ParseJSONToMaps(data []byte) ([]map[string]interface{}, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}
	// Полный ответ ClickHouse: { "data": [ {...}, {...} ] }
	if bytes.HasPrefix(data, []byte("{")) {
		var wrapper struct {
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &wrapper); err != nil {
			return nil, err
		}
		return wrapper.Data, nil
	}
	// Массив объектов: [ {...}, {...} ]
	if bytes.HasPrefix(data, []byte("[")) {
		var arr []map[string]interface{}
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}
	return nil, nil
}

// ParseJSONEachRow разбирает []byte в формате JSONEachRow (каждая строка — отдельный JSON).
// Удобно для стриминга и больших ответов.
func ParseJSONEachRow(data []byte) ([]map[string]interface{}, error) {
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	out := make([]map[string]interface{}, 0, len(lines))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}
