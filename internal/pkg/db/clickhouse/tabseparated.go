package clickhouse

import (
	"bytes"
	"strings"
)

// ParseTabSeparated разбирает []byte в формате TabSeparated (колонки — \t, строки — \n).
// Возвращает срез строк: rows[i][j] — значение j-й колонки в i-й строке.
// Пустая строка в конце (после последнего \n) отбрасывается.
func ParseTabSeparated(data []byte) [][]string {
	if len(data) == 0 {
		return nil
	}
	text := string(data)
	// Убираем завершающий \n, чтобы не получить лишнюю пустую строку
	text = strings.TrimSuffix(text, "\n")
	lines := strings.Split(text, "\n")
	rows := make([][]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, "\t")
		rows = append(rows, cols)
	}
	return rows
}

// ParseTabSeparatedWithBuf то же, что ParseTabSeparated, но разбивает по \n через bytes, без аллокации одной большой строки.
func ParseTabSeparatedWithBuf(data []byte) [][]string {
	if len(data) == 0 {
		return nil
	}
	rows := make([][]string, 0, bytes.Count(data, []byte("\n"))+1)
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			line := bytes.TrimSuffix(data, []byte("\r"))
			if len(line) > 0 {
				rows = append(rows, splitTab(line))
			}
			break
		}
		line := data[:i]
		data = data[i+1:]
		if len(line) > 0 {
			rows = append(rows, splitTab(line))
		}
	}
	return rows
}

func splitTab(line []byte) []string {
	cols := make([]string, 0, bytes.Count(line, []byte("\t"))+1)
	for {
		i := bytes.IndexByte(line, '\t')
		if i < 0 {
			cols = append(cols, string(line))
			break
		}
		cols = append(cols, string(line[:i]))
		line = line[i+1:]
	}
	return cols
}
