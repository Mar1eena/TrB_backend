package postgres

import (
	"errors"
	"fmt"
)

// ErrNotFound — строки нет (обёртка над pgx.ErrNoRows для слоя сервиса).
var ErrNotFound = errors.New("not found")

func clampLimit(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 500 {
		return 500
	}
	return n
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }
