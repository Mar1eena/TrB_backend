package indicator

import (
	"errors"
	"fmt"
)

// ComputeError — ошибка валидации/расчёта (недостаточно данных, неизвестный тип).
type ComputeError struct {
	Msg string
}

func (e *ComputeError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

func errf(format string, args ...any) error {
	return &ComputeError{Msg: fmt.Sprintf(format, args...)}
}

func IsComputeError(err error) bool {
	var ce *ComputeError
	return errors.As(err, &ce)
}
