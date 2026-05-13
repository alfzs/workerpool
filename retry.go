package workerpool

import (
	"context"
	"errors"
)

// RetryPredicate определяет, можно ли повторить задачу при ошибке.
// Возвращает true, если задачу нужно повторить, false — если ошибка фатальна.
type RetryPredicate func(err error) bool

// DefaultRetryPredicate — встроенная эвристика по умолчанию.
func DefaultRetryPredicate(err error) bool {
	// nil не ошибка (не должен попадать сюда, но на всякий случай)
	if err == nil {
		return false
	}

	// Отмена контекста — не ретраим
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return true
	}

	// Проверка HTTP статусов
	type httpStatus interface {
		StatusCode() int
	}
	var httpErr httpStatus
	if errors.As(err, &httpErr) {
		code := httpErr.StatusCode()
		// 4xx (кроме 429 Too Many Requests) — не ретраим
		if code >= 400 && code < 500 && code != 429 {
			return false
		}
		// 5xx и 429 — ретраим
		return true
	}

	// По умолчанию — ретраим (безопаснее, чем потерять задачу)
	return true
}
