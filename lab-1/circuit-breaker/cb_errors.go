package circuitbreaker

import (
	"context"
	"errors"
)

var (
	ErrCircuitOpen  = errors.New("circuit breaker is open")
	ErrTooManyCalls = errors.New("too many calls")
	ErrTimeout      = errors.New("timeout")
)

type NonTrippingError struct {
	Err error
}

var _ error = NonTrippingError{}

func (e NonTrippingError) Error() string {
	return e.Err.Error()
}

func (e NonTrippingError) Unwrap() error {
	return e.Err
}

func MarkNonTripping(err error) error {
	if err == nil {
		return nil
	}
	return NonTrippingError{Err: err}
}

func isFailure(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	var nonTripping NonTrippingError
	return !errors.As(err, &nonTripping)
}
