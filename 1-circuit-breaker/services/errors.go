package services

import (
	"errors"

	circuitbreaker "github.com/yur4uwe/cloud/1-circuit-breaker/circuit-breaker"
)

var (
	ErrInternal   = errors.New("internal service error")
	ErrBadPayload = circuitbreaker.MarkNonTripping(errors.New("invalid user input"))
)
