package services

import (
	"errors"

	circuitbreaker "github.com/yur4uwe/cloud/lab-1/circuit-breaker"
)

var (
	ErrInternal   = errors.New("internal service error")
	ErrBadPayload = circuitbreaker.MarkNonTripping(errors.New("invalid user input"))
)
