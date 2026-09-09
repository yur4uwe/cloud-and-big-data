package circuitbreaker

import (
	"context"
	"sync"
	"time"
)

type CBState int

const (
	StateClosed CBState = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreakerConfig struct {
	OpenStateDuration time.Duration
	TimeoutPerCall    time.Duration
	HalfOpenMaxCalls  int
	FailureThreshold  int
}

type CircuitBreaker struct {
	state                CBState
	config               CircuitBreakerConfig
	openStateEntry       time.Time
	failureCount         int
	halfOpenSuccessCount int
	halfOpenInFlight     bool
	mu                   sync.Mutex
}

func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		state:  StateClosed,
		config: config,
	}
}

func (cb *CircuitBreaker) State() CBState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) FailureCount() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failureCount
}

func (cb *CircuitBreaker) HalfOpenSuccessCount() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.halfOpenSuccessCount
}

func (cb *CircuitBreaker) runWithTimeout(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, cb.config.TimeoutPerCall)
	defer cancel()
	doneChan := make(chan error, 1)
	go func() {
		doneChan <- fn(ctx)
	}()
	select {
	case err := <-doneChan:
		return err
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return ErrTimeout
		default:
			return ctx.Err()
		}
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	cb.mu.Lock()
	if cb.state == StateOpen && time.Since(cb.openStateEntry) >= cb.config.OpenStateDuration {
		cb.state = StateHalfOpen
		cb.failureCount = 0
		cb.halfOpenSuccessCount = 0
		cb.halfOpenInFlight = false
	}
	state := cb.state
	cb.mu.Unlock()

	var err error
	switch state {
	case StateOpen:
		err = ErrCircuitOpen

	case StateClosed:
		err = cb.runWithTimeout(ctx, fn)
		cb.mu.Lock()
		defer cb.mu.Unlock()

		if isFailure(err) {
			cb.failureCount++
			if cb.failureCount >= cb.config.FailureThreshold {
				cb.state = StateOpen
				cb.openStateEntry = time.Now()
				cb.failureCount = 0
			}
		} else {
			cb.failureCount = 0
		}

	case StateHalfOpen:
		cb.mu.Lock()
		// Only allow one probe call in-flight at a time
		if cb.halfOpenInFlight {
			cb.mu.Unlock()
			return ErrTooManyCalls
		}
		cb.halfOpenInFlight = true
		cb.mu.Unlock()

		err = cb.runWithTimeout(ctx, fn)

		cb.mu.Lock()
		defer cb.mu.Unlock()
		cb.halfOpenInFlight = false

		if isFailure(err) {
			// Probe failed: trip back to Open immediately
			cb.state = StateOpen
			cb.openStateEntry = time.Now()
			cb.failureCount = 0
			cb.halfOpenSuccessCount = 0
		} else {
			// Probe succeeded: increment success counter
			cb.halfOpenSuccessCount++
			if cb.halfOpenSuccessCount >= cb.config.HalfOpenMaxCalls {
				cb.state = StateClosed
				cb.failureCount = 0
				cb.halfOpenSuccessCount = 0
			}
		}
	}

	return err
}

// Call is a generic wrapper to execute a typed function through the CircuitBreaker.
func Call[T any](ctx context.Context, cb *CircuitBreaker, fn func(ctx context.Context) (T, error)) (T, error) {
	var result T
	err := cb.Execute(ctx, func(callCtx context.Context) error {
		var err error
		result, err = fn(callCtx)
		return err
	})
	return result, err
}
