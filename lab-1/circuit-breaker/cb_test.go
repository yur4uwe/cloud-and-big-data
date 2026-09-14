package circuitbreaker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	circuitbreaker "github.com/yur4uwe/cloud/lab-1/circuit-breaker"
	"github.com/yur4uwe/cloud/lab-1/services"
)

func newTestConfig() circuitbreaker.CircuitBreakerConfig {
	return circuitbreaker.CircuitBreakerConfig{
		FailureThreshold:  3,
		HalfOpenMaxCalls:  2,
		OpenStateDuration: 100 * time.Millisecond,
		TimeoutPerCall:    40 * time.Millisecond,
	}
}

func TestCircuitBreaker_TripToOpenOnConsecutiveFailures(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	for i := range 2 {
		_, err := circuitbreaker.Call(ctx, cb, failCall)
		if !errors.Is(err, services.ErrInternal) {
			t.Fatalf("expected ErrInternal, got: %v", err)
		}
		if cb.State() != circuitbreaker.StateClosed {
			t.Fatalf("expected state Closed on failure %d, got: %v", i+1, cb.State())
		}
		if cb.FailureCount() != i+1 {
			t.Fatalf("expected failureCount %d, got: %d", i+1, cb.FailureCount())
		}
	}

	_, err := circuitbreaker.Call(ctx, cb, failCall)
	if !errors.Is(err, services.ErrInternal) {
		t.Fatalf("expected ErrInternal, got: %v", err)
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after 3 failures, got: %v", cb.State())
	}
}

func TestCircuitBreaker_TripToOpenOnTimeouts(t *testing.T) {
	cfg := newTestConfig()
	cfg.TimeoutPerCall = 20 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	hangCall := mock.HangFor(100 * time.Millisecond)

	for i := range 3 {
		_, err := circuitbreaker.Call(ctx, cb, hangCall)
		if !errors.Is(err, circuitbreaker.ErrTimeout) {
			t.Fatalf("call %d: expected ErrTimeout, got: %v", i+1, err)
		}
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after 3 timeouts, got: %v", cb.State())
	}
}

func TestCircuitBreaker_FastRejectionInOpenState(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	for range 3 {
		circuitbreaker.Call(ctx, cb, failCall)
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open, got: %v", cb.State())
	}

	callCountBefore := mock.CallCount()

	_, err := circuitbreaker.Call(ctx, cb, failCall)
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got: %v", err)
	}

	if mock.CallCount() != callCountBefore {
		t.Fatalf("expected mock not to be called in Open state, before=%d, after=%d", callCountBefore, mock.CallCount())
	}
}

func TestCircuitBreaker_HalfOpenRecoveryToClosed(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cfg.HalfOpenMaxCalls = 2
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	for range 3 {
		circuitbreaker.Call(ctx, cb, failCall)
	}

	// Wait for cooldown to elapse
	time.Sleep(60 * time.Millisecond)

	res, err := circuitbreaker.Call(ctx, cb, mock.SuccessCall)
	if err != nil || res != "ok" {
		t.Fatalf("probe 1 failed: %v", err)
	}
	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected state HalfOpen after probe 1, got: %v", cb.State())
	}
	if cb.HalfOpenSuccessCount() != 1 {
		t.Fatalf("expected 1 half-open success, got: %d", cb.HalfOpenSuccessCount())
	}

	res2, err := circuitbreaker.Call(ctx, cb, mock.SuccessCall)
	if err != nil || res2 != "ok" {
		t.Fatalf("probe 2 failed: %v", err)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected state Closed after probe 2, got: %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenRelapseOnFailure(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	for range 3 {
		circuitbreaker.Call(ctx, cb, failCall)
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected state HalfOpen after timeout, got: %v", cb.State())
	}

	_, err := circuitbreaker.Call(ctx, cb, failCall)
	if !errors.Is(err, services.ErrInternal) {
		t.Fatalf("expected ErrInternal on probe failure, got: %v", err)
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after probe failure, got: %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenRejectsExcessProbes(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cfg.TimeoutPerCall = 100 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	// Trip to Open
	for range 3 {
		circuitbreaker.Call(ctx, cb, failCall)
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	hangCall := mock.HangFor(80 * time.Millisecond)

	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)
	// Probe 1 starts
	go func() {
		defer wg.Done()
		_, err1 = circuitbreaker.Call(ctx, cb, hangCall)
	}()

	// Small pause to guarantee Probe 1 acquired the in-flight slot
	time.Sleep(10 * time.Millisecond)

	// Probe 2 attempts while Probe 1 is still in flight
	go func() {
		defer wg.Done()
		_, err2 = circuitbreaker.Call(ctx, cb, failCall)
	}()

	wg.Wait()
	_ = err1

	if !errors.Is(err2, circuitbreaker.ErrTooManyCalls) {
		t.Fatalf("expected second concurrent probe to receive ErrTooManyCalls, got: %v", err2)
	}
}

// 7. Test non-tripping business errors do NOT increment failure count
func TestCircuitBreaker_NonTrippingErrorsIgnored(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	ctx := context.Background()

	badPayloadCall := mock.FailWith(services.ErrBadPayload)

	for range 5 {
		_, err := circuitbreaker.Call(ctx, cb, badPayloadCall)
		if !errors.Is(err, services.ErrBadPayload) {
			t.Fatalf("expected ErrBadPayload, got: %v", err)
		}
	}

	// Breaker must remain Closed with 0 failure count
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected state Closed, got: %v", cb.State())
	}
	if cb.FailureCount() != 0 {
		t.Fatalf("expected failureCount 0, got: %d", cb.FailureCount())
	}
}

func TestCircuitBreaker_ConcurrentSafety(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	ctx := context.Background()

	failCall := mock.FailWith(services.ErrInternal)

	const workers = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := range workers {
		go func(id int) {
			defer wg.Done()
			for range 20 {
				var toCall func(context.Context) (services.Payload, error)
				if id%5 == 0 {
					toCall = failCall
				} else {
					toCall = mock.SuccessCall
				}
				circuitbreaker.Call(ctx, cb, toCall)
			}
		}(i)
	}

	wg.Wait()
}
