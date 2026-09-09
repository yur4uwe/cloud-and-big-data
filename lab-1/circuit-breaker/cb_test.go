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

// 1. Test transition to Open after N consecutive failures
func TestCircuitBreaker_TripToOpenOnConsecutiveFailures(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	mock.SetMode(services.ModeFail)
	ctx := context.Background()

	// First 2 failures: breaker stays Closed
	for i := 1; i <= 2; i++ {
		_, err := circuitbreaker.Call(ctx, cb, mock.Call)
		if !errors.Is(err, services.ErrInternal) {
			t.Fatalf("expected ErrInternal, got: %v", err)
		}
		if cb.State() != circuitbreaker.StateClosed {
			t.Fatalf("expected state Closed on failure %d, got: %v", i, cb.State())
		}
		if cb.FailureCount() != i {
			t.Fatalf("expected failureCount %d, got: %d", i, cb.FailureCount())
		}
	}

	// 3rd failure: trips to Open
	_, err := circuitbreaker.Call(ctx, cb, mock.Call)
	if !errors.Is(err, services.ErrInternal) {
		t.Fatalf("expected ErrInternal, got: %v", err)
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after 3 failures, got: %v", cb.State())
	}
}

// 2. Test transition to Open after consecutive timeouts
func TestCircuitBreaker_TripToOpenOnTimeouts(t *testing.T) {
	cfg := newTestConfig()
	cfg.TimeoutPerCall = 20 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	mock.SetHangDuration(100 * time.Millisecond)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		_, err := circuitbreaker.Call(ctx, cb, mock.Call)
		if !errors.Is(err, circuitbreaker.ErrTimeout) {
			t.Fatalf("call %d: expected ErrTimeout, got: %v", i, err)
		}
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after 3 timeouts, got: %v", cb.State())
	}
}

// 3. Test fast rejection in Open state without touching downstream service
func TestCircuitBreaker_FastRejectionInOpenState(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	mock.SetMode(services.ModeFail)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		circuitbreaker.Call(ctx, cb, mock.Call)
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open, got: %v", cb.State())
	}

	callCountBefore := mock.CallCount()

	// Next call in Open state must be fast-rejected with ErrCircuitOpen
	_, err := circuitbreaker.Call(ctx, cb, mock.Call)
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got: %v", err)
	}

	// Mock call count must NOT increase
	if mock.CallCount() != callCountBefore {
		t.Fatalf("expected mock not to be called in Open state, before=%d, after=%d", callCountBefore, mock.CallCount())
	}
}

// 4. Test HalfOpen recovery to Closed after HalfOpenMaxCalls successes
func TestCircuitBreaker_HalfOpenRecoveryToClosed(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cfg.HalfOpenMaxCalls = 2
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	mock.SetMode(services.ModeFail)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		circuitbreaker.Call(ctx, cb, mock.Call)
	}

	// Wait for cooldown to elapse
	time.Sleep(60 * time.Millisecond)

	// Now backend is healthy again
	mock.SetMode(services.ModeSuccess)

	// Probe 1: transitions to HalfOpen and succeeds
	res1, err := circuitbreaker.Call(ctx, cb, mock.Call)
	if err != nil || res1 != "ok" {
		t.Fatalf("probe 1 failed: %v", err)
	}
	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected state HalfOpen after probe 1, got: %v", cb.State())
	}
	if cb.HalfOpenSuccessCount() != 1 {
		t.Fatalf("expected 1 half-open success, got: %d", cb.HalfOpenSuccessCount())
	}

	// Probe 2: reaches HalfOpenMaxCalls (2) -> heals to Closed
	res2, err := circuitbreaker.Call(ctx, cb, mock.Call)
	if err != nil || res2 != "ok" {
		t.Fatalf("probe 2 failed: %v", err)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected state Closed after probe 2, got: %v", cb.State())
	}
}

// 5. Test HalfOpen relapse to Open on probe failure
func TestCircuitBreaker_HalfOpenRelapseOnFailure(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	mock.SetMode(services.ModeFail)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		circuitbreaker.Call(ctx, cb, mock.Call)
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// Probe call fails
	_, err := circuitbreaker.Call(ctx, cb, mock.Call)
	if !errors.Is(err, services.ErrInternal) {
		t.Fatalf("expected ErrInternal on probe failure, got: %v", err)
	}

	// Must immediately trip back to Open
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected state Open after probe failure, got: %v", cb.State())
	}
}

// 6. Test HalfOpen rejects concurrent probe calls with ErrTooManyCalls
func TestCircuitBreaker_HalfOpenRejectsExcessProbes(t *testing.T) {
	cfg := newTestConfig()
	cfg.OpenStateDuration = 50 * time.Millisecond
	cfg.TimeoutPerCall = 100 * time.Millisecond
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	mock.SetMode(services.ModeFail)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		circuitbreaker.Call(ctx, cb, mock.Call)
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// Configure mock to hang during probe
	mock.SetHangDuration(80 * time.Millisecond)

	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)
	// Probe 1 starts
	go func() {
		defer wg.Done()
		_, err1 = circuitbreaker.Call(ctx, cb, mock.Call)
	}()

	// Small pause to guarantee Probe 1 acquired the in-flight slot
	time.Sleep(10 * time.Millisecond)

	// Probe 2 attempts while Probe 1 is still in flight
	go func() {
		defer wg.Done()
		_, err2 = circuitbreaker.Call(ctx, cb, mock.Call)
	}()

	wg.Wait()
	_ = err1

	// Probe 2 must be rejected with ErrTooManyCalls
	if !errors.Is(err2, circuitbreaker.ErrTooManyCalls) {
		t.Fatalf("expected second concurrent probe to receive ErrTooManyCalls, got: %v", err2)
	}
}

// 7. Test non-tripping business errors do NOT increment failure count
func TestCircuitBreaker_NonTrippingErrorsIgnored(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	mock.SetError(services.ErrBadPayload)
	ctx := context.Background()

	// Make 5 calls returning ErrBadPayload
	for i := 0; i < 5; i++ {
		_, err := circuitbreaker.Call(ctx, cb, mock.Call)
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

// 8. Test thread safety under high concurrency
func TestCircuitBreaker_ConcurrentSafety(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker(newTestConfig())
	mock := services.NewMockService()
	ctx := context.Background()

	const workers = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if id%5 == 0 {
					mock.SetMode(services.ModeFail)
				} else {
					mock.SetMode(services.ModeSuccess)
				}
				circuitbreaker.Call(ctx, cb, mock.Call)
			}
		}(i)
	}

	wg.Wait()
}
