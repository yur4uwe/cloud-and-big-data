package timeout

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yur4uwe/cloud/3-retry/retry"
)

func TestWithTimeout_SuccessBeforeTimeout(t *testing.T) {
	var executed atomic.Bool

	resCh, cancel := WithTimeout(func(ctx context.Context) error {
		select {
		case <-time.After(20 * time.Millisecond):
			executed.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 100*time.Millisecond)
	defer cancel()

	err := <-resCh
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !executed.Load() {
		t.Fatalf("expected operation to complete successfully")
	}
}

func TestWithTimeout_ReturnsFnError(t *testing.T) {
	errCustom := errors.New("business logic failure")

	resCh, cancel := WithTimeout(func(ctx context.Context) error {
		return errCustom
	}, 100*time.Millisecond)
	defer cancel()

	err := <-resCh
	if !errors.Is(err, errCustom) {
		t.Fatalf("expected %v, got: %v", errCustom, err)
	}
}

func TestWithTimeout_TimeoutExceeded_SideEffectsPrevented(t *testing.T) {
	var sideEffect atomic.Bool

	start := time.Now()
	resCh, cancel := WithTimeout(func(ctx context.Context) error {
		select {
		case <-time.After(150 * time.Millisecond):
			sideEffect.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 40*time.Millisecond)
	defer cancel()

	err := <-resCh
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("operation took too long to time out: %v", elapsed)
	}

	time.Sleep(120 * time.Millisecond)
	if sideEffect.Load() {
		t.Fatalf("side effect should NOT have occurred after timeout")
	}
}

func TestWithTimeout_CancellationSupport(t *testing.T) {
	resCh, cancel := WithTimeout(func(ctx context.Context) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 500*time.Millisecond)

	time.AfterFunc(25*time.Millisecond, cancel)

	start := time.Now()
	err := <-resCh
	elapsed := time.Since(start)

	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("expected ErrCanceled, got: %v", err)
	}
	if elapsed >= 300*time.Millisecond {
		t.Fatalf("expected early abort on cancellation, took: %v", elapsed)
	}
}

func TestWithTimeout_CombinedWithRetry_GlobalTimeoutBoundsTotalTime(t *testing.T) {
	var attempts atomic.Int32
	maxAttempts := 10
	step := 50 * time.Millisecond
	globalTimeout := 120 * time.Millisecond

	start := time.Now()
	resCh, cancel := WithTimeout(func(ctx context.Context) error {
		return retry.Execute(ctx, func(childCtx context.Context) error {
			attempts.Add(1)
			return errors.New("transient failure")
		}, retry.RetryOptions{
			MaxAttempts: maxAttempts,
			Strategy:    retry.RetryConstant,
			Step:        step,
			Timeout:     globalTimeout,
		})
	}, globalTimeout)
	defer cancel()

	err := <-resCh
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got: %v", err)
	}

	if elapsed > 250*time.Millisecond {
		t.Fatalf("total wait time multiplied uncontrollably: %v (limit: %v)", elapsed, globalTimeout)
	}
}

func TestWithTimeout_CombinedWithRetry_PerAttemptTimeout(t *testing.T) {
	var attempts atomic.Int32
	maxAttempts := 3
	perAttemptTimeout := 30 * time.Millisecond

	err := retry.Execute(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		attemptNum := attempts.Load()

		resCh, cancel := WithTimeout(func(innerCtx context.Context) error {
			if attemptNum < 3 {
				select {
				case <-time.After(100 * time.Millisecond):
					return nil
				case <-innerCtx.Done():
					return innerCtx.Err()
				}
			}
			return nil
		}, perAttemptTimeout)
		defer cancel()

		return <-resCh
	}, retry.RetryOptions{
		MaxAttempts: maxAttempts,
		Strategy:    retry.RetryConstant,
		RetryOn: func(err error) bool {
			return errors.Is(err, ErrTimeout)
		},
		Step:    5 * time.Millisecond,
		Timeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expected eventual success after retrying timed-out attempts, got: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got: %d", attempts.Load())
	}
}
