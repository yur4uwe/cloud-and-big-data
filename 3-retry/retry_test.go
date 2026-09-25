package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var (
	errTransient    = errors.New("temporary failure")
	errNonRetryable = errors.New("fatal client error")
)

func TestExecute_SuccessFirstTry(t *testing.T) {
	var attempts atomic.Int32

	err := Execute(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		return nil
	}, RetryOptions{
		MaxAttempts: 3,
		Strategy:    RetryConstant,
		Step:        10 * time.Millisecond,
		Timeout:     1 * time.Second,
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected 1 attempt, got: %d", got)
	}
}

func TestExecute_SuccessNthTry(t *testing.T) {
	var attempts atomic.Int32
	const targetAttempt = 3

	err := Execute(context.Background(), func(ctx context.Context) error {
		att := attempts.Add(1)
		if att < targetAttempt {
			return errTransient
		}
		return nil
	}, RetryOptions{
		MaxAttempts: 5,
		Strategy:    RetryConstant,
		RetryOn:     func(err error) bool { return errors.Is(err, errTransient) },
		Step:        5 * time.Millisecond,
		Timeout:     1 * time.Second,
	})
	if err != nil {
		t.Fatalf("expected nil error on eventual success, got: %v", err)
	}
	if got := attempts.Load(); got != targetAttempt {
		t.Fatalf("expected %d attempts, got: %d", targetAttempt, got)
	}
}

func TestExecute_MaxAttemptsExhausted(t *testing.T) {
	var attempts atomic.Int32
	const maxAttempts = 4

	err := Execute(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		return errTransient
	}, RetryOptions{
		MaxAttempts: maxAttempts,
		Strategy:    RetryConstant,
		RetryOn:     func(err error) bool { return errors.Is(err, errTransient) },
		Step:        5 * time.Millisecond,
		Timeout:     1 * time.Second,
	})

	if !errors.Is(err, errTransient) {
		t.Fatalf("expected %v, got: %v", errTransient, err)
	}
	if got := attempts.Load(); got != maxAttempts {
		t.Fatalf("expected exactly %d attempts, got: %d", maxAttempts, got)
	}
}

func TestExecute_NonRetryableError_StopsImmediately(t *testing.T) {
	var attempts atomic.Int32

	err := Execute(context.Background(), func(ctx context.Context) error {
		att := attempts.Add(1)
		if att == 1 {
			return errTransient
		}
		return errNonRetryable
	}, RetryOptions{
		MaxAttempts: 5,
		Strategy:    RetryConstant,
		RetryOn:     func(err error) bool { return errors.Is(err, errTransient) },
		Step:        5 * time.Millisecond,
		Timeout:     1 * time.Second,
	})

	if !errors.Is(err, errNonRetryable) {
		t.Fatalf("expected non-retryable error %v, got: %v", errNonRetryable, err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected stopped at attempt 2 on non-retryable error, got: %d", got)
	}
}

func TestExecute_BackoffTiming(t *testing.T) {
	// With RetryExponential and Step = 10ms:
	// Attempt 0 fails -> backoff = 2^0 * 10ms = 10ms
	// Attempt 1 fails -> backoff = 2^1 * 10ms = 20ms
	// Attempt 2 fails -> final attempt, 0 backoff
	// Total expected backoff wait = ~30ms
	const maxAttempts = 3
	const step = 15 * time.Millisecond

	start := time.Now()
	err := Execute(context.Background(), func(ctx context.Context) error {
		return errTransient
	}, RetryOptions{
		MaxAttempts: maxAttempts,
		Strategy:    RetryExponential,
		RetryOn:     func(err error) bool { return true },
		Step:        step,
		Timeout:     1 * time.Second,
	})

	elapsed := time.Since(start)

	if !errors.Is(err, errTransient) {
		t.Fatalf("expected %v, got: %v", errTransient, err)
	}

	// Expected backoff: 2^0 * 15ms (15ms) + 2^1 * 15ms (30ms) = 45ms.
	// Allow acceptable margin for scheduler overhead.
	expected := 45 * time.Millisecond
	margin := 5 * time.Millisecond

	if elapsed < expected-margin || elapsed > expected+margin {
		t.Fatalf("expected elapsed time around %v+-%v, got: %v", expected, margin, elapsed)
	}
}

func TestExecute_ContextCanceledPreflight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var attempts atomic.Int32
	err := Execute(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("expected 0 attempts on pre-canceled context, got: %d", got)
	}
}

func TestExecute_ContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	var attempts atomic.Int32
	start := time.Now()

	err := Execute(ctx, func(ctx context.Context) error {
		attempts.Add(1)
		return errTransient
	}, RetryOptions{
		MaxAttempts: 5,
		Strategy:    RetryExponential,
		RetryOn:     func(err error) bool { return true },
		Step:        500 * time.Millisecond, // Large sleep that should be interrupted
		Timeout:     1 * time.Second,
	})

	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected prompt exit on context cancellation, took: %v", elapsed)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected 1 attempt before cancellation during backoff, got: %d", got)
	}
}

func TestExecute_PerAttemptTimeout(t *testing.T) {
	var attempts atomic.Int32
	const maxAttempts = 2

	err := Execute(context.Background(), func(childCtx context.Context) error {
		attempts.Add(1)
		<-childCtx.Done()
		return childCtx.Err()
	}, RetryOptions{
		MaxAttempts: maxAttempts,
		Strategy:    RetryConstant,
		RetryOn:     func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
		Step:        5 * time.Millisecond,
		Timeout:     20 * time.Millisecond,
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
	if got := attempts.Load(); got != maxAttempts {
		t.Fatalf("expected %d attempts, got: %d", maxAttempts, got)
	}
}
