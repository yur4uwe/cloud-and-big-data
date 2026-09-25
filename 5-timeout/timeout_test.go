package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 1. Успішна операція до тайм-ауту
func TestWithTimeout_SuccessBeforeTimeout(t *testing.T) {
	var executed atomic.Bool

	resCh, cancel := withTimeout(func(ctx context.Context) error {
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

// 1b. Успішна операція з помилкою бізнес-логіки (не тайм-аут) повертає помилку fn
func TestWithTimeout_ReturnsFnError(t *testing.T) {
	errCustom := errors.New("business logic failure")

	resCh, cancel := withTimeout(func(ctx context.Context) error {
		return errCustom
	}, 100*time.Millisecond)
	defer cancel()

	err := <-resCh
	if !errors.Is(err, errCustom) {
		t.Fatalf("expected %v, got: %v", errCustom, err)
	}
}

// 2. Перевищення — повертається помилка тайм-ауту, побічні ефекти не відбулися / відмінені
func TestWithTimeout_TimeoutExceeded_SideEffectsPrevented(t *testing.T) {
	var sideEffect atomic.Bool

	start := time.Now()
	resCh, cancel := withTimeout(func(ctx context.Context) error {
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

// 2b. Підтримка відміни — операція відміняється достроково викликом cancel()
func TestWithTimeout_CancellationSupport(t *testing.T) {
	resCh, cancel := withTimeout(func(ctx context.Context) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 500*time.Millisecond)

	// Викликаємо функцію відміни через 25ms, хоча тайм-аут 500ms
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

// 3. Комбінування з Retry (не множити загальний час очікування безконтрольно)
// Сценарій А: Загальний тайм-аут обмежує цикл повторів (Global Timeout wrapping Retry)
func TestWithTimeout_CombinedWithRetry_GlobalTimeoutBoundsTotalTime(t *testing.T) {
	var attempts atomic.Int32
	maxAttempts := 10
	step := 50 * time.Millisecond
	globalTimeout := 120 * time.Millisecond

	start := time.Now()
	resCh, cancel := withTimeout(func(ctx context.Context) error {
		for i := 0; i < maxAttempts; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			attempts.Add(1)
			time.Sleep(step)
		}
		return errors.New("all attempts failed")
	}, globalTimeout)
	defer cancel()

	err := <-resCh
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got: %v", err)
	}

	// 10 спроб по 50ms зайняли б 500ms. Загальний тайм-аут повертає управління через ~120ms
	if elapsed > 250*time.Millisecond {
		t.Fatalf("total wait time multiplied uncontrollably: %v (limit: %v)", elapsed, globalTimeout)
	}
}

// Сценарій Б: Повтор окремих спроб, якщо кожна окрема перевищує per-attempt тайм-аут
func TestWithTimeout_CombinedWithRetry_PerAttemptTimeout(t *testing.T) {
	var attempts atomic.Int32
	maxAttempts := 3
	perAttemptTimeout := 30 * time.Millisecond

	retryFn := func() error {
		for i := 0; i < maxAttempts; i++ {
			attempts.Add(1)
			attemptNum := attempts.Load()

			resCh, cancel := withTimeout(func(ctx context.Context) error {
				if attemptNum < 3 {
					// Перші 2 спроби зависають довше за тайм-аут
					select {
					case <-time.After(100 * time.Millisecond):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				// 3-я спроба швидка та успішна
				return nil
			}, perAttemptTimeout)

			err := <-resCh
			cancel()

			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrTimeout) {
				return err
			}
		}
		return errors.New("max attempts exceeded")
	}

	err := retryFn()
	if err != nil {
		t.Fatalf("expected eventual success after retrying timed-out attempts, got: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got: %d", attempts.Load())
	}
}
