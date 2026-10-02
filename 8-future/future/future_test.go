package future_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yur4uwe/cloud/8-future/future"
)

var (
	errExpected = errors.New("expected test error")
)

// 1. Successful execution and value retrieval
func TestFuture_SuccessAndRetrieval(t *testing.T) {
	ctx := context.Background()

	f := future.Submit(ctx, func() (int, error) {
		time.Sleep(20 * time.Millisecond)
		return 42, nil
	})

	val, err := f.Await()
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got: %d", val)
	}

	// Repeated Get() should return cached resolved value without re-running
	valGet, errGet := f.Get(100 * time.Millisecond)
	if errGet != nil {
		t.Fatalf("expected nil error from Get, got: %v", errGet)
	}
	if valGet != 42 {
		t.Fatalf("expected 42, got: %d", valGet)
	}
}

// 2. Chaining: Then transformation and Catch recovery
func TestFuture_ThenAndCatchChaining(t *testing.T) {
	ctx := context.Background()

	// Pipeline: 10 -> * 2 (20) -> + 5 (25)
	f := future.Submit(ctx, func() (int, error) {
		return 10, nil
	}).Then(func(v int) (int, error) {
		return v * 2, nil
	}).Then(func(v int) (int, error) {
		return v + 5, nil
	})

	res, err := f.Get(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 25 {
		t.Fatalf("expected 25, got %d", res)
	}
}

// 3. Error propagation and Catch recovery
func TestFuture_ErrorPropagationAndRecovery(t *testing.T) {
	ctx := context.Background()
	var thenRan bool

	// Fail -> Skip Then -> Catch recovers with fallback (999) -> Next Then runs
	f := future.Submit(ctx, func() (int, error) {
		return 0, errExpected
	}).Then(func(v int) (int, error) {
		thenRan = true
		return v + 1, nil
	}).Catch(func(err error) (int, error) {
		if !errors.Is(err, errExpected) {
			return 0, errors.New("unexpected error received in Catch")
		}
		// Recover with fallback value
		return 999, nil
	}).Then(func(v int) (int, error) {
		return v + 1, nil
	})

	res, err := f.Get(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if thenRan {
		t.Fatal("expected Then to be skipped after error")
	}
	if res != 1000 {
		t.Fatalf("expected 1000 after recovery and then, got %d", res)
	}
}

// 4. Uncaught error propagates to Await / Get
func TestFuture_UncaughtErrorPropagates(t *testing.T) {
	ctx := context.Background()

	f := future.Submit(ctx, func() (string, error) {
		return "", errExpected
	}).Then(func(s string) (string, error) {
		return s + " appended", nil
	})

	_, err := f.Get(100 * time.Millisecond)
	if !errors.Is(err, errExpected) {
		t.Fatalf("expected errExpected, got %v", err)
	}
}

// 5. Timeout on Get(timeout)
func TestFuture_GetTimeout(t *testing.T) {
	ctx := context.Background()

	f := future.Submit(ctx, func() (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 123, nil
	})

	// Timeout is 10ms, but task takes 100ms
	_, err := f.Get(10 * time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	// But waiting with sufficient timeout succeeds
	val, err := f.Get(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("expected success after task completed, got: %v", err)
	}
	if val != 123 {
		t.Fatalf("expected 123, got: %d", val)
	}
}

// 6. Combinator: All() waits for all futures
func TestFuture_AllSuccess(t *testing.T) {
	ctx := context.Background()

	f1 := future.Submit(ctx, func() (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 1, nil
	})
	f2 := future.Submit(ctx, func() (int, error) {
		time.Sleep(30 * time.Millisecond)
		return 2, nil
	})
	f3 := future.Submit(ctx, func() (int, error) {
		return 3, nil
	})

	all := future.All(f1, f2, f3)
	results, err := all.Get(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if len(results) != 3 || results[0] != 1 || results[1] != 2 || results[2] != 3 {
		t.Fatalf("expected [1, 2, 3], got: %v", results)
	}
}

func TestFuture_AllFailsIfOneFails(t *testing.T) {
	ctx := context.Background()

	f1 := future.Submit(ctx, func() (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 1, nil
	})
	f2 := future.Submit(ctx, func() (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 0, errExpected
	})

	all := future.All(f1, f2)
	_, err := all.Get(200 * time.Millisecond)
	if !errors.Is(err, errExpected) {
		t.Fatalf("expected errExpected from All, got: %v", err)
	}
}

// 7. Combinator: Race() settles with first finished future
func TestFuture_Race(t *testing.T) {
	ctx := context.Background()

	slow := future.Submit(ctx, func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "slow", nil
	})
	fast := future.Submit(ctx, func() (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "fast", nil
	})

	race := future.Race(slow, fast)
	val, err := race.Get(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "fast" {
		t.Fatalf("expected 'fast' to win race, got: %q", val)
	}
}

// 8. One-time settlement (fulfilled/rejected once)
func TestFuture_SettledOnce(t *testing.T) {
	ctx := context.Background()

	var counter int64
	f := future.Submit(ctx, func() (int, error) {
		atomic.AddInt64(&counter, 1)
		return 100, nil
	})

	// Multiple parallel Awaits
	for range 5 {
		val, err := f.Await()
		if err != nil || val != 100 {
			t.Fatalf("expected (100, nil), got (%d, %v)", val, err)
		}
	}

	if atomic.LoadInt64(&counter) != 1 {
		t.Fatalf("expected computation to execute exactly once, ran %d times", counter)
	}
}
