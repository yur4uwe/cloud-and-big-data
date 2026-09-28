package throttle

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottle_WindowLimit_DropExcess(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   3,
		RefillRate: 3,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	for range 10 {
		throttled()
		time.Sleep(10 * time.Millisecond)
	}

	if got := count.Load(); got != 3 {
		t.Fatalf("expected exactly 3 calls to execute immediately from burst of 10, got: %d", got)
	}
}

func TestThrottle_LeadingOnly(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	throttled()
	if got := count.Load(); got != 1 {
		t.Fatalf("expected immediate execution on leading=true, got: %d", got)
	}

	for range 5 {
		throttled()
		time.Sleep(5 * time.Millisecond)
	}
	if got := count.Load(); got != 1 {
		t.Fatalf("expected count to remain 1 when trailing=false, got: %d", got)
	}

	time.Sleep(120 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected count still 1 after interval, got: %d", got)
	}

	throttled()
	if got := count.Load(); got != 2 {
		t.Fatalf("expected count 2 after refill, got: %d", got)
	}
}

func TestThrottle_TrailingOnly(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    false,
		Trailing:   true,
	})
	defer dispose()

	throttled()
	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls immediately with leading=false, got: %d", got)
	}

	for range 3 {
		throttled()
		time.Sleep(5 * time.Millisecond)
	}

	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls before trailing timer triggers, got: %d", got)
	}

	time.Sleep(150 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected exactly 1 trailing call, got: %d", got)
	}
}

func TestThrottle_LeadingAndTrailing_Burst(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   true,
	})
	defer dispose()

	throttled()
	if got := count.Load(); got != 1 {
		t.Fatalf("expected immediate leading call, got: %d", got)
	}

	for range 4 {
		throttled()
		time.Sleep(10 * time.Millisecond)
	}

	if got := count.Load(); got != 1 {
		t.Fatalf("expected count 1 before interval expiry, got: %d", got)
	}

	time.Sleep(150 * time.Millisecond)

	if got := count.Load(); got != 2 {
		t.Fatalf("expected exactly 2 executions (1 leading + 1 trailing), got: %d", got)
	}
}

func TestThrottle_DropExcess(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   2,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	var accepted int
	for range 10 {
		if throttled() {
			accepted++
		}
	}

	if accepted != 2 {
		t.Fatalf("expected 2 accepted calls, got: %d", accepted)
	}
	if got := count.Load(); got != 2 {
		t.Fatalf("expected count to be 2, got: %d", got)
	}
}

func TestThrottle_Queue_PreservesOrder(t *testing.T) {
	var (
		mu      sync.Mutex
		results []int
	)

	opts := ThrottleOpts{
		Capacity:   1,
		RefillRate: 20,
		Mode:       ModeQueue,
	}

	tb := NewTokenBucket(opts.Capacity, opts.RefillRate)
	queue := make(chan int, 20)
	var wg sync.WaitGroup

	go func() {
		for val := range queue {
			_ = tb.Wait(t.Context(), 1)
			mu.Lock()
			results = append(results, val)
			mu.Unlock()
			wg.Done()
		}
	}()

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		queue <- i
	}

	wg.Wait()
	close(queue)

	expected := []int{1, 2, 3, 4, 5}
	mu.Lock()
	defer mu.Unlock()

	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
	for i, val := range expected {
		if results[i] != val {
			t.Fatalf("expected result at index %d to be %d, got %d (order not preserved)", i, val, results[i])
		}
	}
}

func TestThrottle_Dispose_StopsTrailingExecution(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    false,
		Trailing:   true,
	})

	throttled()

	time.Sleep(20 * time.Millisecond)
	dispose()

	time.Sleep(150 * time.Millisecond)

	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls because dispose canceled trailing timer, got: %d", got)
	}
}

func TestThrottle_ConcurrentSafety(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := Throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   5,
		RefillRate: 50,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   true,
	})
	defer dispose()

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range 20 {
				throttled()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	if got := count.Load(); got == 0 {
		t.Fatalf("expected count > 0 under concurrent load, got: %d", got)
	}
}
