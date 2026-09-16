package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebounce_BurstOfCalls_ExecutesOnce(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay)
	defer debounced.Dispose()

	// Rapid burst of 10 calls spaced by 5ms (total time 45ms < 50ms delay)
	for range 10 {
		debounced.Call()
		time.Sleep(5 * time.Millisecond)
	}

	// At this point, delay hasn't expired since the last call, so count must be 0
	if c := count.Load(); c != 0 {
		t.Fatalf("expected count 0 before delay elapsed, got: %d", c)
	}

	// Wait for the trailing delay to elapse
	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 1 {
		t.Fatalf("expected exactly 1 execution after burst, got: %d", c)
	}
}

func TestDebounce_LeadingOnly_ImmediateFirstAndSuppressesUntilPause(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: false})
	defer debounced.Dispose()

	// 1. First call should execute immediately
	debounced.Call()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count 1 immediately after first call, got: %d", c)
	}

	// 2. Rapid follow-up calls within delay must be suppressed
	for range 5 {
		debounced.Call()
		time.Sleep(5 * time.Millisecond)
	}
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count to remain 1 during rapid calls, got: %d", c)
	}

	// 3. Wait for the debounce delay window to pass
	time.Sleep(delay + 20*time.Millisecond)
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count still 1 after delay when trailing=false, got: %d", c)
	}

	// 4. Next call after pause should trigger leading execution immediately again
	debounced.Call()
	if c := count.Load(); c != 2 {
		t.Fatalf("expected count 2 on new call after pause, got: %d", c)
	}
}

func TestDebounce_LeadingAndTrailing_SingleCall(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer debounced.Dispose()

	debounced.Call()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected immediate leading call (count=1), got: %d", c)
	}

	// Wait for debounce delay
	time.Sleep(delay + 20*time.Millisecond)

	// Since there were no subsequent calls during the window, trailing should NOT fire again
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count 1 (no duplicate trailing for single call), got: %d", c)
	}
}

func TestDebounce_LeadingAndTrailing_Burst(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer debounced.Dispose()

	// First call fires leading immediately
	debounced.Call()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected immediate leading call, got: %d", c)
	}

	// Subsequent calls during window
	for range 4 {
		time.Sleep(10 * time.Millisecond)
		debounced.Call()
	}

	// Still only leading call executed so far
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count still 1 before trailing delay elapses, got: %d", c)
	}

	// Wait for trailing delay
	time.Sleep(delay + 20*time.Millisecond)

	// Should have executed exactly 2 times: 1 leading + 1 trailing
	if c := count.Load(); c != 2 {
		t.Fatalf("expected exactly 2 executions (1 leading + 1 trailing), got: %d", c)
	}
}

// 3c. leading=false, trailing=false → never executes
func TestDebounce_NeitherLeadingNorTrailing(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: false, Trailing: false})
	defer debounced.Dispose()

	for range 5 {
		debounced.Call()
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 0 {
		t.Fatalf("expected count 0 when both leading and trailing are false, got: %d", c)
	}
}

func TestDebounce_Dispose_CancelsScheduledExecution(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay)

	debounced.Call()

	// Dispose before delay expires
	time.Sleep(10 * time.Millisecond)
	debounced.Dispose()

	// Wait past the original delay
	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 0 {
		t.Fatalf("expected count 0 after dispose, got: %d", c)
	}
}

func TestDebounce_ConcurrentSafety(t *testing.T) {
	var count atomic.Int32
	delay := 30 * time.Millisecond

	debounced := debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer debounced.Dispose()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range 10 {
				debounced.Call()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c == 0 {
		t.Fatalf("expected at least 1 execution under concurrent calls, got 0")
	}
}
