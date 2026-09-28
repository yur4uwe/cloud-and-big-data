package debounce

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebounce_BurstOfCalls_ExecutesOnce(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay)
	defer dispose()

	for range 10 {
		debounced()
		time.Sleep(5 * time.Millisecond)
	}

	if c := count.Load(); c != 0 {
		t.Fatalf("expected count 0 before delay elapsed, got: %d", c)
	}

	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 1 {
		t.Fatalf("expected exactly 1 execution after burst, got: %d", c)
	}
}

func TestDebounce_LeadingOnly_ImmediateFirstAndSuppressesUntilPause(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: false})
	defer dispose()

	debounced()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count 1 immediately after first call, got: %d", c)
	}

	for range 5 {
		debounced()
		time.Sleep(5 * time.Millisecond)
	}
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count to remain 1 during rapid calls, got: %d", c)
	}

	time.Sleep(delay + 20*time.Millisecond)
	if c := count.Load(); c != 1 {
		t.Fatalf("expected count still 1 after delay when trailing=false, got: %d", c)
	}

	debounced()
	if c := count.Load(); c != 2 {
		t.Fatalf("expected count 2 on new call after pause, got: %d", c)
	}
}

func TestDebounce_LeadingAndTrailing_SingleCall(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer dispose()

	debounced()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected immediate leading call (count=1), got: %d", c)
	}

	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 1 {
		t.Fatalf("expected count 1 (no duplicate trailing for single call), got: %d", c)
	}
}

func TestDebounce_LeadingAndTrailing_Burst(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer dispose()

	debounced()
	if c := count.Load(); c != 1 {
		t.Fatalf("expected immediate leading call, got: %d", c)
	}

	for range 4 {
		time.Sleep(10 * time.Millisecond)
		debounced()
	}

	if c := count.Load(); c != 1 {
		t.Fatalf("expected count still 1 before trailing delay elapses, got: %d", c)
	}

	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 2 {
		t.Fatalf("expected exactly 2 executions (1 leading + 1 trailing), got: %d", c)
	}
}

func TestDebounce_NeitherLeadingNorTrailing(t *testing.T) {
	var count atomic.Int32
	delay := 50 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: false, Trailing: false})
	defer dispose()

	for range 5 {
		debounced()
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

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay)

	debounced()

	time.Sleep(10 * time.Millisecond)
	dispose()

	time.Sleep(delay + 20*time.Millisecond)

	if c := count.Load(); c != 0 {
		t.Fatalf("expected count 0 after dispose, got: %d", c)
	}
}

func TestDebounce_ConcurrentSafety(t *testing.T) {
	var count atomic.Int32
	delay := 30 * time.Millisecond

	debounced, dispose := Debounce(func() {
		count.Add(1)
	}, delay, DebounceOpts{Leading: true, Trailing: true})
	defer dispose()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range 10 {
				debounced()
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
