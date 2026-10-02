package fanout_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yur4uwe/cloud/7-fan-out/fanout"
)

func simpleTask(ctx context.Context, task int) int {
	_ = ctx
	return task
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected function to panic, but it completed normally")
		}
	}()

	fn()
}

func TestFanOut_DoesNotSupportLT0Workers(t *testing.T) {
	ctx := context.Background()
	assertPanics(t, func() {
		fanout.FanOut(ctx, simpleTask, 0)
	})
}

func TestFanOut_PanicsOnSubmitAfterSeal(t *testing.T) {
	pool := fanout.FanOut(context.Background(), simpleTask, 1)
	pool.Seal()

	assertPanics(t, func() {
		pool.Submit(42)
	})
}

func TestFanOut_SW(t *testing.T) {
	ctx := context.Background()
	pool := fanout.FanOut(ctx, simpleTask, 1)

	pool.Submit(1)
	pool.Seal()
	res := <-pool.Results()

	pool.Wait()
	if !pool.IsFinished() {
		t.Fatal("expected pool to be finished")
	}
	if res != 1 {
		t.Errorf("expected %d, got %d", 1, res)
	}
}

func TestFanOut_MW(t *testing.T) {
	ctx := context.Background()
	pool := fanout.FanOut(ctx, simpleTask, 2)

	var collectorWg sync.WaitGroup
	results := make([]int, 0, 10)
	collectorWg.Go(func() {
		for result := range pool.Results() {
			results = append(results, result)
		}
	})

	for i := range 10 {
		pool.Submit(i)
	}
	pool.Seal()

	collectorWg.Wait()

	pool.Wait()
	if !pool.IsFinished() {
		t.Fatal("expected pool to be finished")
	}
}

func TestFanOut_MW_NoDoubleProcessing(t *testing.T) {
	ctx := context.Background()
	pool := fanout.FanOut(ctx, simpleTask, 2)

	var collectorWg sync.WaitGroup
	results := make([]int, 0, 10)
	collectorWg.Go(func() {
		for result := range pool.Results() {
			results = append(results, result)
		}
	})

	for i := range 10 {
		pool.Submit(i)
	}
	pool.Seal()

	collectorWg.Wait()

	pool.Wait()
	processed := make(map[int]bool)
	for i := range 10 {
		if processed[results[i]] {
			t.Errorf("expected %d to be processed only once", results[i])
		}
		processed[results[i]] = true
	}
}

func TestFanOut_NFasterThan1(t *testing.T) {
	sleepPerTask := time.Duration(50 * time.Millisecond)
	sleepyTask := func(ctx context.Context, task int) int {
		time.Sleep(sleepPerTask)
		return task
	}

	runBatch := func(workers int) time.Duration {
		pool := fanout.FanOut(context.Background(), sleepyTask, workers)

		var collectorWg sync.WaitGroup
		collectorWg.Go(func() {
			for range pool.Results() {
				// drain results
			}
		})

		start := time.Now()

		for i := range 10 {
			pool.Submit(i)
		}
		pool.Seal()

		pool.Wait()
		collectorWg.Wait()

		return time.Since(start)
	}

	// 1 worker should take roughly: 10 * 50ms = ~500ms
	duration1 := runBatch(1)

	// 4 workers should take roughly: (10 / 4) * 50ms = ~125ms
	duration4 := runBatch(4)

	t.Logf("1 worker took: %v", duration1)
	t.Logf("4 workers took: %v", duration4)

	// Assert significant speedup (e.g., 4 workers should be at least 2x faster than 1)
	if duration4 >= duration1*2 {
		t.Errorf("expected 4 workers (%v) to be faster than 1 worker (%v)", duration4, duration1)
	}
}
