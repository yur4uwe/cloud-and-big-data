// Package fanout provides a fan-out implementation of a worker pool.
package fanout

import (
	"context"
	"sync"
)

type WorkerPool[InT, ResT any] struct {
	ctx       context.Context
	done      chan struct{}
	process   func(context.Context, InT) ResT
	results   chan ResT
	taskQueue chan InT
}

// Submit adds a task to the queue. When all tasks are submitted the pool must be sealed. The results must be drained concurrently to avoid backpressure. Panics if the pool is sealed.
func (w *WorkerPool[InT, ResT]) Submit(task InT) {
	w.taskQueue <- task
}

// Results returns a channel that receives the results of the tasks.
func (w *WorkerPool[InT, ResT]) Results() <-chan ResT {
	return w.results
}

// Seal signals that no more tasks will be submitted.
func (w *WorkerPool[InT, ResT]) Seal() {
	close(w.taskQueue)
}

// IsFinished returns true if the pool has finished processing all tasks.
func (w *WorkerPool[InT, ResT]) IsFinished() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// Wait blocks until the pool has finished processing all tasks.
func (w *WorkerPool[InT, ResT]) Wait() {
	<-w.done
}

func FanOut[InT, ResT any](ctx context.Context, process func(context.Context, InT) ResT, workers int) *WorkerPool[InT, ResT] {
	if workers < 1 {
		panic("fanout: workers must be greater than 0")
	}

	w := &WorkerPool[InT, ResT]{
		ctx:       ctx,
		process:   process,
		results:   make(chan ResT, 2*workers+1),
		taskQueue: make(chan InT, workers),
		done:      make(chan struct{}),
	}

	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			// fmt.Printf("worker %d started\n", i)
			for task := range w.taskQueue {
				// fmt.Printf("worker %d got task %v\n", i, task)
				select {
				case <-w.ctx.Done():
					return
				default:
				}
				// fmt.Printf("worker %d processing task %v\n", i, task)
				res := w.process(w.ctx, task)
				// fmt.Printf("worker %d processed task %v\n", i, task)
				w.results <- res
				// fmt.Printf("worker %d sent result %v\n", i, res)
			}
		})
	}

	go func() {
		// fmt.Printf("coordinator waiting for workers to finish\n")
		wg.Wait()
		// fmt.Printf("coordinator closing results channel\n")
		close(w.results)
		close(w.done)
	}()

	return w
}
