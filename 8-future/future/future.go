// Package future provides a Future implementation of a future computation.
package future

import (
	"context"
	"errors"
	"sync"
	"time"
)

type FutureState int

const (
	FutureStatePending FutureState = iota
	FutureStateFulfilled
	FutureStateRejected
)

type Future[T any] struct {
	ctx   context.Context
	mu    sync.Mutex
	state FutureState
	value T
	err   error
	done  chan struct{}
}

func (f *Future[T]) resolve(val T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == FutureStatePending {
		f.state = FutureStateFulfilled
		f.value = val
		close(f.done)
	}
}

func (f *Future[T]) reject(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == FutureStatePending {
		f.state = FutureStateRejected
		f.err = err
		close(f.done)
	}
}

func Submit[T any](ctx context.Context, fn func() (T, error)) *Future[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	f := &Future[T]{
		ctx:   ctx,
		done:  make(chan struct{}),
		state: FutureStatePending,
	}

	go func() {
		val, err := fn()
		if err != nil {
			f.reject(err)
		} else {
			f.resolve(val)
		}
	}()

	return f
}

func (f *Future[T]) Then(fn func(T) (T, error)) *Future[T] {
	next := &Future[T]{
		ctx:   f.ctx,
		done:  make(chan struct{}),
		state: FutureStatePending,
	}

	go func() {
		select {
		case <-f.done:
			f.mu.Lock()
			parentErr := f.err
			parentVal := f.value
			f.mu.Unlock()

			if parentErr != nil {
				next.reject(parentErr)
				return
			}

			val, err := fn(parentVal)
			if err != nil {
				next.reject(err)
			} else {
				next.resolve(val)
			}
		case <-f.ctx.Done():
			next.reject(f.ctx.Err())
		}
	}()

	return next
}

func (f *Future[T]) Catch(fn func(error) (T, error)) *Future[T] {
	next := &Future[T]{
		ctx:   f.ctx,
		done:  make(chan struct{}),
		state: FutureStatePending,
	}

	go func() {
		select {
		case <-f.done:
			f.mu.Lock()
			parentErr := f.err
			parentVal := f.value
			f.mu.Unlock()

			if parentErr == nil {
				next.resolve(parentVal)
				return
			}

			val, err := fn(parentErr)
			if err != nil {
				next.reject(err)
			} else {
				next.resolve(val)
			}
		case <-f.ctx.Done():
			next.reject(f.ctx.Err())
		}
	}()

	return next
}

func (f *Future[T]) Await() (T, error) {
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.value, f.err
	case <-f.ctx.Done():
		var zero T
		return zero, f.ctx.Err()
	}
}

func (f *Future[T]) Get(timeout time.Duration) (T, error) {
	var zero T

	if timeout <= 0 {
		return zero, errors.New("timeout must be greater than 0")
	}

	// Check if already completed without waiting
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.value, f.err
	default:
	}

	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.value, f.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func All[T any](futures ...*Future[T]) *Future[[]T] {
	ctx := context.Background()
	if len(futures) > 0 && futures[0].ctx != nil {
		ctx = futures[0].ctx
	}

	res := &Future[[]T]{
		ctx:   ctx,
		done:  make(chan struct{}),
		state: FutureStatePending,
	}

	if len(futures) == 0 {
		res.resolve([]T{})
		return res
	}

	results := make([]T, len(futures))
	var wg sync.WaitGroup
	wg.Add(len(futures))

	for i, fut := range futures {
		go func(idx int, f *Future[T]) {
			defer wg.Done()
			select {
			case <-f.done:
				f.mu.Lock()
				err := f.err
				val := f.value
				f.mu.Unlock()

				if err != nil {
					res.reject(err)
				} else {
					results[idx] = val
				}
			case <-f.ctx.Done():
				res.reject(f.ctx.Err())
			}
		}(i, fut)
	}

	go func() {
		wg.Wait()
		res.resolve(results)
	}()

	return res
}

func Race[T any](futures ...*Future[T]) *Future[T] {
	ctx := context.Background()
	if len(futures) > 0 && futures[0].ctx != nil {
		ctx = futures[0].ctx
	}

	res := &Future[T]{
		ctx:   ctx,
		done:  make(chan struct{}),
		state: FutureStatePending,
	}

	if len(futures) == 0 {
		return res
	}

	for _, fut := range futures {
		go func(f *Future[T]) {
			select {
			case <-f.done:
				f.mu.Lock()
				err := f.err
				val := f.value
				f.mu.Unlock()

				if err != nil {
					res.reject(err)
				} else {
					res.resolve(val)
				}
			case <-f.ctx.Done():
				res.reject(f.ctx.Err())
			}
		}(fut)
	}

	return res
}
