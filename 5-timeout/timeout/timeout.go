package timeout

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrTimeout  = errors.New("timeout")
	ErrCanceled = errors.New("canceled")
)

// WithTimeout executes f asynchronously and returns a result channel and a cancel function.
// Channels and timers are used for timeout orchestration, while context is passed to f
// to allow canceling/preventing side effects when the timeout or cancel occurs.
func WithTimeout(f func(context.Context) error, timeout time.Duration) (<-chan error, func()) {
	out := make(chan error, 1)
	done := make(chan error, 1)
	cancelCh := make(chan struct{})

	ctx, cancelCtx := context.WithCancel(context.Background())

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			cancelCtx()
			close(cancelCh)
		})
	}

	go func() {
		done <- f(ctx)
	}()

	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case err := <-done:
			out <- err
		case <-cancelCh:
			out <- ErrCanceled
		case <-timer.C:
			cancelCtx()
			out <- ErrTimeout
		}
	}()

	return out, cancel
}
