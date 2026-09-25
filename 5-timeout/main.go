package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	ErrTimeout  = errors.New("timeout")
	ErrCanceled = errors.New("canceled")
)

// withTimeout executes f asynchronously and returns a result channel and a cancel function.
// Channels and timers are used for timeout orchestration, while context is passed to f
// to allow canceling/preventing side effects when the timeout or cancel occurs.
func withTimeout(f func(context.Context) error, timeout time.Duration) (<-chan error, func()) {
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
			cancelCtx() // сповіщаємо fn про настання тайм-ауту через контекст
			out <- ErrTimeout
		}
	}()

	return out, cancel
}

func main() {
	timeout := 5 * time.Second
	fmt.Printf("Quick-time test: Press [Enter] before %v passes!\n> ", timeout)

	start := time.Now()
	resCh, cancel := withTimeout(func(ctx context.Context) error {
		reader := bufio.NewReader(os.Stdin)
		_, err := reader.ReadString('\n')
		return err
	}, timeout)
	defer cancel()

	err := <-resCh
	elapsed := time.Since(start).Round(time.Millisecond)

	if errors.Is(err, ErrTimeout) {
		fmt.Printf("\nTimed out! You took longer than %v.\n", timeout)
		return
	} else if err != nil {
		fmt.Printf("\nInput error: %v\n", err)
		return
	}

	fmt.Printf("Success! You responded in %v.\n", elapsed)
}
