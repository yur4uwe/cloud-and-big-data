package main

import (
	"errors"
	"fmt"
	"go/scanner"
	"time"
)

func withTimeout(f func() error, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout")
	}
}

func main() {
	err := withTimeout(func() error {
		fmt.Println("Please press space before 10 seconds pass")
		return nil
	}, 10*time.Second)
}
