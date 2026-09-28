package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yur4uwe/cloud/3-retry/retry"
)

func main() {
	fmt.Println("=== Retry Pattern Demo ===")

	var attempts int
	err := retry.Execute(context.Background(), func(ctx context.Context) error {
		attempts++
		fmt.Printf("Attempt %d: executing operation...\n", attempts)
		if attempts < 3 {
			fmt.Printf("Attempt %d failed (transient error), retrying...\n", attempts)
			return errors.New("network timeout")
		}
		fmt.Println("Attempt succeeded!")
		return nil
	}, retry.RetryOptions{
		MaxAttempts: 4,
		Strategy:    retry.RetryExponential,
		Step:        100 * time.Millisecond,
		Timeout:     1 * time.Second,
	})

	if err != nil {
		fmt.Printf("Operation ultimately failed: %v\n", err)
	} else {
		fmt.Println("Operation completed successfully!")
	}
}
