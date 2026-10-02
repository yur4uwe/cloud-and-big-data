package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yur4uwe/cloud/8-future/future"
)

func main() {
	ctx := context.Background()

	fmt.Println("=== 1. Basic Async Computation & Retrieval ===")
	f1 := future.Submit(ctx, func() (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "Hello from the future!", nil
	})

	val, err := f1.Get(100 * time.Millisecond)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Received: %q\n\n", val)
	}

	fmt.Println("=== 2. Fluent Chaining with Error Recovery ===")
	// Step 1: Submit (fails with an error)
	// Step 2: Then (skipped due to error)
	// Step 3: Catch (catches the error and recovers with a fallback value)
	// Step 4: Then (executes normally with recovered value)
	f2 := future.Submit(ctx, func() (int, error) {
		fmt.Println("[Step 1] Initial task failing...")
		return 0, errors.New("primary service unavailable")
	}).Then(func(v int) (int, error) {
		fmt.Println("[Step 2] This will be skipped")
		return v * 10, nil
	}).Catch(func(err error) (int, error) {
		fmt.Printf("[Step 3] Caught error: %q. Recovering with fallback value 42\n", err)
		return 42, nil
	}).Then(func(v int) (int, error) {
		fmt.Printf("[Step 4] Resuming pipeline with: %d, adding 8\n", v)
		return v + 8, nil
	})

	finalVal, err := f2.Await()
	fmt.Printf("Final pipeline result: %d (err: %v)\n\n", finalVal, err)

	fmt.Println("=== 3. Timeout Handling with Get() ===")
	slowFuture := future.Submit(ctx, func() (string, error) {
		time.Sleep(150 * time.Millisecond)
		return "Done slowly", nil
	})

	_, err = slowFuture.Get(30 * time.Millisecond)
	fmt.Printf("Get(30ms) on slow task result: %v\n\n", err)

	fmt.Println("=== 4. Combinator: All() ===")
	p1 := future.Submit(ctx, func() (string, error) {
		time.Sleep(20 * time.Millisecond)
		return "UserData", nil
	})
	p2 := future.Submit(ctx, func() (string, error) {
		time.Sleep(40 * time.Millisecond)
		return "OrderData", nil
	})
	p3 := future.Submit(ctx, func() (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "PaymentData", nil
	})

	all := future.All(p1, p2, p3)
	allResults, err := all.Get(200 * time.Millisecond)
	if err != nil {
		fmt.Printf("All() error: %v\n", err)
	} else {
		fmt.Printf("All() gathered %d results: %v\n\n", len(allResults), allResults)
	}

	fmt.Println("=== 5. Combinator: Race() ===")
	mirrorA := future.Submit(ctx, func() (string, error) {
		time.Sleep(80 * time.Millisecond)
		return "Mirror A (slow)", nil
	})
	mirrorB := future.Submit(ctx, func() (string, error) {
		time.Sleep(15 * time.Millisecond)
		return "Mirror B (fast)", nil
	})

	winner, err := future.Race(mirrorA, mirrorB).Get(200 * time.Millisecond)
	if err != nil {
		fmt.Printf("Race() error: %v\n", err)
	} else {
		fmt.Printf("Race() winner: %q\n", winner)
	}
}
