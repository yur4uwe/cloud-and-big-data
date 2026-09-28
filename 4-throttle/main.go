package main

import (
	"fmt"
	"time"

	"github.com/yur4uwe/cloud/4-throttle/throttle"
)

func main() {
	fmt.Println("=== 1. Demonstration of Drop Mode (Capacity: 3, Refill: 1 token/sec) ===")
	droppedThrottled, dispose1 := throttle.Throttle(func() {
		fmt.Printf("[%s] Call executed (Drop mode)\n", time.Now().Format("15:04:05.000"))
	}, throttle.ThrottleOpts{
		Capacity:   3,
		RefillRate: 1,
		Mode:       throttle.ModeDrop,
		Leading:    true,
	})
	defer dispose1()

	for i := range 6 {
		ok := droppedThrottled()
		if !ok {
			fmt.Printf("[%s] Call #%d rejected (no tokens)\n", time.Now().Format("15:04:05.000"), i+1)
		}
		time.Sleep(150 * time.Millisecond)
	}

	fmt.Println("\nWaiting 2 seconds for token refill...")
	time.Sleep(2 * time.Second)

	for i := range 3 {
		ok := droppedThrottled()
		if !ok {
			fmt.Printf("[%s] Call #%d rejected\n", time.Now().Format("15:04:05.000"), i+6+1)
		}
	}

	fmt.Println("\n=== 2. Demonstration of Queue Mode (Capacity: 2, Refill: 2 tokens/sec) ===")
	queuedThrottled, dispose2 := throttle.Throttle(func() {
		fmt.Printf("[%s] Queued task executed\n", time.Now().Format("15:04:05.000"))
	}, throttle.ThrottleOpts{
		Capacity:   2,
		RefillRate: 2,
		Mode:       throttle.ModeQueue,
	})
	defer dispose2()

	for i := range 5 {
		fmt.Printf("[%s] Enqueueing task #%d\n", time.Now().Format("15:04:05.000"), i+1)
		queuedThrottled()
	}

	time.Sleep(2 * time.Second)
	fmt.Println("Completed.")
}
