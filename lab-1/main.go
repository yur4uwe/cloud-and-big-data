package main

import (
	"context"
	"fmt"
	"time"

	circuitbreaker "github.com/yur4uwe/cloud/lab-1/circuit-breaker"
	"github.com/yur4uwe/cloud/lab-1/services"
)

func stateName(s circuitbreaker.CBState) string {
	switch s {
	case circuitbreaker.StateClosed:
		return "CLOSED (Healthy)"
	case circuitbreaker.StateOpen:
		return "OPEN (Tripped)"
	case circuitbreaker.StateHalfOpen:
		return "HALF-OPEN (Trial)"
	default:
		return "UNKNOWN"
	}
}

func main() {
	fmt.Println("=========================================================")
	fmt.Println("       CIRCUIT BREAKER DEMONSTRATION (Lab 1.1)           ")
	fmt.Println("=========================================================")

	cfg := circuitbreaker.CircuitBreakerConfig{
		FailureThreshold:  3,
		HalfOpenMaxCalls:  2,
		OpenStateDuration: 1500 * time.Millisecond,
		TimeoutPerCall:    200 * time.Millisecond,
	}
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	callService := func(stepName string) {
		res, err := circuitbreaker.Call(ctx, cb, mock.Call)
		if err != nil {
			fmt.Printf("[%-18s] Call Result: ❌ ERROR: %-28v | State: %s\n", stepName, err, stateName(cb.State()))
		} else {
			fmt.Printf("[%-18s] Call Result: ✅ SUCCESS: %-26s | State: %s\n", stepName, res, stateName(cb.State()))
		}
	}

	// 1. Normal Healthy Operation
	fmt.Println("\n--- Stage 1: Healthy Calls (Closed State) ---")
	mock.SetMode(services.ModeSuccess)
	callService("Normal Call 1")
	callService("Normal Call 2")

	// 2. Service Outage & Failures
	fmt.Println("\n--- Stage 2: Service Outage (Reaching FailureThreshold = 3) ---")
	mock.SetMode(services.ModeFail)
	callService("Failing Call 1")
	callService("Failing Call 2")
	callService("Failing Call 3")

	// 3. Fast Failures in Open State
	fmt.Println("\n--- Stage 3: Fast-Fail in Open State (Zero traffic to backend) ---")
	callService("Fast-Fail Call 1")
	callService("Fast-Fail Call 2")

	// 4. Cooldown Wait
	fmt.Printf("\n--- Stage 4: Cooldown period (%v) ---\n", cfg.OpenStateDuration)
	fmt.Println("Waiting for backend recovery...")
	time.Sleep(cfg.OpenStateDuration + 100*time.Millisecond)
	mock.SetMode(services.ModeSuccess) // Backend is now recovered

	// 5. Half-Open Trial Probes
	fmt.Println("\n--- Stage 5: Half-Open Probes (HalfOpenMaxCalls = 2) ---")
	callService("Probe Call 1")
	callService("Probe Call 2")

	// 6. Resumed Normal Operation
	fmt.Println("\n--- Stage 6: Fully Restored (Closed State) ---")
	callService("Restored Call 1")
	callService("Restored Call 2")

	fmt.Println("\n=========================================================")
	fmt.Println("       DEMONSTRATION COMPLETED SUCCESSFULLY              ")
	fmt.Println("=========================================================")
}
