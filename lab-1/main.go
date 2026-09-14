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
	fmt.Println("CIRCUIT BREAKER DEMO")

	cfg := circuitbreaker.CircuitBreakerConfig{
		FailureThreshold:  3,
		HalfOpenMaxCalls:  2,
		OpenStateDuration: 1500 * time.Millisecond,
		TimeoutPerCall:    200 * time.Millisecond,
	}
	cb := circuitbreaker.NewCircuitBreaker(cfg)
	mock := services.NewMockService()
	ctx := context.Background()

	callService := func(stepName string, callback func(context.Context) (services.Payload, error)) {
		res, err := circuitbreaker.Call(ctx, cb, callback)
		if err != nil {
			fmt.Printf("[%-18s] Call Result: ERROR: %-28v | State: %s\n", stepName, err, stateName(cb.State()))
		} else {
			fmt.Printf("[%-18s] Call Result: SUCCESS: %-26s | State: %s\n", stepName, res, stateName(cb.State()))
		}
	}

	// 1. Normal Healthy Operation
	fmt.Println("\n--- Stage 1: Healthy Calls (Closed State) ---")
	callService("Normal Call 1", mock.SuccessCall)
	callService("Normal Call 2", mock.SuccessCall)

	fmt.Println("\n--- Stage 2: Service Outage (Reaching FailureThreshold = 3) ---")
	callback := mock.FailWith(services.ErrInternal)
	callService("Failing Call 1", callback)
	callService("Failing Call 2", callback)
	callService("Failing Call 3", callback)

	fmt.Println("\n--- Stage 3: Fast-Fail in Open State (Zero traffic to backend) ---")
	callback = mock.SuccessCall
	callService("Fast-Fail Call 1", callback)
	callService("Fast-Fail Call 2", callback)

	fmt.Printf("\n--- Stage 4: Cooldown period (%v) ---\n", cfg.OpenStateDuration)
	fmt.Println("Waiting for backend recovery...")
	time.Sleep(cfg.OpenStateDuration + 100*time.Millisecond)

	fmt.Println("\n--- Stage 5: Half-Open Probes (HalfOpenMaxCalls = 2) ---")
	callService("Probe Call 1", callback)
	callService("Probe Call 2", callback)

	fmt.Println("\n--- Stage 6: Fully Restored (Closed State) ---")
	callService("Restored Call 1", callback)
	callService("Restored Call 2", callback)
}
