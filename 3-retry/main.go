package main

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type RetryStrategy int8

const (
	RetryConstant RetryStrategy = iota
	RetryExponential
	RetryExponentialJitter
)

func GetBackoff(strategy RetryStrategy, attempt int, step time.Duration) time.Duration {
	switch strategy {
	case RetryConstant:
		return time.Duration(attempt) * step
	case RetryExponential:
		return time.Duration(math.Pow(2, float64(attempt))) * step
	case RetryExponentialJitter:
		return time.Duration(math.Pow(2, float64(attempt))) * step * time.Duration(rand.Intn(100))
	}
	return 0
}

type RetryOptions struct {
	MaxAttempts int
	Strategy    RetryStrategy
	RetryOn     func(error) bool
	Step        time.Duration
	Timeout     time.Duration
}

func Execute(ctx context.Context, callback func(ctx context.Context) error, options ...RetryOptions) error {
	opts := RetryOptions{
		MaxAttempts: 3,
		Strategy:    RetryConstant,
		RetryOn:     func(error) bool { return true },
		Step:        time.Second,
		Timeout:     30 * time.Second,
	}
	if len(options) > 0 {
		opts = options[0]
		if opts.MaxAttempts < 1 {
			opts.MaxAttempts = 1
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var lastErr error
	for att := range opts.MaxAttempts {
		childCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

		lastErr = callback(childCtx)
		if lastErr == nil || !opts.RetryOn(lastErr) {
			cancel()
			return lastErr
		}
		cancel()

		if att == opts.MaxAttempts-1 {
			return lastErr
		}

		backoff := GetBackoff(opts.Strategy, att, opts.Step)
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}

	return lastErr
}
