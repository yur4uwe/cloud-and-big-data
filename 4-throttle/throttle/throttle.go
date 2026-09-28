package throttle

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	tokens     float64
	refillRate float64
	lastRefill time.Time
}

func NewTokenBucket(capacity float64, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refill(now time.Time) {
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now
}

func (tb *TokenBucket) Allow(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.refill(now)

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

func (tb *TokenBucket) Wait(ctx context.Context, n float64) error {
	for {
		tb.mu.Lock()
		now := time.Now()
		tb.refill(now)

		if tb.tokens >= n {
			tb.tokens -= n
			tb.mu.Unlock()
			return nil
		}

		needed := n - tb.tokens
		waitTime := time.Duration((needed / tb.refillRate) * float64(time.Second))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}

type ThrottleMode int

const (
	ModeDrop ThrottleMode = iota
	ModeQueue
)

type ThrottleOpts struct {
	Capacity   float64
	RefillRate float64
	Mode       ThrottleMode
	Leading    bool
	Trailing   bool
}

func Throttle(fn func(), opts ...ThrottleOpts) (throttled func() bool, dispose func()) {
	opt := ThrottleOpts{
		Capacity:   3,
		RefillRate: 1,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   true,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}
	tb := NewTokenBucket(opt.Capacity, opt.RefillRate)

	var (
		mu            sync.Mutex
		trailingCall  bool
		trailingTimer *time.Timer
		closed        bool
	)

	interval := time.Duration(float64(time.Second) / opt.RefillRate)

	if opt.Mode == ModeQueue {
		queueCap := int(opt.Capacity)*2 + 20
		queue := make(chan func(), queueCap)
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-queue:
					if !ok {
						return
					}
					if err := tb.Wait(ctx, 1); err != nil {
						return
					}
					task()
				}
			}
		}()

		throttled = func() bool {
			mu.Lock()
			if closed {
				mu.Unlock()
				return false
			}
			mu.Unlock()

			select {
			case queue <- fn:
				return true
			default:
				return false
			}
		}

		dispose = func() {
			mu.Lock()
			if !closed {
				closed = true
				cancel()
				close(queue)
			}
			mu.Unlock()
		}

		return throttled, dispose
	}

	dispose = func() {
		mu.Lock()
		defer mu.Unlock()
		closed = true
		if trailingTimer != nil {
			trailingTimer.Stop()
			trailingTimer = nil
		}
		trailingCall = false
	}

	var scheduleTrailing func()
	scheduleTrailing = func() {
		trailingTimer = time.AfterFunc(interval, func() {
			mu.Lock()
			if closed || !opt.Trailing || !trailingCall {
				trailingCall = false
				trailingTimer = nil
				mu.Unlock()
				return
			}

			if tb.Allow(1) {
				trailingCall = false
				trailingTimer = nil
				mu.Unlock()
				fn()
			} else {
				scheduleTrailing()
				mu.Unlock()
			}
		})
	}

	throttled = func() bool {
		mu.Lock()
		defer mu.Unlock()

		if closed {
			return false
		}

		if !opt.Leading && !opt.Trailing {
			return false
		}

		hasToken := tb.Allow(1)
		if hasToken {
			if opt.Leading {
				fn()
				return true
			}
			if opt.Trailing {
				trailingCall = true
				if trailingTimer == nil {
					scheduleTrailing()
				}
				return true
			}
			return true
		}

		if opt.Trailing {
			trailingCall = true
			if trailingTimer == nil {
				scheduleTrailing()
			}
			return true
		}

		return false
	}

	return throttled, dispose
}
