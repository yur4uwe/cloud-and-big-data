package debounce

import (
	"sync"
	"time"
)

type DebounceOpts struct {
	Leading  bool
	Trailing bool
}

func Debounce(fn func(), delay time.Duration, opts ...DebounceOpts) (debounced func(), dispose func()) {
	opt := DebounceOpts{Trailing: true}
	if len(opts) > 0 {
		opt = opts[0]
	}

	var (
		mu           sync.Mutex
		timer        *time.Timer
		trailingCall bool
	)

	debounced = func() {
		mu.Lock()
		shouldCallLeading := timer == nil && opt.Leading

		if !shouldCallLeading {
			trailingCall = true
		}

		if timer != nil {
			timer.Stop()
		}

		timer = time.AfterFunc(delay, func() {
			mu.Lock()
			shouldCallTrailing := opt.Trailing && trailingCall
			timer = nil
			trailingCall = false
			mu.Unlock()

			if shouldCallTrailing {
				fn()
			}
		})
		mu.Unlock()

		if shouldCallLeading {
			fn()
		}
	}

	dispose = func() {
		mu.Lock()
		defer mu.Unlock()

		if timer != nil {
			timer.Stop()
			timer = nil
		}
		trailingCall = false
	}

	return debounced, dispose
}
