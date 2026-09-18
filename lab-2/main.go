package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type DebounceOpts struct {
	Leading  bool
	Trailing bool
}

type DebouncedFunc struct {
	mu           sync.Mutex
	timer        *time.Timer
	fn           func()
	delay        time.Duration
	leading      bool
	trailing     bool
	trailingCall bool
}

func debounce(fn func(), delay time.Duration, opts ...DebounceOpts) *DebouncedFunc {
	opt := DebounceOpts{Trailing: true}
	if len(opts) > 0 {
		opt = opts[0]
	}
	return &DebouncedFunc{
		fn:       fn,
		delay:    delay,
		leading:  opt.Leading,
		trailing: opt.Trailing,
	}
}

// Call schedules the debounced function to be called after the delay
func (d *DebouncedFunc) Call() {
	d.mu.Lock()
	shouldCallLeading := d.timer == nil && d.leading

	if !shouldCallLeading {
		d.trailingCall = true
	}

	if d.timer != nil {
		d.timer.Stop()
	}

	d.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		shouldCallTrailing := d.trailing && d.trailingCall
		d.timer = nil
		d.trailingCall = false
		d.mu.Unlock()

		if shouldCallTrailing {
			d.fn()
		}
	})
	d.mu.Unlock()

	if shouldCallLeading {
		d.fn()
	}
}

// Dispose cancels any scheduled execution and cleans up the timer
func (d *DebouncedFunc) Dispose() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.trailingCall = false
}

func main() {
	debouncer := debounce(func() {
		fmt.Println("hello")
	}, time.Second)
	defer debouncer.Dispose()

	s := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">")
		s, err := s.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		if strings.TrimSpace(s) == "exit" {
			break
		} else {
			debouncer.Call()
		}
	}

	fmt.Println("bye")
}
