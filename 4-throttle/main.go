package main

import "time"

type ThrottleOpts struct {
	// The number of requests to allow before throttling.
	MaxRequests int
	// The number of seconds to wait before allowing another request.
	Duration time.Duration
}
