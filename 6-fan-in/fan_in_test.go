package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMergeOrderPreservedPerStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const countPerStream = 50

	s1 := NewProducer(ctx, func(p *Producer[string]) {
		for i := range countPerStream {
			p.Emit(fmt.Sprintf("S1-%02d", i))
			time.Sleep(time.Millisecond)
		}
	})

	s2 := NewProducer(ctx, func(p *Producer[string]) {
		for i := range countPerStream {
			p.Emit(fmt.Sprintf("S2-%02d", i))
			time.Sleep(time.Millisecond)
		}
	})

	s3 := NewProducer(ctx, func(p *Producer[string]) {
		for i := range countPerStream {
			p.Emit(fmt.Sprintf("S3-%02d", i))
			time.Sleep(time.Millisecond)
		}
	})

	merged := FanIn(ctx, s1, s2, s3)

	var s1Received, s2Received, s3Received []string
	var totalCount int

	for res := range merged {
		if res.Err != nil {
			t.Fatalf("unexpected error in stream: %v", res.Err)
		}
		totalCount++
		val := res.Value
		switch {
		case len(val) >= 2 && val[:2] == "S1":
			s1Received = append(s1Received, val)
		case len(val) >= 2 && val[:2] == "S2":
			s2Received = append(s2Received, val)
		case len(val) >= 2 && val[:2] == "S3":
			s3Received = append(s3Received, val)
		default:
			t.Fatalf("unexpected value from unknown stream: %s", val)
		}
	}

	expectedTotal := countPerStream * 3
	if totalCount != expectedTotal {
		t.Fatalf("expected total %d items, got %d", expectedTotal, totalCount)
	}

	verifyOrder := func(streamName string, received []string) {
		if len(received) != countPerStream {
			t.Fatalf("%s: expected %d items, got %d", streamName, countPerStream, len(received))
		}
		for i := range countPerStream {
			expected := fmt.Sprintf("%s-%02d", streamName, i)
			if received[i] != expected {
				t.Fatalf("%s: order violation at index %d: expected %s, got %s", streamName, i, expected, received[i])
			}
		}
	}

	verifyOrder("S1", s1Received)
	verifyOrder("S2", s2Received)
	verifyOrder("S3", s3Received)
}

func TestCompletionAfterAllInputsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s1 := NewProducer(ctx, func(p *Producer[int]) {
		p.Emit(1)
		p.Emit(2)
	})

	s2 := NewProducer(ctx, func(p *Producer[int]) {
		time.Sleep(50 * time.Millisecond)
		p.Emit(10)
	})

	s3 := NewProducer(ctx, func(p *Producer[int]) {
		time.Sleep(100 * time.Millisecond)
		p.Emit(100)
	})

	merged := FanIn(ctx, s1, s2, s3)

	var received []int
	for res := range merged {
		if res.Err != nil {
			t.Fatalf("unexpected error: %v", res.Err)
		}
		received = append(received, res.Value)
	}

	if len(received) != 4 {
		t.Fatalf("expected 4 total items from all inputs, got %d", len(received))
	}

	select {
	case _, open := <-merged:
		if open {
			t.Fatal("expected merged channel to be closed, but it was still open")
		}
	default:
		t.Fatal("expected reading from closed channel to be non-blocking")
	}
}

func TestPolicyResilient_ErrorsDoNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errFault := errors.New("simulated sensor error")

	failingStream := NewProducer(ctx, func(p *Producer[string]) {
		p.EmitErr(errFault)
		p.Emit("Recovered-Item-1")
		p.EmitErr(errFault)
	})

	healthyStream := NewProducer(ctx, func(p *Producer[string]) {
		for i := 1; i <= 5; i++ {
			p.Emit(fmt.Sprintf("Healthy-%d", i))
			time.Sleep(5 * time.Millisecond)
		}
	})

	merged := FanIn(ctx, failingStream, healthyStream)

	var (
		mu             sync.Mutex
		errorsCount    int
		healthyCount   int
		recoveredCount int
	)

	for res := range merged {
		if res.Err != nil {
			mu.Lock()
			errorsCount++
			mu.Unlock()
			continue
		}

		mu.Lock()
		if res.Value == "Recovered-Item-1" {
			recoveredCount++
		} else {
			healthyCount++
		}
		mu.Unlock()
	}

	if errorsCount != 2 {
		t.Errorf("expected 2 errors from failing stream, got %d", errorsCount)
	}
	if healthyCount != 5 {
		t.Errorf("expected 5 items from healthy stream, got %d", healthyCount)
	}
	if recoveredCount != 1 {
		t.Errorf("expected 1 recovered item from failing stream, got %d", recoveredCount)
	}
}

func TestPolicyFailFast_CancelsCleanlyWithoutDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errFatal := errors.New("fatal upstream error")

	s1 := NewProducer(ctx, func(p *Producer[int]) {
		p.EmitErr(errFatal)
	})

	s2 := NewProducer(ctx, func(p *Producer[int]) {
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				p.Emit(i)
				time.Sleep(10 * time.Millisecond)
			}
		}
	})

	merged := FanIn(ctx, s1, s2)
	consumer := NewConsumer(ctx, merged, PolicyFailFast, cancel)

	done := make(chan struct{})
	go func() {
		consumer.ConsumeAll()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("deadlock/timeout: FailFast consumer did not terminate in time")
	}

	if ctx.Err() == nil {
		t.Fatal("expected context to be cancelled under FailFast policy")
	}
}
