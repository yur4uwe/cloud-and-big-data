package fanin

import (
	"context"
	"fmt"
	"sync"
)

type ErrorPolicy int

const (
	PolicyResilient ErrorPolicy = iota
	PolicyFailFast
)

type Result[T any] struct {
	Value T
	Err   error
}

func FanIn[T any](ctx context.Context, channels ...<-chan Result[T]) <-chan Result[T] {
	out := make(chan Result[T])
	var wg sync.WaitGroup

	forward := func(ch <-chan Result[T]) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case val, opened := <-ch:
				if !opened {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}
	}

	wg.Add(len(channels))
	for _, ch := range channels {
		go forward(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

type Consumer[T any] struct {
	ctx      context.Context
	receiver <-chan Result[T]
	policy   ErrorPolicy
	cancel   context.CancelFunc
}

func (c *Consumer[T]) ConsumeAll() {
	for {
		select {
		case res, open := <-c.receiver:
			if !open {
				return
			}
			if res.Err != nil {
				switch c.policy {
				case PolicyFailFast:
					c.cancel()
					return
				case PolicyResilient:
					fmt.Println("[ERROR]", res.Err)
					continue
				}
			}
			fmt.Println("[VALUE]", res.Value)
		case <-c.ctx.Done():
			return
		}
	}
}

func NewConsumer[T any](ctx context.Context, receiver <-chan Result[T], policy ErrorPolicy, ctxCancel context.CancelFunc) *Consumer[T] {
	return &Consumer[T]{ctx: ctx, receiver: receiver, policy: policy, cancel: ctxCancel}
}

type Producer[T any] struct {
	ctx    context.Context
	sender chan<- Result[T]
}

func (p *Producer[T]) Emit(v T) {
	select {
	case <-p.ctx.Done():
		return
	case p.sender <- Result[T]{Value: v}:
	}
}

func (p *Producer[T]) EmitErr(err error) {
	select {
	case <-p.ctx.Done():
		return
	case p.sender <- Result[T]{Err: err}:
	}
}

func NewProducer[T any](ctx context.Context, work func(p *Producer[T])) <-chan Result[T] {
	ch := make(chan Result[T])
	p := &Producer[T]{ctx: ctx, sender: ch}
	go func() {
		defer close(ch)
		work(p)
	}()
	return ch
}
