package services

import (
	"context"
	"sync/atomic"
	"time"
)

type MockMode int

const (
	ModeSuccess MockMode = iota
	ModeFail
	ModeHang
)

type MockService struct {
	callCount atomic.Int64
}

func NewMockService() *MockService {
	return &MockService{}
}

func (m *MockService) CallCount() int64 {
	return m.callCount.Load()
}

func (m *MockService) ResetCallCount() {
	m.callCount.Store(0)
}

type Payload string

func (m *MockService) SuccessCall(ctx context.Context) (Payload, error) {
	m.callCount.Add(1)
	return "ok", nil
}

func (m *MockService) FailWith(err error) func(context.Context) (Payload, error) {
	return func(ctx context.Context) (Payload, error) {
		m.callCount.Add(1)
		return "", err
	}
}

func (m *MockService) HangFor(d time.Duration) func(context.Context) (Payload, error) {
	return func(ctx context.Context) (Payload, error) {
		m.callCount.Add(1)
		timer := time.NewTimer(d)
		defer timer.Stop()

		select {
		case <-timer.C:
			return "ok", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}
