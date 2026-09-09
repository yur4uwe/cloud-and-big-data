package services

import (
	"context"
	"sync"
	"time"
)

type MockMode int

const (
	ModeSuccess MockMode = iota
	ModeFail
	ModeHang
)

type MockService struct {
	mu           sync.Mutex
	mode         MockMode
	errToReturn  error
	hangDuration time.Duration
	callCount    int
}

func NewMockService() *MockService {
	return &MockService{
		mode:        ModeSuccess,
		errToReturn: ErrInternal,
	}
}

func (m *MockService) SetMode(mode MockMode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = mode
}

func (m *MockService) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = ModeFail
	m.errToReturn = err
}

func (m *MockService) SetHangDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = ModeHang
	m.hangDuration = d
}

func (m *MockService) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func (m *MockService) ResetCallCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount = 0
}

func (m *MockService) Call(ctx context.Context) (string, error) {
	m.mu.Lock()
	m.callCount++
	mode := m.mode
	errToReturn := m.errToReturn
	hangDuration := m.hangDuration
	m.mu.Unlock()

	switch mode {
	case ModeSuccess:
		return "ok", nil
	case ModeFail:
		return "", errToReturn
	case ModeHang:
		select {
		case <-time.After(hangDuration):
			return "ok after hang", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	default:
		return "ok", nil
	}
}
