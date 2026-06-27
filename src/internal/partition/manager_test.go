package partition

import (
	"context"
	"sync"
	"testing"
	"time"

	"temporis/internal/model"
)

type mockTracker struct {
	hasFired     func(id string) bool
	claimFiring  func(id string, t time.Time) bool
	recordFiring func(id string, t time.Time) bool
}

func (m *mockTracker) HasFired(ctx context.Context, timerID string) bool {
	if m.hasFired != nil {
		return m.hasFired(timerID)
	}
	return false
}

func (m *mockTracker) ClaimFiring(ctx context.Context, timerID string, t time.Time) bool {
	if m.claimFiring != nil {
		return m.claimFiring(timerID, t)
	}
	return true
}

func (m *mockTracker) RecordFiring(ctx context.Context, timerID string, t time.Time) bool {
	if m.recordFiring != nil {
		return m.recordFiring(timerID, t)
	}
	return true
}

func TestManager_StartTimers_Once(t *testing.T) {
	callbackCalled := false
	var mu sync.Mutex

	timer := &model.Timer{
		ID:       "timer1",
		Interval: 10 * time.Millisecond,
		Once:     true,
		Callback: func() {
			mu.Lock()
			callbackCalled = true
			mu.Unlock()
		},
	}

	partition := &model.Partition{
		ID:     "part1",
		Timers: []*model.Timer{timer},
	}

	claimCount := 0
	tracker := &mockTracker{
		hasFired: func(id string) bool { return false },
		claimFiring: func(id string, tm time.Time) bool {
			mu.Lock()
			claimCount++
			mu.Unlock()
			return true
		},
	}

	m := NewManager(partition, tracker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.StartTimers(ctx)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if !callbackCalled {
		t.Errorf("Expected callback to be called")
	}
	if claimCount != 1 {
		t.Errorf("Expected ClaimFiring to be called once, got %d", claimCount)
	}
}

func TestManager_StartTimers_AlreadyFired(t *testing.T) {
	callbackCalled := false
	timer := &model.Timer{
		ID:       "timer1",
		Interval: 10 * time.Millisecond,
		Once:     true,
		Callback: func() {
			callbackCalled = true
		},
	}

	partition := &model.Partition{
		ID:     "part1",
		Timers: []*model.Timer{timer},
	}

	tracker := &mockTracker{
		hasFired: func(id string) bool { return true },
	}

	m := NewManager(partition, tracker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.StartTimers(ctx)

	time.Sleep(30 * time.Millisecond)

	if callbackCalled {
		t.Errorf("Expected callback to NOT be called since it already fired")
	}
}

func TestManager_StartTimers_Recurring(t *testing.T) {
	var mu sync.Mutex
	callbackCount := 0

	timer := &model.Timer{
		ID:       "timer1",
		Interval: 10 * time.Millisecond,
		Once:     false,
		Callback: func() {
			mu.Lock()
			callbackCount++
			mu.Unlock()
		},
	}

	partition := &model.Partition{
		ID:     "part1",
		Timers: []*model.Timer{timer},
	}

	tracker := &mockTracker{
		hasFired: func(id string) bool { return false },
	}

	m := NewManager(partition, tracker)
	ctx, cancel := context.WithCancel(context.Background())
	
	m.StartTimers(ctx)

	time.Sleep(35 * time.Millisecond)
	cancel()

	mu.Lock()
	count := callbackCount
	mu.Unlock()

	if count < 2 {
		t.Errorf("Expected callback to be called at least twice, got %d", count)
	}
}
