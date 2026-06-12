package partition

import (
	"context"
	"sync"
	"testing"
	"time"

	"temporis/internal/model"
)

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

	m := NewManager(partition, func(id string) bool { return false })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firedCount := 0
	recordFiring := func(id string, tm time.Time) bool {
		mu.Lock()
		firedCount++
		mu.Unlock()
		return true
	}

	m.StartTimers(ctx, recordFiring)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if !callbackCalled {
		t.Errorf("Expected callback to be called")
	}
	if firedCount != 1 {
		t.Errorf("Expected firedCount to be 1, got %d", firedCount)
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

	m := NewManager(partition, func(id string) bool { return true }) // already fired
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.StartTimers(ctx, func(id string, tm time.Time) bool { return true })

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

	m := NewManager(partition, func(id string) bool { return false })
	ctx, cancel := context.WithCancel(context.Background())
	
	m.StartTimers(ctx, func(id string, tm time.Time) bool { return true })

	time.Sleep(35 * time.Millisecond)
	cancel() // Stop timers

	mu.Lock()
	count := callbackCount
	mu.Unlock()

	if count < 2 {
		t.Errorf("Expected callback to be called at least twice, got %d", count)
	}
}
