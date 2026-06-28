package service

import (
	"testing"
	"time"

	"temporis/internal/model"
)

func TestPartitionsEqualDetectsTimerConfigChange(t *testing.T) {
	left := &model.Partition{
		ID: "partition-1",
		Timers: []*model.Timer{
			{ID: "timer-2", Partition: "partition-1", Interval: 2 * time.Second, Once: false},
			{ID: "timer-1", Partition: "partition-1", Interval: time.Second, Once: true},
		},
	}
	right := &model.Partition{
		ID: "partition-1",
		Timers: []*model.Timer{
			{ID: "timer-1", Partition: "partition-1", Interval: time.Second, Once: true},
			{ID: "timer-2", Partition: "partition-1", Interval: 2 * time.Second, Once: false},
		},
	}

	if !partitionsEqual(left, right) {
		t.Fatal("expected equivalent timer configs with different order to compare equal")
	}

	right.Timers[1].Interval = 3 * time.Second
	if partitionsEqual(left, right) {
		t.Fatal("expected interval change to require partition restart")
	}
}
