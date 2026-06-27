package model

import (
	"testing"
)

func TestNewPartition(t *testing.T) {
	timers := []*Timer{
		{ID: "timer1"},
	}
	p := NewPartition("part1", timers)

	if p.ID != "part1" {
		t.Errorf("expected part1, got %v", p.ID)
	}
	if len(p.Timers) != 1 || p.Timers[0].ID != "timer1" {
		t.Errorf("expected 1 timer with ID timer1, got %v", p.Timers)
	}
}
