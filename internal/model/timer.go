package model

import "time"

// Timer represents a timer configuration with either one-time or recurring execution.
type Timer struct {
	ID        string        // Unique identifier for the timer
	Partition string        // Partition ID this timer belongs to
	Interval  time.Duration // Interval for recurring timers or delay for one-time timers
	Once      bool          // True if one-time, false if recurring
	Callback  func()        // Function to execute when the timer fires
}
