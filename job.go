package main

import (
	"sync"
	"time"
)

const (
	StateInvalid  byte = 0
	StateReady    byte = 1
	StateReserved byte = 2
	StateBuried   byte = 3
	StateDelayed  byte = 4
	StateCopy     byte = 5
)

func stateName(s byte) string {
	switch s {
	case StateReady:
		return "ready"
	case StateReserved:
		return "reserved"
	case StateBuried:
		return "buried"
	case StateDelayed:
		return "delayed"
	default:
		return "unknown"
	}
}

type Job struct {
	ID          uint64
	Pri         uint32
	Delay       time.Duration
	ScheduledAt time.Time // put-at: absolute wake time, resolved to Delay by enqueueJob
	TTR         time.Duration
	BodySize    int // size as reported to clients (without trailing \r\n)
	CreatedAt   time.Time
	DeadlineAt  time.Time
	ReserveCt   uint32
	TimeoutCt   uint32
	ReleaseCt   uint32
	BuryCt      uint32
	KickCt      uint32
	State       byte
	Tube        *Tube
	Body        []byte

	// For buried list (doubly-linked)
	buriedPrev *Job
	buriedNext *Job

	// For reserved list per connection (doubly-linked)
	resPrev *Job
	resNext *Job

	// Heap indices
	readyIdx int
	delayIdx int

	// Connection that reserved this job
	Reserver *Conn

	mu sync.Mutex
}

func (j *Job) age() time.Duration {
	return time.Since(j.CreatedAt)
}

func (j *Job) timeLeft() time.Duration {
	if j.State == StateReserved || j.State == StateDelayed {
		left := time.Until(j.DeadlineAt)
		if left < 0 {
			return 0
		}
		return left
	}
	return 0
}
