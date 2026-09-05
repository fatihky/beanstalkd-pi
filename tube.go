package main

import (
	"sync"
	"time"
)

type TubeStats struct {
	UrgentCt     int64
	ReadyCt      int64
	ReservedCt   int64
	DelayedCt    int64
	BuriedCt     int64
	TotalJobsCt  uint64
	UsingCt      int64
	WatchingCt   int64
	WaitingCt    int64
	PauseCt      uint64
	DeleteCt     uint64
	PauseTubeCt  uint64
}

type Tube struct {
	Name      string
	Ready     *Heap[*Job]
	Delay     *Heap[*Job]
	BuriedHead *Job // doubly-linked circular list sentinel
	WaitingConns []*Conn

	Stat       TubeStats
	Pause      time.Duration
	UnpauseAt  time.Time

	mu sync.Mutex
}

func NewTube(name string) *Tube {
	t := &Tube{
		Name: name,
	}
	t.Ready = NewHeap[*Job](jobPriLess, func(j *Job, idx int) {
		j.readyIdx = idx
	})
	t.Delay = NewHeap[*Job](jobDelayLess, func(j *Job, idx int) {
		j.delayIdx = idx
	})
	// Initialize buried list sentinel
	t.BuriedHead = &Job{}
	t.BuriedHead.buriedPrev = t.BuriedHead
	t.BuriedHead.buriedNext = t.BuriedHead
	return t
}

func jobPriLess(a, b *Job) bool {
	if a.Pri != b.Pri {
		return a.Pri < b.Pri
	}
	return a.ID < b.ID
}

func jobDelayLess(a, b *Job) bool {
	if a.DeadlineAt != b.DeadlineAt {
		return a.DeadlineAt.Before(b.DeadlineAt)
	}
	return a.ID < b.ID
}

func (t *Tube) buryPush(j *Job) {
	// Insert at tail of buried list (FIFO)
	head := t.BuriedHead
	prev := head.buriedPrev
	prev.buriedNext = j
	j.buriedPrev = prev
	j.buriedNext = head
	head.buriedPrev = j
}

func (t *Tube) buryRemove(j *Job) {
	if j.buriedPrev == nil && j.buriedNext == nil {
		return
	}
	j.buriedPrev.buriedNext = j.buriedNext
	j.buriedNext.buriedPrev = j.buriedPrev
	j.buriedPrev = nil
	j.buriedNext = nil
}

func (t *Tube) buriedLen() int {
	count := 0
	for j := t.BuriedHead.buriedNext; j != t.BuriedHead; j = j.buriedNext {
		count++
	}
	return count
}

func (t *Tube) isPaused(now time.Time) bool {
	if t.Pause == 0 {
		return false
	}
	if now.After(t.UnpauseAt) || now.Equal(t.UnpauseAt) {
		t.Pause = 0
		return false
	}
	return true
}
