package main

import (
	"sync"
	"time"
)

type TubeStats struct {
	UrgentCt    int64
	ReadyCt     int64
	ReservedCt  int64
	DelayedCt   int64
	BuriedCt    int64
	TotalJobsCt uint64
	UsingCt     int64
	WatchingCt  int64
	WaitingCt   int64
	PauseCt     uint64
	DeleteCt    uint64
	PauseTubeCt uint64

	// WaitHist samples how long each job that got reserved out of this
	// tube had spent sitting ready beforehand (time.Since(j.ReadyAt) at
	// reservation time). Exposed per tube via /metrics as
	// beanstalkd_tube_ready_wait_seconds - the aggregate that stats-job's
	// per-job "age" has no equivalent of.
	WaitHist Histogram
}

type Tube struct {
	Name         string
	Ready        *Heap[*Job]
	Delay        *Heap[*Job]
	BuriedHead   *Job // doubly-linked circular list sentinel
	WaitingConns []*Conn

	Stat      TubeStats
	Pause     time.Duration
	UnpauseAt time.Time

	// Dead-letter routing (set-dlq extension command; see checkDeadLetter
	// in server.go). MaxAttempts == 0 means disabled. Neither field is
	// persisted - like Pause, it resets to disabled on restart.
	MaxAttempts    uint32
	DeadLetterTube string

	// Tombstoned is set by delete-tube: the tube's ready, delayed, and
	// buried jobs have already been purged, and any job still reserved
	// by another connection at the time is dropped (not re-enqueued)
	// once that connection releases, buries, times out, or disconnects,
	// so a stale in-flight job can't resurrect a deleted tube. A fresh
	// put clears the flag, since the client is deliberately using the
	// tube again.
	Tombstoned bool

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

// addWaiter and removeWaiter maintain WaitingConns, the set of
// connections currently blocked in reserve watching t - consulted by
// Server.wakeWaitersForTube wherever a job in t turns Ready, so a
// blocked reserve is satisfied immediately instead of on tick's next
// 100ms pass. Order doesn't matter, so removeWaiter swaps in the last
// element rather than shifting. Caller must hold Server.mu.
func (t *Tube) addWaiter(c *Conn) {
	t.WaitingConns = append(t.WaitingConns, c)
}

func (t *Tube) removeWaiter(c *Conn) {
	for i, wc := range t.WaitingConns {
		if wc == c {
			last := len(t.WaitingConns) - 1
			t.WaitingConns[i] = t.WaitingConns[last]
			t.WaitingConns[last] = nil
			t.WaitingConns = t.WaitingConns[:last]
			return
		}
	}
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
