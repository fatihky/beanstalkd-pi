package main

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type ConnState int

const (
	StateWantCommand ConnState = iota
	StateWantData
	StateSendJob
	StateSendWord
	StateBitBucket
	StateClose
	StateWantEndLine
)

type Conn struct {
	ID       uint64
	Conn     net.Conn
	Server   *Server
	UseTube  *Tube
	WatchMap map[*Tube]bool

	// Reserved jobs doubly-linked list sentinel
	resHead Job

	// Incoming job being read
	inJob     *Job
	inJobRead int // bytes read so far of body+2

	// Outgoing data
	outBuf []byte

	// Reserve timeout
	hasTimeout bool
	timeoutAt  time.Time

	state  ConnState
	cmdBuf []byte
	reader *bufio.Reader

	halfClosed bool
	isProducer bool
	isWorker   bool
	isWaiting  bool

	soonestJob *Job // cached earliest deadline reserved job

	// wakeCh is signaled (non-blocking, buffered 1) whenever a job in a
	// tube this connection watches turns Ready while it's blocked in
	// reserve - see wake() and Server.wakeWaitersForTube. A blocked
	// reserve's own goroutine selects on it in waitForReserve instead of
	// waiting for tick's next 100ms pass.
	wakeCh chan struct{}

	mu sync.Mutex
}

func NewConn(c net.Conn, srv *Server, id uint64) *Conn {
	conn := &Conn{
		ID:       id,
		Conn:     c,
		Server:   srv,
		WatchMap: make(map[*Tube]bool),
		reader:   bufio.NewReaderSize(c, 8192),
		state:    StateWantCommand,
		wakeCh:   make(chan struct{}, 1),
	}
	// Init reserved jobs sentinel
	conn.resHead.resPrev = &conn.resHead
	conn.resHead.resNext = &conn.resHead
	return conn
}

// wake nudges a connection blocked in waitForReserve to re-check its
// condition immediately, instead of waiting for tick's next 100ms pass.
// Safe to call from any goroutine that holds Server.mu (put, release,
// kick, kick-job, kick-tube, tick's delayed-job promotion and tube
// unpause, a TTR expiry's re-enqueue, a connection close re-enqueueing
// its reserved jobs); never blocks.
func (c *Conn) wake() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

// registerWaiting adds c to the WaitingConns list of every tube it
// watches, so wake() can find it. Caller must hold Server.mu.
func (c *Conn) registerWaiting() {
	for t := range c.WatchMap {
		t.addWaiter(c)
	}
}

// unregisterWaiting is registerWaiting's inverse, called once c's
// blocked reserve resolves. Caller must hold Server.mu.
func (c *Conn) unregisterWaiting() {
	for t := range c.WatchMap {
		t.removeWaiter(c)
	}
}

func (c *Conn) reserveJob(j *Job) {
	j.Reserver = c
	j.State = StateReserved
	j.DeadlineAt = time.Now().Add(j.TTR)
	j.ReserveCt++

	// Insert into reserved list
	prev := c.resHead.resPrev
	prev.resNext = j
	j.resPrev = prev
	j.resNext = &c.resHead
	c.resHead.resPrev = j

	c.clearSoonestCache()
}

func (c *Conn) unreserveJob(j *Job) {
	if j.resPrev == nil && j.resNext == nil {
		return
	}
	j.resPrev.resNext = j.resNext
	j.resNext.resPrev = j.resPrev
	j.resPrev = nil
	j.resNext = nil
	j.Reserver = nil
	c.clearSoonestCache()
}

func (c *Conn) clearSoonestCache() {
	c.soonestJob = nil
}

func (c *Conn) getSoonestJob() *Job {
	if c.soonestJob != nil {
		return c.soonestJob
	}
	var soonest *Job
	for j := c.resHead.resNext; j != &c.resHead; j = j.resNext {
		if soonest == nil || j.DeadlineAt.Before(soonest.DeadlineAt) {
			soonest = j
		}
	}
	c.soonestJob = soonest
	return soonest
}

func (c *Conn) hasDeadlineSoon(now time.Time) bool {
	sj := c.getSoonestJob()
	if sj == nil {
		return false
	}
	return sj.DeadlineAt.Sub(now) <= safetyMargin
}

func (c *Conn) hasReadyJob() bool {
	for t := range c.WatchMap {
		if t.Ready.Len() > 0 && !t.isPaused(time.Now()) {
			return true
		}
	}
	return false
}

func (c *Conn) reenqueueReservedJobs() {
	for j := c.resHead.resNext; j != &c.resHead; {
		next := j.resNext
		c.unreserveJob(j)
		t := j.Tube
		if t != nil && t.Tombstoned {
			// The tube was deleted (delete-tube) while this job was
			// reserved here; drop it instead of resurrecting the tube.
			t.Stat.ReservedCt--
			c.Server.reservedCt--
			c.Server.finishDeleteJob(j)
			c.Server.gcTube(t)
			j = next
			continue
		}
		j.State = StateReady
		j.DeadlineAt = time.Time{}
		if t != nil {
			t.Ready.Push(j)
			t.Stat.ReadyCt++
			c.Server.readyCt++
			if j.Pri < 1024 {
				t.Stat.UrgentCt++
				c.Server.globalUrgentCt++
			}
			c.Server.wakeWaitersForTube(t)
		}
		j = next
	}
}
