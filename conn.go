package main

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type ConnState int

const (
	StateWantCommand  ConnState = iota
	StateWantData
	StateSendJob
	StateSendWord
	StateWait
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
	inJob      *Job
	inJobRead  int // bytes read so far of body+2

	// Outgoing data
	outBuf []byte

	// Reserve timeout
	hasTimeout bool
	timeoutAt  time.Time

	state    ConnState
	cmdBuf   []byte
	reader   *bufio.Reader

	halfClosed bool
	isProducer bool
	isWorker   bool
	isWaiting  bool

	soonestJob *Job // cached earliest deadline reserved job

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
	}
	// Init reserved jobs sentinel
	conn.resHead.resPrev = &conn.resHead
	conn.resHead.resNext = &conn.resHead
	return conn
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
		j.State = StateReady
		j.DeadlineAt = time.Time{}
		t := j.Tube
		if t != nil {
			t.Ready.Push(j)
			t.Stat.ReadyCt++
			c.Server.readyCt++
			if j.Pri < 1024 {
				t.Stat.UrgentCt++
				c.Server.globalUrgentCt++
			}
		}
		j = next
	}
}
