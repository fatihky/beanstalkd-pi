package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func (c *Conn) serve() {
	for {
		switch c.state {
		case StateWantCommand:
			line, err := c.reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				logger.Debug("conn read error", "conn", c.ID, "err", err)
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if len(line) > maxLineLen {
				c.replyWord("BAD_FORMAT\r\n")
				continue
			}
			c.dispatchCmd(line)

		case StateWantData:
			remaining := c.inJob.BodySize + 2 - c.inJobRead
			buf := make([]byte, remaining)
			n, err := io.ReadFull(c.reader, buf)
			c.inJobRead += n
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return
				}
				logger.Debug("conn body read error", "conn", c.ID, "err", err)
				return
			}
			copy(c.inJob.Body[c.inJobRead-n:], buf[:n])

			if c.inJobRead >= c.inJob.BodySize+2 {
				body := c.inJob.Body
				bs := c.inJob.BodySize
				if body[bs] != '\r' || body[bs+1] != '\n' {
					c.replyWord("EXPECTED_CRLF\r\n")
					c.inJob = nil
					c.inJobRead = 0
					c.state = StateWantCommand
					continue
				}
				c.finishPut()
			}

		case StateBitBucket:
			remaining := c.inJob.BodySize + 2 - c.inJobRead
			if remaining > 0 {
				buf := make([]byte, remaining)
				n, err := io.ReadFull(c.reader, buf)
				c.inJobRead += n
				if err != nil {
					return
				}
			}
			if c.inJobRead >= c.inJob.BodySize+2 {
				c.inJob = nil
				c.inJobRead = 0
				c.state = StateWantCommand
			}

		case StateClose:
			return

		default:
			return
		}
	}
}

func (c *Conn) dispatchCmd(line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	cmd := parts[0]

	switch cmd {
	case "put":
		c.handlePut(parts[1:])
	case "put-at":
		c.handlePutAt(parts[1:])
	case "use":
		c.handleUse(parts[1:])
	case "reserve":
		c.handleReserve(parts[1:])
	case "reserve-with-timeout":
		c.handleReserveWithTimeout(parts[1:])
	case "reserve-job":
		c.handleReserveJob(parts[1:])
	case "delete":
		c.handleDelete(parts[1:])
	case "delete-tube":
		c.handleDeleteTube(parts[1:])
	case "release":
		c.handleRelease(parts[1:])
	case "bury":
		c.handleBury(parts[1:])
	case "touch":
		c.handleTouch(parts[1:])
	case "watch":
		c.handleWatch(parts[1:])
	case "ignore":
		c.handleIgnore(parts[1:])
	case "peek":
		c.handlePeek(parts[1:])
	case "peek-ready":
		c.handlePeekReady()
	case "peek-delayed":
		c.handlePeekDelayed()
	case "peek-buried":
		c.handlePeekBuried()
	case "peek-tube":
		c.handlePeekTube(parts[1:])
	case "kick":
		c.handleKick(parts[1:])
	case "kick-tube":
		c.handleKickTube(parts[1:])
	case "kick-job":
		c.handleKickJob(parts[1:])
	case "stats-job":
		c.handleStatsJob(parts[1:])
	case "stats-tube":
		c.handleStatsTube(parts[1:])
	case "stats-conn":
		c.handleStatsConn(parts[1:])
	case "stats":
		c.handleStats()
	case "list-tubes":
		c.handleListTubes()
	case "list-tube-used":
		c.handleListTubeUsed()
	case "list-tubes-watched":
		c.handleListTubesWatched()
	case "list-connections":
		c.handleListConnections()
	case "pause-tube":
		c.handlePauseTube(parts[1:])
	case "set-dlq":
		c.handleSetDlq(parts[1:])
	case "quit":
		c.state = StateClose
		return
	case "ping":
		c.handlePing()
	case "capabilities":
		c.handleCapabilities()
	default:
		c.replyWord("UNKNOWN_COMMAND\r\n")
	}
}

func (c *Conn) handlePut(args []string) {
	if len(args) != 4 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	pri, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	delaySec, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	ttrSec, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	bodySize, err := strconv.Atoi(args[3])
	if err != nil || bodySize < 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.startPut(uint32(pri), time.Duration(delaySec)*time.Second, time.Time{}, ttrSec, bodySize, false)
}

func (c *Conn) handlePutAt(args []string) {
	if len(args) != 4 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	pri, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	ts, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	ttrSec, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	bodySize, err := strconv.Atoi(args[3])
	if err != nil || bodySize < 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.startPut(uint32(pri), 0, time.Unix(ts, 0), ttrSec, bodySize, true)
}

// startPut validates and begins insertion of a new job, shared by put and
// put-at. Exactly one of delay/scheduledAt is meaningful: delay for put's
// relative wait, scheduledAt for put-at's absolute wake time; pass the zero
// value for whichever doesn't apply. scheduledAt is resolved to a concrete
// delay by enqueueJob at the moment the job is actually enqueued (after the
// body has been read), so it isn't affected by how long that takes.
func (c *Conn) startPut(pri uint32, delay time.Duration, scheduledAt time.Time, ttrSec uint64, bodySize int, isPutAt bool) {
	if bodySize > c.Server.maxJobSize {
		c.replyWord("JOB_TOO_BIG\r\n")
		c.state = StateBitBucket
		c.inJob = &Job{BodySize: bodySize}
		c.inJobRead = 0
		return
	}

	c.Server.mu.Lock()
	if c.Server.drainMode.Load() {
		c.Server.mu.Unlock()
		c.replyWord("DRAINING\r\n")
		c.state = StateBitBucket
		c.inJob = &Job{BodySize: bodySize}
		c.inJobRead = 0
		return
	}

	id := c.Server.nextID.Add(1)
	if isPutAt {
		c.Server.globalStats.CmdPutAt++
	} else {
		c.Server.globalStats.CmdPut++
	}
	c.Server.globalStats.TotalJobs++
	if !c.isProducer {
		c.isProducer = true
		c.Server.producers++
	}
	c.Server.mu.Unlock()

	ttr := time.Duration(ttrSec) * time.Second
	if ttr < time.Second {
		ttr = time.Second
	}

	j := &Job{
		ID:          id,
		Pri:         pri,
		Delay:       delay,
		ScheduledAt: scheduledAt,
		TTR:         ttr,
		BodySize:    bodySize,
		CreatedAt:   time.Now(),
		State:       StateInvalid,
		Tube:        c.UseTube,
		Body:        make([]byte, bodySize+2),
	}

	c.inJob = j
	c.inJobRead = 0
	c.state = StateWantData
}

func (c *Conn) finishPut() {
	j := c.inJob
	c.inJob = nil
	c.inJobRead = 0
	c.state = StateWantCommand

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	t := j.Tube
	t.Stat.TotalJobsCt++
	c.Server.jobIdx.Add(j)
	c.Server.enqueueJob(j)
	c.Server.persistJob(j)

	c.replyWord(fmt.Sprintf("INSERTED %d\r\n", j.ID))
}

func (c *Conn) handleUse(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdUse++

	oldTube := c.UseTube
	if oldTube != nil {
		oldTube.Stat.UsingCt--
		c.Server.gcTube(oldTube)
	}

	t := c.Server.makeTube(name)
	c.UseTube = t
	t.Stat.UsingCt++

	c.replyWord(fmt.Sprintf("USING %s\r\n", name))
}

func (c *Conn) handleReserve(args []string) {
	if len(args) != 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	c.Server.globalStats.CmdReserve++
	if !c.isWorker {
		c.isWorker = true
		c.Server.workers++
	}
	c.Server.mu.Unlock()

	c.doReserve(-1)
}

func (c *Conn) handleReserveWithTimeout(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	timeout, err := strconv.Atoi(args[0])
	if err != nil || timeout < 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	c.Server.globalStats.CmdReserveWithTimeout++
	if !c.isWorker {
		c.isWorker = true
		c.Server.workers++
	}
	c.Server.mu.Unlock()

	if timeout == 0 {
		c.doReserveImmediate()
	} else {
		c.doReserve(time.Duration(timeout) * time.Second)
	}
}

// doReserve handles both "reserve" (timeout < 0, blocks indefinitely)
// and "reserve-with-timeout" with a positive timeout. If a job is
// available right away it's returned synchronously, same as before;
// otherwise it registers c as waiting and blocks c's own connection
// goroutine in waitForReserve until a job turns up, DEADLINE_SOON
// applies, or the timeout elapses - see waitForReserve for how that's
// now event-driven rather than polled from tick.
func (c *Conn) doReserve(timeout time.Duration) {
	start := time.Now()

	unlock := c.Server.lockTraced("reserve")

	if c.hasDeadlineSoon(start) && !c.hasReadyJobLocked() {
		unlock()
		c.traceReserve(start, "reserve")
		c.replyWord("DEADLINE_SOON\r\n")
		return
	}

	if j := c.Server.findJobForConn(c); j != nil {
		unlock()
		c.traceReserve(start, "reserve")
		c.sendReservedJob(j)
		return
	}

	c.isWaiting = true
	c.Server.waiting++
	for t := range c.WatchMap {
		t.Stat.WaitingCt++
	}
	c.registerWaiting()

	c.hasTimeout = timeout >= 0
	if c.hasTimeout {
		c.timeoutAt = time.Now().Add(timeout)
	}

	unlock()
	c.traceReserve(start, "reserve")

	c.waitForReserve()
}

// clearWaitingLocked ends c's blocked-reserve bookkeeping - the inverse
// of the isWaiting/waiting/WaitingCt/registerWaiting bump doReserve
// makes when it starts blocking. Caller must hold Server.mu.
func (c *Conn) clearWaitingLocked() {
	c.isWaiting = false
	c.Server.waiting--
	for t := range c.WatchMap {
		t.Stat.WaitingCt--
	}
	c.unregisterWaiting()
	c.hasTimeout = false
}

// checkWait re-examines a blocked reserve's condition under Server.mu,
// in the same priority tick() used to poll in: DEADLINE_SOON, then
// TIMED_OUT, then a ready job. As soon as one applies it clears c's
// waiting state and reports it as resolved for waitForReserve to reply
// to. If none apply yet, it instead reports the next wall-clock instant
// (if any) at which DEADLINE_SOON or the reserve timeout would apply,
// so waitForReserve can set a precise timer rather than poll; a job
// turning Ready is not time-based, so that outcome relies entirely on
// wake() being called wherever it happens (see wakeWaitersForTube).
func (c *Conn) checkWait() (job *Job, reply string, resolved bool, deadline time.Time, hasDeadline bool) {
	unlock := c.Server.lockTraced("reserve-wait")
	defer unlock()

	if !c.isWaiting {
		// Already resolved elsewhere - shouldn't normally happen since
		// only this goroutine clears its own waiting state, but this
		// guards against resolving twice rather than assuming it can't.
		resolved = true
		return
	}

	now := time.Now()

	if c.hasDeadlineSoon(now) && !c.hasReadyJobLocked() {
		c.clearWaitingLocked()
		reply = "DEADLINE_SOON\r\n"
		resolved = true
		return
	}

	if c.hasTimeout && !now.Before(c.timeoutAt) {
		c.clearWaitingLocked()
		reply = "TIMED_OUT\r\n"
		resolved = true
		return
	}

	if j := c.Server.findJobForConn(c); j != nil {
		c.clearWaitingLocked()
		job = j
		resolved = true
		return
	}

	if sj := c.getSoonestJob(); sj != nil {
		deadline = sj.DeadlineAt.Add(-safetyMargin)
		hasDeadline = true
	}
	if c.hasTimeout && (!hasDeadline || c.timeoutAt.Before(deadline)) {
		deadline = c.timeoutAt
		hasDeadline = true
	}
	return
}

// waitForReserve blocks c's own connection goroutine - without holding
// Server.mu - until checkWait resolves it: a job becomes available, a
// DEADLINE_SOON condition applies, or the reserve timeout elapses. Only
// the latter two are purely time-based, so it sets a timer for exactly
// checkWait's reported deadline (not a fixed poll interval) to cover
// them precisely; a job turning Ready instead relies on being woken via
// wakeCh, signaled by wake() at every site a job in a watched tube
// turns Ready. Before this, all three outcomes were only ever noticed
// on tick's next 100ms pass, adding up to 100ms of latency to every
// reserve that had to block.
func (c *Conn) waitForReserve() {
	for {
		job, reply, resolved, deadline, hasDeadline := c.checkWait()
		if resolved {
			switch {
			case job != nil:
				c.sendReservedJob(job)
			case reply != "":
				c.replyWord(reply)
			}
			return
		}

		var timerC <-chan time.Time
		var timer *time.Timer
		if hasDeadline {
			d := time.Until(deadline)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			timerC = timer.C
		}

		select {
		case <-c.wakeCh:
		case <-timerC:
		case <-c.Server.closeCh:
			if timer != nil {
				timer.Stop()
			}
			return
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (c *Conn) doReserveImmediate() {
	start := time.Now()

	unlock := c.Server.lockTraced("reserve-with-timeout-0")
	defer unlock()
	defer c.traceReserve(start, "reserve-with-timeout-0")

	if j := c.Server.findJobForConn(c); j != nil {
		c.sendReservedJob(j)
		return
	}

	c.replyWord("TIMED_OUT\r\n")
}

// traceReserve logs the time spent handling a reserve call: at debug
// level always (request-level tracing), and as a warning when it
// exceeds the server's slow-log threshold - a symptom of lock
// contention or an oversized watch list.
func (c *Conn) traceReserve(start time.Time, op string) {
	elapsed := time.Since(start)
	if elapsed > c.Server.slowLogThreshold {
		logger.Warn("slow reserve", "op", op, "conn", c.ID, "elapsed", elapsed)
	} else {
		logger.Debug("reserve", "op", op, "conn", c.ID, "elapsed", elapsed)
	}
}

func (c *Conn) hasReadyJobLocked() bool {
	for t := range c.WatchMap {
		if t.Ready.Len() > 0 && !t.isPaused(time.Now()) {
			return true
		}
	}
	return false
}

func (c *Conn) sendReservedJob(j *Job) {
	c.state = StateWantCommand
	header := fmt.Sprintf("RESERVED %d %d\r\n", j.ID, j.BodySize)
	data := make([]byte, 0, len(header)+j.BodySize+2)
	data = append(data, header...)
	data = append(data, j.Body[:j.BodySize]...)
	data = append(data, '\r', '\n')
	c.writeAll(data)
}

func (c *Conn) handleReserveJob(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdReserveJob++
	if !c.isWorker {
		c.isWorker = true
		c.Server.workers++
	}

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State == StateReserved || j.State == StateInvalid {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	if j.State != StateReady && j.State != StateBuried && j.State != StateDelayed {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	switch j.State {
	case StateReady:
		j.Tube.Ready.Remove(j.readyIdx)
		j.Tube.Stat.ReadyCt--
		c.Server.readyCt--
		if j.Pri < 1024 {
			j.Tube.Stat.UrgentCt--
			c.Server.globalUrgentCt--
		}
		j.Tube.Stat.WaitHist.Observe(time.Since(j.ReadyAt))
	case StateBuried:
		j.Tube.buryRemove(j)
		j.Tube.Stat.BuriedCt--
		c.Server.buriedCt--
	case StateDelayed:
		j.Tube.Delay.Remove(j.delayIdx)
		j.Tube.Stat.DelayedCt--
		c.Server.delayedCt--
	}

	j.Tube.Stat.ReservedCt++
	c.Server.reservedCt++
	c.reserveJob(j)
	c.sendReservedJob(j)
}

func (c *Conn) handleDelete(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdDelete++

	j := c.Server.jobIdx.Get(id)
	if j == nil {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	if j.State == StateReserved && j.Reserver != c {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	t := j.Tube
	if !c.Server.deleteJob(j) {
		c.replyWord("NOT_FOUND\r\n")
		return
	}
	c.Server.gcTube(t)

	c.replyWord("DELETED\r\n")
}

func (c *Conn) handleRelease(args []string) {
	if len(args) != 3 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	pri, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	delaySec, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdRelease++

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State != StateReserved || j.Reserver != c {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	c.unreserveJob(j)
	t := j.Tube
	t.Stat.ReservedCt--
	c.Server.reservedCt--

	if t.Tombstoned {
		// The tube was deleted (delete-tube) while this job was
		// reserved; drop it instead of resurrecting the tube.
		c.Server.finishDeleteJob(j)
		c.Server.gcTube(t)
		c.replyWord("RELEASED\r\n")
		return
	}

	j.Pri = uint32(pri)
	j.Delay = time.Duration(delaySec) * time.Second
	j.ReleaseCt++

	if c.Server.checkDeadLetter(j) {
		// Routed to the tube's dead-letter tube instead of back to t;
		// from the client's point of view it still just released the
		// job (see checkDeadLetter in server.go).
		c.replyWord("RELEASED\r\n")
		return
	}

	c.Server.enqueueJob(j)
	c.Server.persistJob(j)
	c.replyWord("RELEASED\r\n")
}

func (c *Conn) handleBury(args []string) {
	if len(args) != 2 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	pri, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdBury++

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State != StateReserved || j.Reserver != c {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	c.unreserveJob(j)
	t := j.Tube
	t.Stat.ReservedCt--
	c.Server.reservedCt--

	if t.Tombstoned {
		// The tube was deleted (delete-tube) while this job was
		// reserved; drop it instead of resurrecting the tube.
		c.Server.finishDeleteJob(j)
		c.Server.gcTube(t)
		c.replyWord("BURIED\r\n")
		return
	}

	j.Pri = uint32(pri)
	j.State = StateBuried
	j.BuryCt++
	j.DeadlineAt = time.Time{}
	t.buryPush(j)
	t.Stat.BuriedCt++
	c.Server.buriedCt++
	c.Server.persistJob(j)

	c.replyWord("BURIED\r\n")
}

func (c *Conn) handleTouch(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdTouch++

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State != StateReserved || j.Reserver != c {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	j.DeadlineAt = time.Now().Add(j.TTR)
	c.clearSoonestCache()

	c.replyWord("TOUCHED\r\n")
}

func (c *Conn) handleWatch(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdWatch++

	t := c.Server.makeTube(name)
	if !c.WatchMap[t] {
		c.WatchMap[t] = true
		t.Stat.WatchingCt++
	}

	c.replyWord(fmt.Sprintf("WATCHING %d\r\n", len(c.WatchMap)))
}

func (c *Conn) handleIgnore(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdIgnore++

	if len(c.WatchMap) <= 1 {
		c.replyWord("NOT_IGNORED\r\n")
		return
	}

	t, ok := c.Server.tubes[name]
	if ok && c.WatchMap[t] {
		delete(c.WatchMap, t)
		t.Stat.WatchingCt--
		c.Server.gcTube(t)
	}

	c.replyWord(fmt.Sprintf("WATCHING %d\r\n", len(c.WatchMap)))
}

func (c *Conn) handlePeek(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPeek++

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State == StateInvalid {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	c.sendFoundJob(j)
}

func (c *Conn) handlePeekReady() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPeekReady++

	t := c.UseTube
	if t.Ready.Len() == 0 {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	j := t.Ready.Peek()
	c.sendFoundJob(j)
}

func (c *Conn) handlePeekDelayed() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPeekDelayed++

	t := c.UseTube
	if t.Delay.Len() == 0 {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	j := t.Delay.Peek()
	c.sendFoundJob(j)
}

func (c *Conn) handlePeekBuried() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPeekBuried++

	t := c.UseTube
	head := t.BuriedHead
	if head.buriedNext == head {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	j := head.buriedNext
	c.sendFoundJob(j)
}

// handlePeekTube is "peek-ready"/"peek-delayed"/"peek-buried" against an
// explicit tube instead of the connection's currently used tube - useful
// for an operator inspecting a tube they are not otherwise using, without
// the use/peek/use-back dance that would otherwise be needed (and which
// mutates connection state along the way). The tube map lookup makes
// finding the named tube O(1), same as kick-tube/delete-tube. This is a
// beanstalkd-pi extension, not part of stock beanstalkd's protocol.
func (c *Conn) handlePeekTube(args []string) {
	if len(args) != 2 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	state := args[1]
	if state != "ready" && state != "delayed" && state != "buried" {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPeekTube++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	var j *Job
	switch state {
	case "ready":
		if t.Ready.Len() > 0 {
			j = t.Ready.Peek()
		}
	case "delayed":
		if t.Delay.Len() > 0 {
			j = t.Delay.Peek()
		}
	case "buried":
		head := t.BuriedHead
		if head.buriedNext != head {
			j = head.buriedNext
		}
	}

	if j == nil {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	c.sendFoundJob(j)
}

func (c *Conn) sendFoundJob(j *Job) {
	header := fmt.Sprintf("FOUND %d %d\r\n", j.ID, j.BodySize)
	data := make([]byte, 0, len(header)+j.BodySize+2)
	data = append(data, header...)
	data = append(data, j.Body[:j.BodySize]...)
	data = append(data, '\r', '\n')
	c.writeAll(data)
}

// kickTube moves up to bound jobs of t from buried (or, if no buried jobs,
// from delayed) into the ready queue, updating per-tube and global
// counters. Caller must hold Server.mu.
func (s *Server) kickTube(t *Tube, bound int) int {
	kicked := 0

	head := t.BuriedHead
	for j := head.buriedNext; j != head && kicked < bound; {
		next := j.buriedNext
		t.buryRemove(j)
		t.Stat.BuriedCt--
		s.buriedCt--
		j.KickCt++
		j.State = StateReady
		j.DeadlineAt = time.Time{}
		j.ReadyAt = time.Now()
		t.Ready.Push(j)
		t.Stat.ReadyCt++
		s.readyCt++
		if j.Pri < 1024 {
			t.Stat.UrgentCt++
			s.globalUrgentCt++
		}
		s.persistJob(j)
		kicked++
		j = next
	}

	if kicked == 0 {
		for t.Delay.Len() > 0 && kicked < bound {
			j := t.Delay.Pop()
			t.Stat.DelayedCt--
			s.delayedCt--
			j.KickCt++
			j.State = StateReady
			j.DeadlineAt = time.Time{}
			j.ReadyAt = time.Now()
			t.Ready.Push(j)
			t.Stat.ReadyCt++
			s.readyCt++
			if j.Pri < 1024 {
				t.Stat.UrgentCt++
				s.globalUrgentCt++
			}
			s.persistJob(j)
			kicked++
		}
	}

	if kicked > 0 {
		s.wakeWaitersForTube(t)
	}

	return kicked
}

func (c *Conn) handleKick(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	bound, err := strconv.Atoi(args[0])
	if err != nil || bound < 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdKick++

	kicked := c.Server.kickTube(c.UseTube, bound)
	c.replyWord(fmt.Sprintf("KICKED %d\r\n", kicked))
}

// handleDeleteTube deletes tube by name: every ready, delayed, and
// buried job in it is deleted outright, and a job still reserved by
// another connection is deleted too, once that connection is done with
// it, rather than being allowed to resurrect the tube (see
// Tube.Tombstoned). A tube with no such holdouts - and, per gcTube, not
// currently used/watched by any connection - disappears from
// list-tubes/stats-tube immediately; the "default" tube is never
// removed from the tube map (mirroring gcTube), but its jobs are still
// purged. This is a beanstalkd-pi extension, not part of stock
// beanstalkd's protocol.
func (c *Conn) handleDeleteTube(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdDeleteTube++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	n := c.Server.purgeTube(t)
	c.Server.gcTube(t)

	c.replyWord(fmt.Sprintf("DELETED %d\r\n", n))
}

func (c *Conn) handleKickTube(args []string) {
	if len(args) != 2 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	bound, err := strconv.Atoi(args[1])
	if err != nil || bound < 0 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdKickTube++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	kicked := c.Server.kickTube(t, bound)
	c.replyWord(fmt.Sprintf("KICKED %d\r\n", kicked))
}

func (c *Conn) handleKickJob(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdKickJob++

	j := c.Server.jobIdx.Get(id)
	if j == nil {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	if j.State != StateBuried && j.State != StateDelayed {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	switch j.State {
	case StateBuried:
		j.Tube.buryRemove(j)
		j.Tube.Stat.BuriedCt--
		c.Server.buriedCt--
	case StateDelayed:
		j.Tube.Delay.Remove(j.delayIdx)
		j.Tube.Stat.DelayedCt--
		c.Server.delayedCt--
	}

	j.KickCt++
	j.State = StateReady
	j.DeadlineAt = time.Time{}
	j.ReadyAt = time.Now()
	j.Tube.Ready.Push(j)
	j.Tube.Stat.ReadyCt++
	c.Server.readyCt++
	if j.Pri < 1024 {
		j.Tube.Stat.UrgentCt++
		c.Server.globalUrgentCt++
	}
	c.Server.wakeWaitersForTube(j.Tube)

	c.Server.persistJob(j)
	c.replyWord("KICKED\r\n")
}

func (c *Conn) handleStatsJob(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdStatsJob++

	j := c.Server.jobIdx.Get(id)
	if j == nil || j.State == StateInvalid {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	yaml := c.Server.formatJobStats(j)
	c.sendYAML(yaml)
}

func (c *Conn) handleStatsTube(args []string) {
	if len(args) != 1 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdStatsTube++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	yaml := c.Server.formatTubeStats(t)
	c.sendYAML(yaml)
}

func (c *Conn) handleStats() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdStats++
	c.Server.mu.Unlock()

	yaml := c.Server.formatStats()
	c.sendYAML(yaml)
}

func (c *Conn) handleListTubes() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdListTubes++
	c.Server.mu.Unlock()

	yaml := c.Server.formatListTubes()
	c.sendYAML(yaml)
}

func (c *Conn) handlePing() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdPing++
	c.Server.mu.Unlock()

	c.replyWord("PONG\r\n")
}

// handleCapabilities replies with the server version, the list of
// extension commands with no stock beanstalkd equivalent, and the
// limits (max-job-size, max-tube-name-len) a client would otherwise
// have to hardcode or discover by probing and eating UNKNOWN_COMMAND.
func (c *Conn) handleCapabilities() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdCapabilities++
	c.Server.mu.Unlock()

	yaml := c.Server.formatCapabilities()
	c.sendYAML(yaml)
}

func (c *Conn) handleListTubeUsed() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdListTubeUsed++
	name := c.UseTube.Name
	c.Server.mu.Unlock()

	c.replyWord(fmt.Sprintf("USING %s\r\n", name))
}

func (c *Conn) handleListTubesWatched() {
	c.Server.mu.Lock()
	c.Server.globalStats.CmdListTubesWatched++

	result := "---\n"
	for t := range c.WatchMap {
		result += "- " + t.Name + "\n"
	}
	c.Server.mu.Unlock()

	c.sendYAML(result)
}

func (c *Conn) handlePauseTube(args []string) {
	if len(args) != 2 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	delaySec, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdPauseTube++

	t, ok := c.Server.tubes[name]
	if !ok {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

	delay := time.Duration(delaySec) * time.Second
	if delay == 0 {
		delay = time.Nanosecond
	}

	t.Pause = delay
	t.UnpauseAt = time.Now().Add(delay)
	t.Stat.PauseTubeCt++

	c.replyWord("PAUSED\r\n")
}

// handleSetDlq implements the "set-dlq" extension command: configures
// automatic dead-letter routing for a tube (see checkDeadLetter in
// server.go). Unlike pause-tube/kick-tube/delete-tube, <tube> need not
// already exist - like "use", it is created if missing, so DLQ policy
// can be set up before any producer has touched the tube. <dead-tube> is
// validated but, deliberately, not created here: it only comes into
// existence once a job is actually routed into it.
func (c *Conn) handleSetDlq(args []string) {
	if len(args) != 3 {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	name := args[0]
	if !validTubeName(name) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	maxAttempts, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	deadTube := args[2]
	if !validTubeName(deadTube) {
		c.replyWord("BAD_FORMAT\r\n")
		return
	}

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdSetDlq++

	t := c.Server.makeTube(name)
	t.MaxAttempts = uint32(maxAttempts)
	if t.MaxAttempts == 0 {
		t.DeadLetterTube = ""
	} else {
		t.DeadLetterTube = deadTube
	}

	c.replyWord("DLQ_SET\r\n")
}

func (c *Conn) sendYAML(yaml string) {
	header := fmt.Sprintf("OK %d\r\n", len(yaml))
	data := make([]byte, 0, len(header)+len(yaml)+2)
	data = append(data, header...)
	data = append(data, yaml...)
	data = append(data, '\r', '\n')
	c.writeAll(data)
}

func (c *Conn) replyWord(s string) {
	c.writeAll([]byte(s))
}

func (c *Conn) writeAll(data []byte) {
	c.Conn.Write(data)
}

func validTubeName(name string) bool {
	if len(name) == 0 || len(name) > maxTubeName {
		return false
	}
	if name[0] == '-' {
		return false
	}
	for _, ch := range name {
		if !((ch >= 'A' && ch <= 'Z') ||
			(ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '+' || ch == '/' ||
			ch == ';' || ch == '.' || ch == '$' ||
			ch == '_' || ch == '(' || ch == ')') {
			return false
		}
	}
	return true
}
