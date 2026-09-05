package main

import (
	"fmt"
	"io"
	"log"
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
				log.Printf("conn %d read error: %v", c.ID, err)
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
				log.Printf("conn %d body read error: %v", c.ID, err)
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

		case StateWait:
			time.Sleep(5 * time.Millisecond)
			continue

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
	case "kick":
		c.handleKick(parts[1:])
	case "kick-job":
		c.handleKickJob(parts[1:])
	case "stats-job":
		c.handleStatsJob(parts[1:])
	case "stats-tube":
		c.handleStatsTube(parts[1:])
	case "stats":
		c.handleStats()
	case "list-tubes":
		c.handleListTubes()
	case "list-tube-used":
		c.handleListTubeUsed()
	case "list-tubes-watched":
		c.handleListTubesWatched()
	case "pause-tube":
		c.handlePauseTube(parts[1:])
	case "quit":
		c.state = StateClose
		return
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

	if bodySize > maxJobSize {
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
	c.Server.globalStats.CmdPut++
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
		ID:        id,
		Pri:       uint32(pri),
		Delay:     time.Duration(delaySec) * time.Second,
		TTR:       ttr,
		BodySize:  bodySize,
		CreatedAt: time.Now(),
		State:     StateInvalid,
		Tube:      c.UseTube,
		Body:      make([]byte, bodySize+2),
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

func (c *Conn) doReserve(timeout time.Duration) {
	now := time.Now()

	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	if c.hasDeadlineSoon(now) && !c.hasReadyJobLocked() {
		c.replyWord("DEADLINE_SOON\r\n")
		return
	}

	if j := c.Server.findJobForConn(c); j != nil {
		c.sendReservedJob(j)
		return
	}

	c.isWaiting = true
	c.Server.waiting++
	for t := range c.WatchMap {
		t.Stat.WaitingCt++
	}

	if timeout >= 0 {
		c.hasTimeout = true
		c.timeoutAt = time.Now().Add(timeout)
	}

	c.state = StateWait
}

func (c *Conn) doReserveImmediate() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	if j := c.Server.findJobForConn(c); j != nil {
		c.sendReservedJob(j)
		return
	}

	c.replyWord("TIMED_OUT\r\n")
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

	if !c.Server.deleteJob(j) {
		c.replyWord("NOT_FOUND\r\n")
		return
	}

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
	j.Tube.Stat.ReservedCt--
	c.Server.reservedCt--

	j.Pri = uint32(pri)
	j.Delay = time.Duration(delaySec) * time.Second
	j.ReleaseCt++

	c.Server.enqueueJob(j)
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
	j.Tube.Stat.ReservedCt--
	c.Server.reservedCt--

	j.Pri = uint32(pri)
	j.State = StateBuried
	j.BuryCt++
	j.DeadlineAt = time.Time{}
	j.Tube.buryPush(j)
	j.Tube.Stat.BuriedCt++
	c.Server.buriedCt++

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

func (c *Conn) sendFoundJob(j *Job) {
	header := fmt.Sprintf("FOUND %d %d\r\n", j.ID, j.BodySize)
	data := make([]byte, 0, len(header)+j.BodySize+2)
	data = append(data, header...)
	data = append(data, j.Body[:j.BodySize]...)
	data = append(data, '\r', '\n')
	c.writeAll(data)
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

	t := c.UseTube
	kicked := 0

	head := t.BuriedHead
	for j := head.buriedNext; j != head && kicked < bound; {
		next := j.buriedNext
		t.buryRemove(j)
		t.Stat.BuriedCt--
		c.Server.buriedCt--
		j.KickCt++
		j.State = StateReady
		j.DeadlineAt = time.Time{}
		t.Ready.Push(j)
		t.Stat.ReadyCt++
		c.Server.readyCt++
		if j.Pri < 1024 {
			t.Stat.UrgentCt++
			c.Server.globalUrgentCt++
		}
		kicked++
		j = next
	}

	if kicked == 0 {
		for t.Delay.Len() > 0 && kicked < bound {
			j := t.Delay.Pop()
			t.Stat.DelayedCt--
			c.Server.delayedCt--
			j.KickCt++
			j.State = StateReady
			j.DeadlineAt = time.Time{}
			t.Ready.Push(j)
			t.Stat.ReadyCt++
			c.Server.readyCt++
			if j.Pri < 1024 {
				t.Stat.UrgentCt++
				c.Server.globalUrgentCt++
			}
			kicked++
		}
	}

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
	j.Tube.Ready.Push(j)
	j.Tube.Stat.ReadyCt++
	c.Server.readyCt++
	if j.Pri < 1024 {
		j.Tube.Stat.UrgentCt++
		c.Server.globalUrgentCt++
	}

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
