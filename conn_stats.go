package main

// Implements "stats-conn" and "list-connections", beanstalkd-pi extensions
// with no stock equivalent: stock beanstalkd has no notion of a connection
// as something a client can inspect at all. See protocol.txt's "Extension
// Commands" section for the wire-level contract and dispatchCmd in
// protocol.go for where these are wired into command dispatch.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// handleStatsConn replies with introspection stats about the calling
// connection itself: its id, remote address, used tube, watched tubes,
// the ids of jobs it currently holds reserved, and its
// producer/worker/waiting flags. Unlike stats-job/stats-tube it takes no
// argument - it is always about the connection that sent it - so a
// client that hasn't otherwise learned its own connection id (e.g. from
// list-connections) can retrieve it here.
func (c *Conn) handleStatsConn() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdStatsConn++

	yaml := c.Server.formatConnStats(c)
	c.sendYAML(yaml)
}

// handleListConnections replies with the same fields as stats-conn for
// every currently connected client, letting an operator answer
// questions stats-conn can't - e.g. "which connection is holding job
// 4711, and what tubes is it watching".
func (c *Conn) handleListConnections() {
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()

	c.Server.globalStats.CmdListConnections++

	yaml := c.Server.formatListConnections()
	c.sendYAML(yaml)
}

// formatConnStats returns introspection stats for a single connection: its
// id, remote address, used tube, watched tubes, the ids of jobs it
// currently holds reserved, and its producer/worker/waiting flags. See
// formatListConnections for the same fields across every connection.
//
// Caller must hold s.mu.
func (s *Server) formatConnStats(c *Conn) string {
	return fmt.Sprintf(`---
id: %d
addr: %s
tube: %s
watching: %s
reserved-jobs: %s
producer: %s
worker: %s
waiting: %s
`,
		c.ID,
		c.Conn.RemoteAddr().String(),
		c.UseTube.Name,
		yamlStringList(watchedTubeNames(c)),
		yamlUint64List(reservedJobIDs(c)),
		boolStr(c.isProducer),
		boolStr(c.isWorker),
		boolStr(c.isWaiting),
	)
}

// formatListConnections returns the same fields as formatConnStats for
// every currently connected client, as a YAML sequence of mappings
// ordered by connection id - the introspection stats-conn can't provide
// about connections other than the caller's own.
//
// Caller must hold s.mu.
func (s *Server) formatListConnections() string {
	ids := make([]uint64, 0, len(s.conns))
	for id := range s.conns {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var b strings.Builder
	b.WriteString("---\n")
	for _, id := range ids {
		c := s.conns[id]
		fmt.Fprintf(&b, `- id: %d
  addr: %s
  tube: %s
  watching: %s
  reserved-jobs: %s
  producer: %s
  worker: %s
  waiting: %s
`,
			c.ID,
			c.Conn.RemoteAddr().String(),
			c.UseTube.Name,
			yamlStringList(watchedTubeNames(c)),
			yamlUint64List(reservedJobIDs(c)),
			boolStr(c.isProducer),
			boolStr(c.isWorker),
			boolStr(c.isWaiting),
		)
	}
	return b.String()
}

// watchedTubeNames returns c's watched tube names, sorted for
// deterministic output. Caller must hold s.mu.
func watchedTubeNames(c *Conn) []string {
	names := make([]string, 0, len(c.WatchMap))
	for t := range c.WatchMap {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return names
}

// reservedJobIDs returns the ids of jobs c currently holds reserved,
// sorted for deterministic output. Caller must hold s.mu.
func reservedJobIDs(c *Conn) []uint64 {
	ids := make([]uint64, 0)
	for j := c.resHead.resNext; j != &c.resHead; j = j.resNext {
		ids = append(ids, j.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// yamlStringList renders items as a YAML flow-style sequence, e.g.
// "[default, foo]" or "[]" when empty.
func yamlStringList(items []string) string {
	return "[" + strings.Join(items, ", ") + "]"
}

// yamlUint64List renders items as a YAML flow-style sequence of
// integers, e.g. "[101, 102]" or "[]" when empty.
func yamlUint64List(items []uint64) string {
	strs := make([]string, len(items))
	for i, v := range items {
		strs[i] = strconv.FormatUint(v, 10)
	}
	return yamlStringList(strs)
}
