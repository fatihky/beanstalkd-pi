package main

// Tests for the capabilities command, a beanstalkd-pi extension not
// present in stock beanstalkd. Like the rest of the suite, these drive
// the real TCP wire protocol via the harness in compat_harness_test.go.
// See protocol.txt's "Extension Commands" section for the command's
// exact reply format.

import (
	"strings"
	"testing"
)

func TestCapabilitiesReportsVersionAndLimits(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("capabilities")
	body := c.readOK()

	if !strings.Contains(body, "version: beanstalkd-pi-") {
		t.Fatalf("expected a version line, got:\n%s", body)
	}
	if !strings.Contains(body, "max-job-size: 65536\n") {
		t.Fatalf("expected max-job-size: 65536, got:\n%s", body)
	}
	if !strings.Contains(body, "max-tube-name-len: 200\n") {
		t.Fatalf("expected max-tube-name-len: 200, got:\n%s", body)
	}
}

func TestCapabilitiesListsExtensionCommands(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("capabilities")
	body := c.readOK()

	for _, cmd := range []string{
		"ping",
		"put-at",
		"kick-tube",
		"delete-tube",
		"peek-tube",
		"stats-conn",
		"list-connections",
		"set-dlq",
		"capabilities",
		"drain",
	} {
		if !strings.Contains(body, "- "+cmd+"\n") {
			t.Errorf("expected extensions list to contain %q, got:\n%s", cmd, body)
		}
	}
}

func TestCapabilitiesReflectsConfiguredMaxJobSize(t *testing.T) {
	addr := startTestServerWithMax(t, 1024)
	c := dial(t, addr)

	c.send("capabilities")
	body := c.readOK()

	if !strings.Contains(body, "max-job-size: 1024\n") {
		t.Fatalf("expected max-job-size: 1024, got:\n%s", body)
	}
}

func TestCapabilitiesCountedInStats(t *testing.T) {
	addr := startTestServer(t)
	c := dial(t, addr)

	c.send("capabilities")
	c.readOK()
	c.send("capabilities")
	c.readOK()

	c.send("stats")
	body := c.readOK()
	if !strings.Contains(body, "cmd-capabilities: 2\n") {
		t.Errorf("expected stats to contain \"cmd-capabilities: 2\", got:\n%s", body)
	}
}
