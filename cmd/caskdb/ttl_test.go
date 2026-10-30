package main

import (
	"strings"
	"testing"
	"time"
)

func TestCLIExpiry(t *testing.T) {
	dir := t.TempDir()
	if _, err := cli(t, dir, "", "put", "-ttl", "1h", "session", "abc"); err != nil {
		t.Fatal(err)
	}
	out, err := cli(t, dir, "", "ttl", "session")
	if err != nil {
		t.Fatal(err)
	}
	d, err := time.ParseDuration(strings.TrimSpace(out))
	if err != nil || d <= 59*time.Minute || d > time.Hour {
		t.Fatalf("ttl = %q (%v)", out, err)
	}
	if out, _ := cli(t, dir, "", "stats"); !strings.Contains(out, "expiring:   1") {
		t.Fatalf("stats = %q", out)
	}
	if _, err := cli(t, dir, "", "persist", "session"); err != nil {
		t.Fatal(err)
	}
	if out, _ := cli(t, dir, "", "ttl", "session"); out != "no expiry\n" {
		t.Fatalf("ttl after persist = %q", out)
	}
	if _, err := cli(t, dir, "", "expire", "session", "1ms"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := cli(t, dir, "", "get", "session"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("get after expiry: %v", err)
	}
	if _, err := cli(t, dir, "", "ttl", "session"); err == nil {
		t.Fatal("ttl of expired key succeeded")
	}
}

func TestCLIExpiryErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := cli(t, dir, "", "expire", "missing", "10s"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expire missing: %v", err)
	}
	if _, err := cli(t, dir, "", "persist", "missing"); err == nil {
		t.Error("persist missing succeeded")
	}
	cli(t, dir, "", "put", "k", "v")
	if _, err := cli(t, dir, "", "expire", "k", "soon"); err == nil || !strings.Contains(err.Error(), "bad duration") {
		t.Errorf("bad duration: %v", err)
	}
	if _, err := cli(t, dir, "", "get", "-ttl", "1s", "k"); err != errUsage {
		t.Errorf("-ttl on get: %v", err)
	}
	if _, err := cli(t, dir, "", "put", "-ttl", "-1s", "k", "v"); err != errUsage {
		t.Errorf("negative -ttl: %v", err)
	}
	if _, err := cli(t, dir, "", "ttl"); err != errUsage {
		t.Errorf("ttl without key: %v", err)
	}
}
