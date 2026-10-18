package main

import (
	"bytes"
	"strings"
	"testing"
)

func cli(t *testing.T, dir string, stdin string, args ...string) (string, error) {
	t.Helper()
	var out, errb bytes.Buffer
	argv := append([]string{args[0], "-dir", dir}, args[1:]...)
	err := run(argv, strings.NewReader(stdin), &out, &errb)
	return out.String(), err
}

func TestCLIRoundTrip(t *testing.T) {
	dir := t.TempDir()
	steps := []struct {
		stdin string
		args  []string
		want  string
	}{
		{"", []string{"put", "name", "caskdb"}, ""},
		{"from stdin\n", []string{"put", "piped"}, ""},
		{"", []string{"put", "tmp", "x"}, ""},
		{"", []string{"get", "name"}, "caskdb\n"},
		{"", []string{"get", "piped"}, "from stdin\n"},
		{"", []string{"keys"}, "name\npiped\ntmp\n"},
		{"", []string{"del", "tmp"}, ""},
		{"", []string{"keys"}, "name\npiped\n"},
	}
	for _, s := range steps {
		got, err := cli(t, dir, s.stdin, s.args...)
		if err != nil {
			t.Fatalf("%v: %v", s.args, err)
		}
		if got != s.want {
			t.Fatalf("%v = %q, want %q", s.args, got, s.want)
		}
	}
	out, err := cli(t, dir, "", "stats")
	if err != nil || !strings.Contains(out, "keys:       2") {
		t.Fatalf("stats = %q, %v", out, err)
	}
	out, err = cli(t, dir, "", "merge")
	if err != nil || !strings.HasPrefix(out, "merged:") {
		t.Fatalf("merge = %q, %v", out, err)
	}
	if got, _ := cli(t, dir, "", "get", "name"); got != "caskdb\n" {
		t.Fatalf("after merge get = %q", got)
	}
}

func TestCLIErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := cli(t, dir, "", "get", "missing"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("get missing: %v", err)
	}
	if _, err := cli(t, dir, "", "get"); err != errUsage {
		t.Errorf("get without key: %v", err)
	}
	if _, err := cli(t, dir, "", "frobnicate"); err == nil {
		t.Error("unknown command accepted")
	}
	var out bytes.Buffer
	if err := run([]string{"help"}, nil, &out, &out); err != nil || !strings.Contains(out.String(), "usage:") {
		t.Errorf("help: %v %q", err, out.String())
	}
}
