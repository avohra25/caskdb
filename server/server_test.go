package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/avohra25/caskdb"
)

func start(t *testing.T) (addr string, db *caskdb.DB) {
	t.Helper()
	db, err := caskdb.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(db)
	go s.Serve(ln)
	t.Cleanup(func() {
		s.Close()
		db.Close()
	})
	return ln.Addr().String(), db
}

type client struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { c.Close() })
	return &client{t, c, bufio.NewReader(c)}
}

// do sends a RESP command and returns the reply rendered as text.
func (cl *client) do(args ...string) string {
	cl.t.Helper()
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(cl.c, sb.String()); err != nil {
		cl.t.Fatal(err)
	}
	return cl.reply()
}

func (cl *client) reply() string {
	cl.t.Helper()
	line, err := cl.r.ReadString('\n')
	if err != nil {
		cl.t.Fatal(err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(cl.r, buf); err != nil {
			cl.t.Fatal(err)
		}
		return string(buf[:n])
	case '*':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = cl.reply()
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	cl.t.Fatalf("unexpected reply %q", line)
	return ""
}

func TestCommands(t *testing.T) {
	addr, _ := start(t)
	cl := dial(t, addr)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG"},
		{[]string{"ping", "hi"}, "hi"},
		{[]string{"ECHO", "hello world"}, "hello world"},
		{[]string{"GET", "user:1"}, "(nil)"},
		{[]string{"SET", "user:1", "ada"}, "+OK"},
		{[]string{"SET", "user:2", "grace hopper"}, "+OK"},
		{[]string{"SET", "other", ""}, "+OK"},
		{[]string{"GET", "user:2"}, "grace hopper"},
		{[]string{"GET", "other"}, ""},
		{[]string{"EXISTS", "user:1", "user:2", "nope"}, ":2"},
		{[]string{"KEYS", "user:*"}, "[user:1 user:2]"},
		{[]string{"KEYS"}, "[other user:1 user:2]"},
		{[]string{"DBSIZE"}, ":3"},
		{[]string{"DEL", "user:1", "nope"}, ":1"},
		{[]string{"DBSIZE"}, ":2"},
		{[]string{"MERGE"}, "+OK"},
		{[]string{"GET", "user:2"}, "grace hopper"},
		{[]string{"GET"}, "-ERR wrong number of arguments for 'get' command"},
		{[]string{"KEYS", "["}, "-ERR invalid pattern"},
		{[]string{"FLY"}, "-ERR unknown command 'FLY'"},
		{[]string{"SET", "", "x"}, "-ERR caskdb: empty key"},
	}
	for _, c := range cases {
		if got := cl.do(c.args...); got != c.want {
			t.Errorf("%v = %q, want %q", c.args, got, c.want)
		}
	}
	if info := cl.do("INFO"); !strings.Contains(info, "keys:2") {
		t.Errorf("INFO missing key count: %q", info)
	}
	if got := cl.do("QUIT"); got != "+OK" {
		t.Errorf("QUIT = %q", got)
	}
	if _, err := cl.r.ReadByte(); err != io.EOF {
		t.Errorf("connection still open after QUIT (err=%v)", err)
	}
}

func TestBinarySafeValues(t *testing.T) {
	addr, db := start(t)
	cl := dial(t, addr)
	val := "line1\r\nline2\x00\xff"
	if got := cl.do("SET", "bin", val); got != "+OK" {
		t.Fatal(got)
	}
	if got := cl.do("GET", "bin"); got != val {
		t.Fatalf("GET = %q", got)
	}
	if v, _ := db.Get([]byte("bin")); string(v) != val {
		t.Fatalf("stored %q", v)
	}
}

func TestInlineAndPipelining(t *testing.T) {
	addr, _ := start(t)
	cl := dial(t, addr)
	io.WriteString(cl.c, "SET a 1\r\nSET b 2\r\nGET a\r\nGET b\r\nDBSIZE\r\n")
	for _, want := range []string{"+OK", "+OK", "1", "2", ":2"} {
		if got := cl.reply(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	addr, _ := start(t)
	cl := dial(t, addr)
	io.WriteString(cl.c, "*1\r\n:oops\r\n")
	if got := cl.reply(); !strings.HasPrefix(got, "-ERR protocol error") {
		t.Fatalf("got %q", got)
	}
	if _, err := cl.r.ReadByte(); err != io.EOF {
		t.Fatalf("expected close, got %v", err)
	}
}

func TestManyClients(t *testing.T) {
	addr, db := start(t)
	done := make(chan error)
	for i := 0; i < 10; i++ {
		go func(i int) {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			for j := 0; j < 50; j++ {
				fmt.Fprintf(c, "SET c%d-%d v\r\n", i, j)
				if line, err := r.ReadString('\n'); err != nil || line != "+OK\r\n" {
					done <- fmt.Errorf("reply %q err %v", line, err)
					return
				}
			}
			done <- nil
		}(i)
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if db.Len() != 500 {
		t.Fatalf("Len = %d, want 500", db.Len())
	}
}

func TestCloseDisconnectsClients(t *testing.T) {
	db, _ := caskdb.Open(t.TempDir(), nil)
	defer db.Close()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	s := New(db)
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ln) }()
	cl := dial(t, ln.Addr().String())
	if got := cl.do("PING"); got != "+PONG" {
		t.Fatal(got)
	}
	s.Close()
	if err := <-errc; err != ErrServerClosed {
		t.Fatalf("Serve returned %v", err)
	}
	if _, err := cl.r.ReadByte(); err == nil {
		t.Fatal("client still connected")
	}
}
