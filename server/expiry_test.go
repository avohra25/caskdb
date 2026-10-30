package server

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/avohra25/caskdb"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func startClock(t *testing.T) (*client, *clock) {
	t.Helper()
	clk := &clock{t: time.UnixMilli(1_800_000_000_000)}
	db, err := caskdb.Open(t.TempDir(), &caskdb.Options{Now: clk.Now})
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
	return dial(t, ln.Addr().String()), clk
}

type step struct {
	args    []string
	want    string
	advance time.Duration // applied before the command
}

func run(t *testing.T, cl *client, clk *clock, steps []step) {
	t.Helper()
	for _, s := range steps {
		clk.Advance(s.advance)
		if got := cl.do(s.args...); got != s.want {
			t.Errorf("%v = %q, want %q", s.args, got, s.want)
		}
	}
}

func TestSetExpiryOptions(t *testing.T) {
	cl, clk := startClock(t)
	nowS := strconv.FormatInt(clk.Now().Unix(), 10)
	run(t, cl, clk, []step{
		{args: []string{"SET", "s", "v", "EX", "10"}, want: "+OK"},
		{args: []string{"TTL", "s"}, want: ":10"},
		{args: []string{"PTTL", "s"}, want: ":10000"},
		{args: []string{"TTL", "s"}, want: ":9", advance: 1400 * time.Millisecond},
		{args: []string{"TTL", "s"}, want: ":8", advance: 600 * time.Millisecond},
		{args: []string{"GET", "s"}, want: "(nil)", advance: 8500 * time.Millisecond},
		{args: []string{"TTL", "s"}, want: ":-2"},

		{args: []string{"set", "p", "v", "px", "250"}, want: "+OK"},
		{args: []string{"PTTL", "p"}, want: ":250"},
		{args: []string{"EXISTS", "p"}, want: ":0", advance: 250 * time.Millisecond},

		{args: []string{"SET", "a", "v", "EXAT", nowS}, want: "+OK"}, // already past: deleted
		{args: []string{"EXISTS", "a"}, want: ":0"},
	})
	run(t, cl, clk, []step{
		{args: []string{"SET", "a", "v", "PXAT", strconv.FormatInt(clk.Now().UnixMilli()+5000, 10)}, want: "+OK"},
		{args: []string{"PTTL", "a"}, want: ":5000"},

		{args: []string{"SET", "k", "v1", "EX", "100"}, want: "+OK"},
		{args: []string{"SET", "k", "v2", "KEEPTTL"}, want: "+OK"},
		{args: []string{"TTL", "k"}, want: ":100"},
		{args: []string{"SET", "k", "v3"}, want: "+OK"},
		{args: []string{"TTL", "k"}, want: ":-1"},
		{args: []string{"SET", "fresh", "v", "KEEPTTL"}, want: "+OK"},
		{args: []string{"TTL", "fresh"}, want: ":-1"},

		{args: []string{"SET", "x", "v", "EX", "0"}, want: "-ERR invalid expire time in 'set' command"},
		{args: []string{"SET", "x", "v", "EX", "abc"}, want: "-ERR invalid expire time in 'set' command"},
		{args: []string{"SET", "x", "v", "EX", "99999999999999999"}, want: "-ERR invalid expire time in 'set' command"},
		{args: []string{"SET", "x", "v", "EX", "1", "PX", "1"}, want: "-ERR syntax error"},
		{args: []string{"SET", "x", "v", "EX", "1", "KEEPTTL"}, want: "-ERR syntax error"},
		{args: []string{"SET", "x", "v", "NX", "XX"}, want: "-ERR syntax error"},
		{args: []string{"SET", "x", "v", "EX"}, want: "-ERR syntax error"},
		{args: []string{"SET", "x", "v", "BOGUS"}, want: "-ERR syntax error"},
		{args: []string{"EXISTS", "x"}, want: ":0"},
	})
}

func TestSetConditionsAndGet(t *testing.T) {
	cl, clk := startClock(t)
	run(t, cl, clk, []step{
		{args: []string{"SET", "lock", "a", "NX", "PX", "1000"}, want: "+OK"},
		{args: []string{"SET", "lock", "b", "NX", "PX", "1000"}, want: "(nil)"},
		{args: []string{"GET", "lock"}, want: "a"},
		{args: []string{"SET", "lock", "b", "NX", "PX", "1000"}, want: "+OK", advance: time.Second},
		{args: []string{"GET", "lock"}, want: "b"},

		{args: []string{"SET", "missing", "v", "XX"}, want: "(nil)"},
		{args: []string{"EXISTS", "missing"}, want: ":0"},
		{args: []string{"SET", "lock", "c", "XX"}, want: "+OK"},

		{args: []string{"SET", "lock", "d", "GET"}, want: "c"},
		{args: []string{"SET", "new", "v", "GET"}, want: "(nil)"},
		{args: []string{"SET", "new", "w", "NX", "GET"}, want: "v"}, // not applied, old value returned
		{args: []string{"GET", "new"}, want: "v"},
		{args: []string{"SETEX", "se", "5", "v"}, want: "+OK"},
		{args: []string{"TTL", "se"}, want: ":5"},
		{args: []string{"PSETEX", "pse", "1500", "v"}, want: "+OK"},
		{args: []string{"PTTL", "pse"}, want: ":1500"},
		{args: []string{"SETEX", "se", "-1", "v"}, want: "-ERR invalid expire time in 'setex' command"},
	})
}

func TestExpireFamily(t *testing.T) {
	cl, clk := startClock(t)
	at := strconv.FormatInt(clk.Now().Unix()+60, 10)
	run(t, cl, clk, []step{
		{args: []string{"EXPIRE", "nope", "10"}, want: ":0"},
		{args: []string{"SET", "k", "v"}, want: "+OK"},
		{args: []string{"EXPIRE", "k", "10", "XX"}, want: ":0"},
		{args: []string{"EXPIRE", "k", "10", "GT"}, want: ":0"}, // permanent counts as infinite
		{args: []string{"EXPIRE", "k", "10", "NX"}, want: ":1"},
		{args: []string{"EXPIRE", "k", "20", "NX"}, want: ":0"},
		{args: []string{"EXPIRE", "k", "5", "GT"}, want: ":0"},
		{args: []string{"EXPIRE", "k", "20", "GT"}, want: ":1"},
		{args: []string{"TTL", "k"}, want: ":20"},
		{args: []string{"PEXPIRE", "k", "30000", "LT"}, want: ":0"},
		{args: []string{"PEXPIRE", "k", "3000", "LT"}, want: ":1"},
		{args: []string{"PTTL", "k"}, want: ":3000"},
		{args: []string{"EXPIREAT", "k", at}, want: ":1"},
		{args: []string{"TTL", "k"}, want: ":60"},
		{args: []string{"EXPIRETIME", "k"}, want: ":" + at},
		{args: []string{"PEXPIRETIME", "k"}, want: ":" + at + "000"},
		{args: []string{"PERSIST", "k"}, want: ":1"},
		{args: []string{"PERSIST", "k"}, want: ":0"},
		{args: []string{"TTL", "k"}, want: ":-1"},
		{args: []string{"EXPIRETIME", "k"}, want: ":-1"},
		{args: []string{"EXPIRETIME", "nope"}, want: ":-2"},
		{args: []string{"EXPIRE", "k", "10", "LT"}, want: ":1"},
		{args: []string{"EXPIRE", "k", "-1"}, want: ":1"}, // deletes
		{args: []string{"EXISTS", "k"}, want: ":0"},
		{args: []string{"SET", "k", "v"}, want: "+OK"},
		{args: []string{"PEXPIREAT", "k", "1"}, want: ":1"},
		{args: []string{"GET", "k"}, want: "(nil)"},
		{args: []string{"EXPIRE", "k", "x"}, want: "-ERR invalid expire time in 'expire' command"},
		{args: []string{"SET", "k", "v"}, want: "+OK"},
		{args: []string{"EXPIRE", "k", "1", "ZZ"}, want: "-ERR Unsupported option ZZ"},
		{args: []string{"TTL"}, want: "-ERR wrong number of arguments for 'ttl' command"},
	})
}

func TestExpiredKeysLeaveListings(t *testing.T) {
	cl, clk := startClock(t)
	run(t, cl, clk, []step{
		{args: []string{"SET", "a", "1"}, want: "+OK"},
		{args: []string{"SET", "b", "2", "EX", "1"}, want: "+OK"},
		{args: []string{"DBSIZE"}, want: ":2"},
		{args: []string{"KEYS", "*"}, want: "[a b]"},
		{args: []string{"DBSIZE"}, want: ":1", advance: time.Second},
		{args: []string{"KEYS", "*"}, want: "[a]"},
		{args: []string{"DEL", "b"}, want: ":0"},
	})
}

func TestSetNXIsAtomic(t *testing.T) {
	cl, _ := startClock(t)
	addr := cl.c.RemoteAddr().String()
	const n = 20
	var wg sync.WaitGroup
	wins := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dial(t, addr)
			if c.do("SET", "leader", strconv.Itoa(i), "NX", "EX", "30") == "+OK" {
				wins <- strconv.Itoa(i)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	var got []string
	for w := range wins {
		got = append(got, w)
	}
	if len(got) != 1 {
		t.Fatalf("%d clients won SET NX: %v", len(got), got)
	}
	if v := cl.do("GET", "leader"); v != got[0] {
		t.Fatalf("leader = %q, winner %q", v, got[0])
	}
}
