package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Limits that protect the server from hostile or broken clients.
const (
	maxArgs      = 1024
	maxBulkLen   = 64<<20 + 1
	maxInlineLen = 64 << 10
)

var errProtocol = errors.New("protocol error")

// readCommand reads one client command, either a RESP array of bulk strings
// (what redis-cli and client libraries send) or an inline space-separated
// line (handy with telnet or nc).
func readCommand(r *bufio.Reader) ([][]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		fields := strings.Fields(string(line))
		args := make([][]byte, len(fields))
		for i, f := range fields {
			args[i] = []byte(f)
		}
		return args, nil
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 || n > maxArgs {
		return nil, fmt.Errorf("%w: invalid multibulk length", errProtocol)
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("%w: expected '$', got %q", errProtocol, hdr)
		}
		size, err := strconv.Atoi(string(hdr[1:]))
		if err != nil || size < 0 || size > maxBulkLen {
			return nil, fmt.Errorf("%w: invalid bulk length", errProtocol)
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, fmt.Errorf("%w: bulk string not CRLF terminated", errProtocol)
		}
		args = append(args, buf[:size])
	}
	return args, nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, chunk...)
		if len(line) > maxInlineLen {
			return nil, fmt.Errorf("%w: line too long", errProtocol)
		}
		if !isPrefix {
			return line, nil
		}
	}
}

// Reply writers.

func writeSimple(w *bufio.Writer, s string) { w.WriteString("+" + s + "\r\n") }

func writeError(w *bufio.Writer, s string) { w.WriteString("-" + s + "\r\n") }

func writeInt(w *bufio.Writer, n int64) {
	w.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func writeBulk(w *bufio.Writer, b []byte) {
	if b == nil {
		w.WriteString("$-1\r\n")
		return
	}
	w.WriteString("$" + strconv.Itoa(len(b)) + "\r\n")
	w.Write(b)
	w.WriteString("\r\n")
}

func writeArray(w *bufio.Writer, items []string) {
	w.WriteString("*" + strconv.Itoa(len(items)) + "\r\n")
	for _, it := range items {
		writeBulk(w, []byte(it))
	}
}
