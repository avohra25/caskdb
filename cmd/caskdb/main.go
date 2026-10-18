// Command caskdb is a command-line front end for a caskdb database: it can
// serve a directory over the Redis protocol or run one-off operations on it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/avohra25/caskdb"
	"github.com/avohra25/caskdb/server"
)

const usage = `usage: caskdb <command> [flags] [args]

commands:
  serve                 serve the database over the Redis protocol
  get <key>             print the value stored under key
  put <key> [value]     store value (read from stdin when omitted)
  del <key>...          delete keys
  keys                  list keys in sorted order
  merge                 compact data files
  stats                 print storage statistics

flags:
  -dir path             database directory (default "./data")
  -addr host:port       listen address for serve (default "127.0.0.1:6380")
  -max-file-size bytes  data file rotation size (default 64 MiB)
  -sync                 fsync after every write
`

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "caskdb:", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("invalid usage (run 'caskdb help')")

func run(argv []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(argv) == 0 || argv[0] == "help" || argv[0] == "-h" || argv[0] == "--help" {
		fmt.Fprint(stdout, usage)
		return nil
	}
	cmd := argv[0]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "./data", "database directory")
	addr := fs.String("addr", "127.0.0.1:6380", "listen address")
	maxSize := fs.Int64("max-file-size", caskdb.DefaultMaxFileSize, "rotation size in bytes")
	syncPut := fs.Bool("sync", false, "fsync after every write")
	if err := fs.Parse(argv[1:]); err != nil {
		return errUsage
	}
	args := fs.Args()

	db, err := caskdb.Open(*dir, &caskdb.Options{MaxFileSize: *maxSize, SyncOnPut: *syncPut})
	if err != nil {
		return err
	}
	defer db.Close()

	switch cmd {
	case "serve":
		return serve(db, *addr, stderr)
	case "get":
		if len(args) != 1 {
			return errUsage
		}
		v, err := db.Get([]byte(args[0]))
		if err != nil {
			return err
		}
		stdout.Write(v)
		if len(v) == 0 || v[len(v)-1] != '\n' {
			fmt.Fprintln(stdout)
		}
	case "put":
		if len(args) < 1 || len(args) > 2 {
			return errUsage
		}
		var v []byte
		if len(args) == 2 {
			v = []byte(args[1])
		} else if v, err = io.ReadAll(io.LimitReader(stdin, caskdb.MaxValueSize+1)); err != nil {
			return err
		}
		return db.Put([]byte(args[0]), v)
	case "del":
		if len(args) == 0 {
			return errUsage
		}
		for _, k := range args {
			if err := db.Delete([]byte(k)); err != nil {
				return err
			}
		}
	case "keys":
		for _, k := range db.Keys() {
			fmt.Fprintln(stdout, k)
		}
	case "merge":
		before, _ := db.Stats()
		if err := db.Merge(); err != nil {
			return err
		}
		after, _ := db.Stats()
		fmt.Fprintf(stdout, "merged: %d -> %d bytes on disk\n", before.DiskBytes, after.DiskBytes)
	case "stats":
		st, err := db.Stats()
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "keys:       %d\ndata files: %d\ndisk bytes: %d\nlive bytes: %d\n",
			st.Keys, st.DataFiles, st.DiskBytes, st.LiveBytes)
	default:
		return fmt.Errorf("unknown command %q (run 'caskdb help')", cmd)
	}
	return nil
}

func serve(db *caskdb.DB, addr string, stderr io.Writer) error {
	logger := log.New(stderr, "", log.LstdFlags)
	s := server.New(db)
	s.Logger = logger
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Print("shutting down")
		s.Close()
	}()
	logger.Printf("caskdb listening on %s", addr)
	if err := s.ListenAndServe(addr); err != nil && !errors.Is(err, server.ErrServerClosed) {
		return err
	}
	return nil
}
