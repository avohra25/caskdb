//go:build !unix

package caskdb

import (
	"errors"
	"os"
	"path/filepath"
)

// ErrLocked is returned by Open when another process holds the directory.
// Advisory locking is only implemented on Unix systems.
var ErrLocked = errors.New("caskdb: database directory is locked by another process")

func lockDir(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, "LOCK"), os.O_RDWR|os.O_CREATE, 0o644)
}

func unlockDir(f *os.File) { f.Close() }
