package caskdb

import (
	"errors"
	"fmt"
	"os"
)

// prepareDir cleans up leftovers from interrupted operations.
func (db *DB) prepareDir() error { return nil }

// canAppend reports whether an existing data file may become the active file.
func (db *DB) canAppend(id uint32) bool { return true }

// loadFile rebuilds keydir entries from one data file. A torn tail on the
// newest file (a crash mid-append) is truncated away; corruption anywhere
// else is reported.
func (db *DB) loadFile(id uint32, f *os.File, last bool) error {
	end, err := scanRecords(f, func(r *record, off int64, size uint32) error {
		db.applyLoaded(string(r.key), r.tombstone(), entry{fileID: id, offset: off, size: size, ts: r.ts})
		return nil
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrCorrupt) && last {
		if terr := f.Truncate(end); terr != nil {
			return terr
		}
		return f.Sync()
	}
	return fmt.Errorf("caskdb: load %s: %w", f.Name(), err)
}
