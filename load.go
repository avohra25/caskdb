package caskdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// prepareDir finishes or discards interrupted merges before loading.
func (db *DB) prepareDir() error {
	if err := db.finishManifest(); err != nil {
		return err
	}
	ents, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), tmpExt) {
			if err := os.Remove(filepath.Join(db.dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// canAppend reports whether an existing data file may become the active
// file. Merge outputs carry a hint file describing their exact contents, so
// they are never appended to.
func (db *DB) canAppend(id uint32) bool {
	_, err := os.Stat(fileName(db.dir, id, hintExt))
	return errors.Is(err, os.ErrNotExist)
}

// loadFile rebuilds keydir entries from one data file, preferring its hint
// file when a valid one exists. A torn tail on the newest file (a crash
// mid-append) is truncated away; corruption anywhere else is reported.
func (db *DB) loadFile(id uint32, f *os.File, last bool) error {
	if ok, err := db.loadHint(id); err != nil || ok {
		return err
	}
	end, err := scanRecords(f, func(r *record, off int64, size uint32) error {
		db.applyLoaded(string(r.key), r.tombstone(), entry{fileID: id, offset: off, size: size, ts: r.ts, expires: r.expires})
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

// loadHint applies a hint file if one exists and is intact. A damaged hint
// is ignored (the data file is scanned instead) because hints are only an
// optimisation.
func (db *DB) loadHint(id uint32) (bool, error) {
	data, err := os.ReadFile(fileName(db.dir, id, hintExt))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	hints, err := decodeHints(data)
	if err != nil {
		return false, nil
	}
	for _, h := range hints {
		db.applyLoaded(h.key, false, entry{fileID: id, offset: h.offset, size: h.size, ts: h.ts, expires: h.expires})
	}
	return true, nil
}
