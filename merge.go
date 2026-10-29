package caskdb

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const manifestName = "MERGE_MANIFEST"

// Merge compacts the database: the live value of every key held in sealed
// data files is copied into fresh files (with hint files for fast startup),
// and the old files are deleted. Overwritten values and tombstones are
// dropped. Reads and writes continue while the copy runs; only the final
// index swap takes the write lock.
//
// Crash safety: outputs are written under temporary names, fsynced and
// renamed into place before a manifest of obsolete files is committed. If the
// process dies before the manifest exists, the merge simply never happened
// (leftover outputs only duplicate records with identical timestamps). If it
// dies afterwards, the next Open finishes deleting the listed files.
func (db *DB) Merge() error {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	// Seal the active file so everything written so far is immutable.
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.activeSize > 0 {
		if err := db.rotateLocked(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	old := make(map[uint32]bool)
	for id := range db.files {
		if id != db.activeID {
			old[id] = true
		}
	}
	type item struct {
		key string
		e   entry
	}
	var items []item
	for k, e := range db.keydir {
		if old[e.fileID] {
			items = append(items, item{k, e})
		}
	}
	db.mu.Unlock()
	if len(old) == 0 {
		return nil
	}

	w := &mergeWriter{db: db}
	moved := make([]entry, len(items))
	for i, it := range items {
		db.mu.RLock()
		rec, err := db.readLocked(it.e)
		db.mu.RUnlock()
		if err != nil {
			w.abort()
			return err
		}
		ne, err := w.write(rec)
		if err != nil {
			w.abort()
			return err
		}
		moved[i] = ne
	}
	outIDs, err := w.finish()
	if err != nil {
		w.abort()
		return err
	}

	oldIDs := make([]uint32, 0, len(old))
	for id := range old {
		oldIDs = append(oldIDs, id)
	}
	if err := writeManifest(db.dir, oldIDs); err != nil {
		return err
	}

	db.mu.Lock()
	for _, id := range outIDs {
		f, err := os.Open(fileName(db.dir, id, dataExt))
		if err != nil {
			db.mu.Unlock()
			return err
		}
		db.files[id] = f
	}
	for i, it := range items {
		if cur, ok := db.keydir[it.key]; ok && cur == it.e {
			db.keydir[it.key] = moved[i]
		}
	}
	for _, id := range oldIDs {
		db.files[id].Close()
		delete(db.files, id)
	}
	db.mu.Unlock()
	return db.finishManifest()
}

type mergeWriter struct {
	db    *DB
	ids   []uint32
	data  *bufio.Writer
	file  *os.File
	size  int64
	hints []byte
}

func (w *mergeWriter) open() error {
	w.db.mu.Lock()
	id := w.db.nextID
	w.db.nextID++
	w.db.mu.Unlock()
	f, err := os.OpenFile(fileName(w.db.dir, id, dataExt+tmpExt), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	w.ids = append(w.ids, id)
	w.file, w.data, w.size, w.hints = f, bufio.NewWriterSize(f, 256<<10), 0, w.hints[:0]
	return nil
}

func (w *mergeWriter) write(rec *record) (entry, error) {
	buf := encodeRecord(rec)
	if w.file != nil && w.size > 0 && w.size+int64(len(buf)) > w.db.opts.MaxFileSize {
		if err := w.seal(); err != nil {
			return entry{}, err
		}
	}
	if w.file == nil {
		if err := w.open(); err != nil {
			return entry{}, err
		}
	}
	if _, err := w.data.Write(buf); err != nil {
		return entry{}, err
	}
	e := entry{fileID: w.ids[len(w.ids)-1], offset: w.size, size: uint32(len(buf)), ts: rec.ts, expires: rec.expires}
	w.hints = appendHint(w.hints, hint{key: string(rec.key), ts: rec.ts, offset: e.offset, size: e.size, expires: rec.expires})
	w.size += int64(len(buf))
	return e, nil
}

// seal flushes the current output and writes its hint file, both still under
// temporary names.
func (w *mergeWriter) seal() error {
	id := w.ids[len(w.ids)-1]
	if err := w.data.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	return writeFileSync(fileName(w.db.dir, id, hintExt+tmpExt), w.hints)
}

// finish seals the last output and renames every output into place.
func (w *mergeWriter) finish() ([]uint32, error) {
	if w.file != nil {
		if err := w.seal(); err != nil {
			return nil, err
		}
	}
	for _, id := range w.ids {
		for _, ext := range []string{dataExt, hintExt} {
			if err := os.Rename(fileName(w.db.dir, id, ext+tmpExt), fileName(w.db.dir, id, ext)); err != nil {
				return nil, err
			}
		}
	}
	return w.ids, syncDir(w.db.dir)
}

func (w *mergeWriter) abort() {
	if w.file != nil {
		w.file.Close()
	}
	for _, id := range w.ids {
		for _, ext := range []string{dataExt, hintExt} {
			os.Remove(fileName(w.db.dir, id, ext+tmpExt))
		}
	}
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeManifest(dir string, ids []uint32) error {
	var sb strings.Builder
	for _, id := range ids {
		fmt.Fprintln(&sb, id)
	}
	tmp := filepath.Join(dir, manifestName+tmpExt)
	if err := writeFileSync(tmp, []byte(sb.String())); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// finishManifest deletes the files named by a committed merge manifest, then
// removes the manifest itself.
func (db *DB) finishManifest() error {
	path := filepath.Join(db.dir, manifestName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, line := range strings.Fields(string(data)) {
		n, err := strconv.ParseUint(line, 10, 32)
		if err != nil {
			return fmt.Errorf("caskdb: bad merge manifest entry %q", line)
		}
		for _, ext := range []string{dataExt, hintExt} {
			if err := os.Remove(fileName(db.dir, uint32(n), ext)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDir(db.dir)
}
