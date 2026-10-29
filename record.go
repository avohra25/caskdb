package caskdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// On-disk record layout (little endian):
//
//	crc32   uint32  checksum of everything after this field
//	ts      uint64  logical timestamp (monotonic per database)
//	flags   uint8   bit 0 = tombstone, bit 1 = has expiry
//	ksize   uint32
//	vsize   uint32  value bytes, plus 8 when the record has an expiry
//	key     [ksize]byte
//	expiry  int64   unix milliseconds; present only when bit 1 is set
//	value   [vsize-8 or vsize]byte
//
// Records written before expiry support never set bit 1, so they decode
// unchanged.
const headerSize = 4 + 8 + 1 + 4 + 4

const (
	flagTombstone byte = 1 << 0
	flagExpires   byte = 1 << 1
	knownFlags         = flagTombstone | flagExpires
)

const expirySize = 8

// ErrCorrupt reports a record that failed its checksum or has an
// impossible header.
var ErrCorrupt = errors.New("caskdb: corrupt record")

type record struct {
	ts      uint64
	flags   byte
	expires int64 // unix milliseconds, 0 = never expires
	key     []byte
	value   []byte
}

func (r *record) tombstone() bool { return r.flags&flagTombstone != 0 }

func (r *record) bodyExtra() int {
	if r.expires != 0 {
		return expirySize
	}
	return 0
}

func (r *record) size() int { return headerSize + len(r.key) + r.bodyExtra() + len(r.value) }

func encodeRecord(r *record) []byte {
	buf := make([]byte, r.size())
	flags := r.flags &^ flagExpires
	extra := r.bodyExtra()
	if extra != 0 {
		flags |= flagExpires
	}
	binary.LittleEndian.PutUint64(buf[4:], r.ts)
	buf[12] = flags
	binary.LittleEndian.PutUint32(buf[13:], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[17:], uint32(extra+len(r.value)))
	copy(buf[headerSize:], r.key)
	if extra != 0 {
		binary.LittleEndian.PutUint64(buf[headerSize+len(r.key):], uint64(r.expires))
	}
	copy(buf[headerSize+len(r.key)+extra:], r.value)
	binary.LittleEndian.PutUint32(buf[0:], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

type header struct {
	crc   uint32
	ts    uint64
	flags byte
	ksize uint32
	vsize uint32
}

func parseHeader(b []byte) (header, error) {
	h := header{
		crc:   binary.LittleEndian.Uint32(b[0:]),
		ts:    binary.LittleEndian.Uint64(b[4:]),
		flags: b[12],
		ksize: binary.LittleEndian.Uint32(b[13:]),
		vsize: binary.LittleEndian.Uint32(b[17:]),
	}
	maxV := uint32(MaxValueSize)
	if h.flags&flagExpires != 0 {
		maxV += expirySize
	}
	switch {
	case h.ksize == 0 || h.ksize > MaxKeySize || h.vsize > maxV || h.flags&^knownFlags != 0:
		return h, fmt.Errorf("%w: bad header", ErrCorrupt)
	case h.flags&flagExpires != 0 && (h.flags&flagTombstone != 0 || h.vsize < expirySize):
		return h, fmt.Errorf("%w: bad expiry header", ErrCorrupt)
	}
	return h, nil
}

// decodeRecord decodes one complete record held in buf.
func decodeRecord(buf []byte) (*record, error) {
	if len(buf) < headerSize {
		return nil, fmt.Errorf("%w: short record", ErrCorrupt)
	}
	h, err := parseHeader(buf)
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) != int64(headerSize)+int64(h.ksize)+int64(h.vsize) {
		return nil, fmt.Errorf("%w: length mismatch", ErrCorrupt)
	}
	if crc32.ChecksumIEEE(buf[4:]) != h.crc {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	k := headerSize + int(h.ksize)
	rec := &record{ts: h.ts, flags: h.flags, key: buf[headerSize:k]}
	if h.flags&flagExpires != 0 {
		rec.expires = int64(binary.LittleEndian.Uint64(buf[k:]))
		if rec.expires == 0 {
			return nil, fmt.Errorf("%w: zero expiry", ErrCorrupt)
		}
		k += expirySize
	}
	rec.value = buf[k:]
	return rec, nil
}

// scanRecords reads records sequentially from r, calling fn for each valid
// one with its offset and encoded size. It returns the offset just past the
// last valid record. A torn or corrupt tail is reported as an error wrapping
// ErrCorrupt, with end still pointing at the last good boundary.
func scanRecords(r io.Reader, fn func(rec *record, off int64, size uint32) error) (end int64, err error) {
	br := bufio.NewReaderSize(r, 64<<10)
	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			if err == io.EOF {
				return end, nil
			}
			if err == io.ErrUnexpectedEOF {
				return end, fmt.Errorf("%w: truncated header at offset %d", ErrCorrupt, end)
			}
			return end, err
		}
		h, err := parseHeader(hdr)
		if err != nil {
			return end, fmt.Errorf("%w at offset %d", err, end)
		}
		size := headerSize + int(h.ksize) + int(h.vsize)
		buf := make([]byte, size)
		copy(buf, hdr)
		if _, err := io.ReadFull(br, buf[headerSize:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return end, fmt.Errorf("%w: truncated body at offset %d", ErrCorrupt, end)
			}
			return end, err
		}
		rec, err := decodeRecord(buf)
		if err != nil {
			return end, fmt.Errorf("%w at offset %d", err, end)
		}
		if err := fn(rec, end, uint32(size)); err != nil {
			return end, err
		}
		end += int64(size)
	}
}
