package caskdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Hint files sit next to merged data files and list every record in them, so
// startup can rebuild the keydir without reading values. Each entry is:
//
//	crc32  uint32  checksum of the rest of the entry
//	ts     uint64
//	offset uint64
//	size   uint32  encoded size of the data record
//	ksize  uint32  high bit set when an expiry follows the key
//	key    [ksize]byte
//	expiry int64   unix milliseconds; present only when the high bit is set
//
// Hint files written before expiry support never set the high bit (keys are
// at most 64 KiB), so they still decode.
const hintHeaderSize = 4 + 8 + 8 + 4 + 4

const hintHasExpiry = 1 << 31

type hint struct {
	key     string
	ts      uint64
	offset  int64
	size    uint32
	expires int64
}

func appendHint(dst []byte, h hint) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, hintHeaderSize)...)
	b := dst[start:]
	binary.LittleEndian.PutUint64(b[4:], h.ts)
	binary.LittleEndian.PutUint64(b[12:], uint64(h.offset))
	binary.LittleEndian.PutUint32(b[20:], h.size)
	ks := uint32(len(h.key))
	if h.expires != 0 {
		ks |= hintHasExpiry
	}
	binary.LittleEndian.PutUint32(b[24:], ks)
	dst = append(dst, h.key...)
	if h.expires != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(h.expires))
	}
	binary.LittleEndian.PutUint32(dst[start:], crc32.ChecksumIEEE(dst[start+4:]))
	return dst
}

func decodeHints(data []byte) ([]hint, error) {
	var out []hint
	for len(data) > 0 {
		if len(data) < hintHeaderSize {
			return nil, fmt.Errorf("%w: truncated hint", ErrCorrupt)
		}
		raw := binary.LittleEndian.Uint32(data[24:])
		ksize := raw &^ hintHasExpiry
		extra := 0
		if raw&hintHasExpiry != 0 {
			extra = 8
		}
		if ksize == 0 || ksize > MaxKeySize || len(data) < hintHeaderSize+int(ksize)+extra {
			return nil, fmt.Errorf("%w: bad hint key size", ErrCorrupt)
		}
		k := hintHeaderSize + int(ksize)
		n := k + extra
		if crc32.ChecksumIEEE(data[4:n]) != binary.LittleEndian.Uint32(data) {
			return nil, fmt.Errorf("%w: hint checksum mismatch", ErrCorrupt)
		}
		h := hint{
			key:    string(data[hintHeaderSize:k]),
			ts:     binary.LittleEndian.Uint64(data[4:]),
			offset: int64(binary.LittleEndian.Uint64(data[12:])),
			size:   binary.LittleEndian.Uint32(data[20:]),
		}
		if extra != 0 {
			h.expires = int64(binary.LittleEndian.Uint64(data[k:]))
			if h.expires == 0 {
				return nil, fmt.Errorf("%w: zero hint expiry", ErrCorrupt)
			}
		}
		out = append(out, h)
		data = data[n:]
	}
	return out, nil
}
