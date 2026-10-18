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
//	ksize  uint32
//	key    [ksize]byte
const hintHeaderSize = 4 + 8 + 8 + 4 + 4

type hint struct {
	key    string
	ts     uint64
	offset int64
	size   uint32
}

func appendHint(dst []byte, h hint) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, hintHeaderSize)...)
	b := dst[start:]
	binary.LittleEndian.PutUint64(b[4:], h.ts)
	binary.LittleEndian.PutUint64(b[12:], uint64(h.offset))
	binary.LittleEndian.PutUint32(b[20:], h.size)
	binary.LittleEndian.PutUint32(b[24:], uint32(len(h.key)))
	dst = append(dst, h.key...)
	binary.LittleEndian.PutUint32(dst[start:], crc32.ChecksumIEEE(dst[start+4:]))
	return dst
}

func decodeHints(data []byte) ([]hint, error) {
	var out []hint
	for len(data) > 0 {
		if len(data) < hintHeaderSize {
			return nil, fmt.Errorf("%w: truncated hint", ErrCorrupt)
		}
		ksize := binary.LittleEndian.Uint32(data[24:])
		if ksize == 0 || ksize > MaxKeySize || len(data) < hintHeaderSize+int(ksize) {
			return nil, fmt.Errorf("%w: bad hint key size", ErrCorrupt)
		}
		n := hintHeaderSize + int(ksize)
		if crc32.ChecksumIEEE(data[4:n]) != binary.LittleEndian.Uint32(data) {
			return nil, fmt.Errorf("%w: hint checksum mismatch", ErrCorrupt)
		}
		out = append(out, hint{
			key:    string(data[hintHeaderSize:n]),
			ts:     binary.LittleEndian.Uint64(data[4:]),
			offset: int64(binary.LittleEndian.Uint64(data[12:])),
			size:   binary.LittleEndian.Uint32(data[20:]),
		})
		data = data[n:]
	}
	return out, nil
}
