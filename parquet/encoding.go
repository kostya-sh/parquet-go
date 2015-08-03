package parquet

import (
	"errors"
	"fmt"
)

var errUnexpectedEndOfInput = errors.New("unexpected end of input")
var errInputTooLarge = errors.New("input too large")

// readVarInt reads ULEB-128 encoded integer value from src[i:] and returns
// encoded value and the next position in src to read from.
//
// See https://en.wikipedia.org/wiki/Variable-length_quantity
func readVarInt(src []byte, i int) (uint64, int, error) {
	var val uint64
	j := i
	for {
		b := src[j]
		val += uint64(b & 0x7F)
		if b&0x80 == 0 {
			break
		}
		val <<= 7
		j++
		if j >= len(src) {
			return 0, 0, errUnexpectedEndOfInput
		}
		if j-i > 7 {
			return 0, 0, errInputTooLarge
		}
	}
	return val, j + 1, nil
}

// maximum supported value for writeVarInt
const maxWriteVarIntValue = 1<<56 - 1

// writeVarInt appends ULEB-128 representation of val to dst.
//
// See https://en.wikipedia.org/wiki/Variable-length_quantity
func writeVarInt(dst []byte, val uint64) ([]byte, error) {
	if val > maxWriteVarIntValue {
		// TODO: get rid of this limit if possible without affecting performance
		return nil, fmt.Errorf("writeVarInt: %d > %d", val, maxWriteVarIntValue)
	}

	buf := uint64(val & 0x7F)
	for {
		val >>= 7
		if val == 0 {
			break
		}
		buf <<= 8
		buf |= ((val & 0x7F) | 0x80)
	}

	for {
		dst = append(dst, byte(buf))
		if buf&0x80 == 0 {
			break
		}
		buf >>= 8
	}

	return dst, nil
}
