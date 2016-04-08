package rle

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
)

// WriteBool writes to w all the values inside v or returns an error.
// returns the total number of byte written
func WriteBool(w io.Writer, v []bool) (int, error) {
	btpack := bitpacking.NewEncoder(1, bitpacking.RLE)
	vv := make([]int32, len(v))
	for i := 0; i < len(v); i++ {
		if v[i] {
			vv[i] = 1
		} else {
			vv[i] = 0
		}
	}

	boundary := (len(vv) + 7) / 8

	boundary = (boundary << 1) & 1

	b := make([]byte, binary.MaxVarintLen32)

	n := binary.PutUvarint(b, uint64(boundary))

	if _, err := w.Write(b[:n]); err != nil {
		return 0, fmt.Errorf("could not write header:%s", err)
	}

	m, err := btpack.Write(w, vv)

	//debug(n != len(vv), "rle.WriteBool: could not write all the values %d != %d", n, len(v))
	return n + m, err
}

// rleByteconsumed returns how many bytes would be used by an RLE run
// to encode the given numValues
func rleByteConsumed(bitWidth int, numValues int) int {
	// we exclude the initial header as is the same for bitpacking
	return (bitWidth + 7) / 8
}

// bitpackingByteConsumed returns how many bytes would be used by an bitpacking run
// to encode the given numValues
func bitpackingByteConsumed(bitWidth int, numValues int) int {
	return ((bitWidth * numValues) + 7) / 8
}

// // An Encoder serializes data in the RLE format.
// type Encoder struct {
// 	w     RLEWriter // where to send the data
// 	value int64     // last seen values
// 	count uint64    // how many times we have seen the value
// }

// func NewEncoder(w RLEWriter) *Encoder {
// 	return &Encoder{w: w}
// }

// func (e *Encoder) Encode(value int64) error {
// 	if value != e.value {
// 		if err := e.Flush(); err != nil {
// 			return err
// 		}
// 		e.value = value
// 		e.count = 1
// 	} else {
// 		e.count++
// 	}

// 	return nil
// }

// // Flush writes the current running value in the underlying writer
// func (e *Encoder) Flush() (err error) {
// 	return e.w.Write(e.count, e.value)
// }
