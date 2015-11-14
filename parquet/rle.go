package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Implementation of RLE/Bit-Packing Hybrid encoding

// <encoded-data> part of the following spec:
//
// rle-bit-packed-hybrid: <length> <encoded-data>
// length := length of the <encoded-data> in bytes stored as 4 bytes little endian
// encoded-data := <run>*
// run := <bit-packed-run> | <rle-run>
// bit-packed-run := <bit-packed-header> <bit-packed-values>
// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
// // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
// bit-pack-count := (number of values in this run) / 8
// bit-packed-values := *see 1 below*
// rle-run := <rle-header> <repeated-value>
// rle-header := varint-encode( (number of times repeated) << 1)
// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)

type rleDecoder struct {
	data  []byte
	width int
	e     error
	i     int
	eod   bool

	// rle
	rleCount uint32
	rleValue int32
	//header  uint32 // uint32 is enough because parquet-mr uses java int
}

var (
	ErrInvalidBitWidth = errors.New("bitWidth but be >=0 and <= 32")
)

func newRLEDecoder(data []byte, bitWidth int) (*rleDecoder, error) {

	if bitWidth < 0 || bitWidth > 32 {
		return nil, ErrInvalidBitWidth
	}

	d := rleDecoder{
		data:  data,
		width: bitWidth,
	}
	d.readRunHeader()

	return &d, nil
}

func (d *rleDecoder) readRLERunValue() {
	byteWidth := (d.width + 7) / 8 // TODO: remember this in d
	n := d.i + byteWidth
	if n > len(d.data) {
		d.e = fmt.Errorf("cannot read RLE run value (no more data)")
		return
	}
	switch byteWidth {
	case 1:
		d.rleValue = int32(d.data[d.i])
	case 2:
		d.rleValue = int32(binary.LittleEndian.Uint16(d.data[d.i:n]))
	case 3:
		b3 := d.data[d.i]
		b2 := d.data[d.i+1]
		b1 := d.data[d.i+2]
		d.rleValue = int32(b3) + int32(b2)<<8 + int32(b1)<<16
	case 4:
		d.rleValue = int32(binary.LittleEndian.Uint32(d.data[d.i:n]))
	default:
		panic("should not happen")
	}
	d.i = n
}

func (d *rleDecoder) readRunHeader() {
	if d.i >= len(d.data) {
		d.eod = true
		return
	}

	h, n := binary.Uvarint(d.data[d.i:])
	if n <= 0 || h > math.MaxUint32 {
		d.e = fmt.Errorf("failed to read RLE run header at pos %d. Uvarint result: (%d, %d)", d.i, h, n)
	}
	d.i += n
	if h&1 == 1 {
		// bit packed run
		panic("nyi")
	} else {
		d.rleCount = uint32(h >> 1)
		d.readRLERunValue()
	}
}

func (d *rleDecoder) nextInt32() int32 {
	if d.rleCount > 0 {
		d.rleCount--
		if d.rleCount == 0 {
			d.readRunHeader()
		}
		return d.rleValue
	}
	panic("nyi")
}

func (d *rleDecoder) hasNext() bool {
	return !d.eod && d.e == nil
}

func (d *rleDecoder) err() error {
	return d.e
}
