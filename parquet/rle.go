package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// Implementation of RLE/Bit-Packing Hybrid encoding

// rle-bit-packed-hybrid: <length> <encoded-data>
// length := length of the <encoded-data> in bytes stored as 4 bytes little endian (unsigned int32)
// encoded-data := <run>*
// run := <bit-packed-run> | <rle-run>
// bit-packed-run := <bit-packed-header> <bit-packed-values>
// bit-packed-header := varint-encode(<bit-pack-scaled-run-len> << 1 | 1)
// // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
// bit-pack-scaled-run-len := (bit-packed-run-len) / 8
// bit-packed-run-len := *see 3 below*
// bit-packed-values := *see 1 below*
// rle-run := <rle-header> <repeated-value>
// rle-header := varint-encode( (rle-run-len) << 1)
// rle-run-len := *see 3 below*
// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)

type rleDecoder struct {
	bitWidth     int
	rleValueSize int
	bpUnpacker   unpack8int32Func

	data []byte
	pos  int

	// rle
	rleCount uint32
	rleValue int32

	// bit-packed
	bpCount  uint32
	bpRunPos uint8
	bpRun    [8]int32
}

// newRLEDecoder creates a new RLE decoder with bit-width w
func newRLEDecoder(w int) *rleDecoder {
	// TODO: support w = 0 or not (used in dict.go:28)
	return &rleDecoder{
		bitWidth:     w,
		rleValueSize: (w + 7) / 8,
		bpUnpacker:   unpack8Int32FuncByWidth[w],
	}
}

func (d *rleDecoder) init(data []byte) {
	d.data = data
	d.pos = 0
	d.bpCount = 0
	d.bpRunPos = 0
	d.rleCount = 0
}

func (d *rleDecoder) next() (next int32, err error) {
	if d.rleCount == 0 && d.bpCount == 0 && d.bpRunPos == 0 {
		if err = d.readRunHeader(); err != nil {
			return 0, err
		}
	}

	switch {
	case d.rleCount > 0:
		next = d.rleValue
		d.rleCount--
	case d.bpCount > 0 || d.bpRunPos > 0:
		if d.bpRunPos == 0 {
			if err = d.readBitPackedRun(); err != nil {
				return 0, err
			}
			d.bpCount--
		}
		next = d.bpRun[d.bpRunPos]
		d.bpRunPos = (d.bpRunPos + 1) % 8
	default:
		panic("should not happen")
	}

	return next, err
}

func (d *rleDecoder) decodeLevels(dst []uint16) error {
	for i := 0; i < len(dst); i++ {
		v, err := d.next()
		if err != nil {
			return err
		}
		dst[i] = uint16(v)
	}
	return nil
}

func (d *rleDecoder) readRLERunValue() error {
	pos := d.pos + d.rleValueSize
	if uint(pos) > uint(len(d.data)) {
		return errors.New("rle: not enough data to read RLE run value")
	}
	d.rleValue = decodeRLEValue(d.data[d.pos:pos])
	if bits.LeadingZeros32(uint32(d.rleValue)) < 32-d.bitWidth {
		return errors.New("rle: RLE run value is too large")
	}
	d.pos = pos
	return nil
}

func decodeRLEValue(bytes []byte) int32 {
	switch len(bytes) {
	case 1:
		return int32(bytes[0])
	case 2:
		return int32(bytes[0]) + int32(bytes[1])<<8
	case 3:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16
	case 4:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16 + int32(bytes[3])<<24
	default:
		panic("invalid argument")
	}
}

func (d *rleDecoder) readBitPackedRun() error {
	if d.pos >= len(d.data) {
		return errors.New("rle: not enough data to read bit-packed run")
	}
	pos := d.pos + d.bitWidth
	var data []byte
	if uint(pos) > uint(len(d.data)) {
		// TODO: return errNED correctly in this case (if possible)
		data = make([]byte, d.bitWidth, d.bitWidth)
		copy(data, d.data[d.pos:])
	} else {
		data = d.data[d.pos:pos]
	}
	d.bpRun = d.bpUnpacker(data)
	d.pos = pos
	return nil
}

func (d *rleDecoder) readRunHeader() error {
	if d.pos >= len(d.data) {
		return errNED
	}

	h, n := binary.Uvarint(d.data[d.pos:])
	if n <= 0 || h > math.MaxUint32 {
		return errors.New("rle: invalid run header")
	}
	d.pos += n
	if h&1 == 1 {
		d.bpCount = uint32(h >> 1)
		if d.bpCount == 0 {
			return fmt.Errorf("rle: empty bit-packed run")
		}
		d.bpRunPos = 0
	} else {
		d.rleCount = uint32(h >> 1)
		if d.rleCount == 0 {
			return fmt.Errorf("rle: empty RLE run")
		}
		return d.readRLERunValue()
	}
	return nil
}
