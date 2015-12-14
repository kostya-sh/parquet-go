package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Implementation of RLE/Bit-Packing Hybrid encoding

// encoded-data := <run>*
// run := <bit-packed-run> | <rle-run>
// bit-packed-run := <bit-packed-header> <bit-packed-values>
// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
//  (we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8)
// bit-pack-count := (number of values in this run) / 8
// bit-packed-values := bit packed values
// rle-run := <rle-header> <repeated-value>
// rle-header := varint-encode( (number of times repeated) << 1)
// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)

type rleDecoder struct {
	width int

	data []byte
	pos  int
	e    error
	eod  bool

	// rle
	rleCount uint32
	rleValue int32

	// bit-packed
	bpCount  uint32
	bpRunPos uint8
	bpRun    [8]int32
}

func newRLEDecoder(width int) *rleDecoder {
	if width <= 0 || width > 32 {
		panic("invalid width value")
	}
	d := rleDecoder{
		width: width,
	}
	return &d
}

func (d *rleDecoder) init(data []byte) {
	d.data = data
	d.pos = 0
	d.e = nil
	d.eod = false
	d.readRunHeader()
}

func (d *rleDecoder) readRLERunValue() {
	byteWidth := (d.width + 7) / 8 // TODO: remember this in d
	n := d.pos + byteWidth
	if n > len(d.data) {
		d.e = fmt.Errorf("cannot read RLE run value (not enough data)")
		return
	}
	switch byteWidth {
	case 1:
		d.rleValue = int32(d.data[d.pos])
	case 2:
		d.rleValue = int32(binary.LittleEndian.Uint16(d.data[d.pos:n]))
	case 3:
		b3 := d.data[d.pos]
		b2 := d.data[d.pos+1]
		b1 := d.data[d.pos+2]
		d.rleValue = int32(b3) + int32(b2)<<8 + int32(b1)<<16
	case 4:
		d.rleValue = int32(binary.LittleEndian.Uint32(d.data[d.pos:n]))
	default:
		panic("should not happen")
	}
	d.pos = n
}

func (d *rleDecoder) readBitPackedRun() {
	n := d.pos + d.width
	if n > len(d.data) {
		d.e = fmt.Errorf("cannot read bit-packed run (not enough data)")
		return
	}
	// TODO: remember unpack func in d
	d.bpRun = unpack8Int32FuncForWidth(d.width)(d.data[d.pos:n])
	d.pos = n
}

func (d *rleDecoder) readRunHeader() {
	if d.pos >= len(d.data) {
		d.eod = true
		return
	}

	h, n := binary.Uvarint(d.data[d.pos:])
	if n <= 0 || h > math.MaxUint32 {
		d.e = fmt.Errorf("failed to read RLE run header at pos %d. Uvarint result: (%d, %d)", d.pos, h, n)
		return
	}
	d.pos += n
	if h&1 == 1 {
		d.bpCount = uint32(h >> 1)
		d.bpRunPos = 0
	} else {
		d.rleCount = uint32(h >> 1)
		d.readRLERunValue()
	}
}

func (d *rleDecoder) nextInt32() int32 {
	var next int32
	if d.rleCount > 0 {
		next = d.rleValue
		d.rleCount--
	} else if d.bpCount > 0 || d.bpRunPos > 0 {
		if d.bpRunPos == 0 {
			d.readBitPackedRun()
			d.bpCount--
		}
		next = d.bpRun[d.bpRunPos]
		d.bpRunPos = (d.bpRunPos + 1) % 8
	} else {
		panic("should not happen")
	}
	if d.rleCount == 0 && d.bpCount == 0 && d.bpRunPos == 0 {
		d.readRunHeader()
	}
	return next
}

func (d *rleDecoder) hasNext() bool {
	return !d.eod && d.e == nil
}

func (d *rleDecoder) err() error {
	return d.e
}
