package rle

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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

// Taken from https://github.com/apache/parquet-cpp/blob/master/src/impala/rle-encoding.h:
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
//  https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/rle-encoding.h
type Decoder struct {
	r        *bufio.Reader
	bitWidth int
	e        error
	i        int
	eod      bool

	literalCount uint32
	repeatCount  uint32

	// rle
	rleCount uint32
	rleValue int32
	//header  uint32 // uint32 is enough because parquet-mr uses java int
}

var (
	ErrInvalidBitWidth = errors.New("bitWidth must be >=0 and <= 64")
)

func NewDecoder(r io.Reader, bitWidth int) (*Decoder, error) {

	if bitWidth < 0 || bitWidth > math.MaxInt64 {
		return nil, ErrInvalidBitWidth
	}

	d := &Decoder{r: bufio.NewReader(r), bitWidth: bitWidth}

	d.nextHeader()

	return d, nil
}

func (d *Decoder) nextValue() {
	byteWidth := (d.bitWidth + 7) / 8 // TODO: remember this in d

	buff, err := d.r.Peek(byteWidth)
	if err != nil {
		d.e = err
		return
	}

	switch byteWidth {
	case 1:
		d.rleValue = int32(buff[0])
	case 2:
		d.rleValue = int32(binary.LittleEndian.Uint16(buff))
	case 3:
		b3 := buff[0]
		b2 := buff[1]
		b1 := buff[2]
		d.rleValue = int32(b3) + int32(b2)<<8 + int32(b1)<<16
	case 4:
		d.rleValue = int32(binary.LittleEndian.Uint32(buff))
	default:
		panic("should not happen")
	}

	if discarded, err := d.r.Discard(byteWidth); err != nil {
		d.e = err
		return
	} else if discarded != byteWidth {
		panic("should discard the same amount that was read")
	}
}

func (d *Decoder) nextHeader() {
	indicatorValue, err := binary.ReadUvarint(d.r)

	if err != nil {
		d.e = err
		return
	}

	var isLiteral bool = (indicatorValue & 1) == 1

	if isLiteral {
		d.literalCount = uint32(indicatorValue>>1) * 8
	} else {
		d.repeatCount = uint32(indicatorValue >> 1)
	}

	d.nextValue()
}

func (d *Decoder) nextInt32() int32 {
	if d.repeatCount > 0 {
		d.repeatCount--
		if d.repeatCount == 0 {
			d.nextHeader()
		}
		return d.rleValue
	}
	panic("nyi")
}

func (d *Decoder) hasNext() bool {
	return !d.eod && d.e == nil
}

func (d *Decoder) err() error {
	return d.e
}
