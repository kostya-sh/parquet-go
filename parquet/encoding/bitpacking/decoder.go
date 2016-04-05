package bitpacking

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type decodef func([]byte, []int32) error

type Decoder struct {
	b      [32]byte
	p      []byte
	decode decodef
}

func NewDecoder(bitWidth uint) *Decoder {
	d := &Decoder{}
	d.p = d.b[:((bitWidth * 8) / 8)] // we need 8 values of bitWidth length
	if bitWidth == 0 || bitWidth > 32 {
		panic("invalid 0 > bitWidth <= 32")
	}

	switch bitWidth {

	case 1:
		d.decode = d.decode1RLE
	case 2:
		d.decode = d.decode2RLE
	case 3:
		d.decode = d.decode3RLE
	case 4:
		d.decode = d.decode4RLE
	case 5:
		d.decode = d.decode5RLE
	case 6:
		d.decode = d.decode6RLE
	case 7:
		d.decode = d.decode7RLE
	case 8:
		d.decode = d.decode8RLE
	case 9:
		d.decode = d.decode9RLE
	case 10:
		d.decode = d.decode10RLE
	case 11:
		d.decode = d.decode11RLE
	case 12:
		d.decode = d.decode12RLE
	case 13:
		d.decode = d.decode13RLE
	case 14:
		d.decode = d.decode14RLE
	case 15:
		d.decode = d.decode15RLE
	case 16:
		d.decode = d.decode16RLE
	case 17:
		d.decode = d.decode17RLE
	case 18:
		d.decode = d.decode18RLE
	case 19:
		d.decode = d.decode19RLE
	case 20:
		d.decode = d.decode20RLE
	case 21:
		d.decode = d.decode21RLE
	case 22:
		d.decode = d.decode22RLE
	case 23:
		d.decode = d.decode23RLE
	case 24:
		d.decode = d.decode24RLE
	case 25:
		d.decode = d.decode25RLE
	case 26:
		d.decode = d.decode26RLE
	case 27:
		d.decode = d.decode27RLE
	case 28:
		d.decode = d.decode28RLE
	case 29:
		d.decode = d.decode29RLE
	case 30:
		d.decode = d.decode30RLE
	case 31:
		d.decode = d.decode31RLE
	case 32:
		d.decode = d.decode32RLE

	default:
		panic("invalid bitWidth")
	}

	return d
}

func (d *Decoder) ReadLength(r io.Reader) (uint, error) {
	// run := <bit-packed-run> | <rle-run>
	header, err := binary.ReadUvarint(bufio.NewReader(r))

	if err == io.EOF {
		return 0, err
	} else if err != nil {
		return 0, err
	}

	if (header & 1) == 1 {
		// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
		// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
		// bit-pack-count := (number of values in this run) / 8
		literalCount := int32(header >> 1)
		return uint(literalCount), nil
	}

	return 0, fmt.Errorf("invalid header: rle header found, expected bitpacking header")
}

func (d *Decoder) Read(r io.Reader, out []int32) error {
	// this assumes len(out) has the exact right
	// amount of data to read
	buffer := make([]int32, 8)

	i := 0

	for i < len(out) {
		_, err := r.Read(d.p)
		if err != nil {
			return fmt.Errorf("decodeRLE:reader:%s", err)
		}

		if err := d.decode(d.p, buffer); err != nil {
			return fmt.Errorf("decodeRLE:decode:%s", err)
		}

		for j := 0; j < 8 && i < len(out); j, i = j+1, i+1 {
			out[i] = buffer[j]
		}

	}

	return nil
}

// type Decoder struct {
// 	r         *bufio.Reader
// 	bitWidth  uint // encoded bits
// 	byteWidth uint // min number of bytes required to encode one <bitWidth> encoded value
// 	value     int64
// 	buff      []byte
// 	bits      uint // number of bits read from the current byte
// 	err       error
// }

// func NewDecoder(r io.Reader, bitWidth uint) *Decoder {
// 	return &Decoder{
// 		r:         bufio.NewReader(r),
// 		bitWidth:  bitWidth,
// 		byteWidth: (bitWidth + uint(7)) / uint(8),
// 	}
// }

// func (d *Decoder) Scan() bool {
// 	var err error

// 	if d.err != nil {
// 		return false
// 	}

// 	if d.buff == nil {
// 		d.buff, err = d.r.Peek(int(d.byteWidth))
// 		if err != nil {
// 			d.setErr(err)
// 			return false
// 		}
// 	}

// 	switch d.byteWidth {
// 	case 1:
// 		// how many bits are left to consume in the current byte
// 		bitsLeftToConsume := 8 - d.bits
// 		missingBits := d.bitWidth - bitsLeftToConsume

// 		// create mask of bitWidth.
// 		// i.e for bitWidth = 3 would be: 0b00000111
// 		mask := byte(0xff >> (8 - d.bitWidth))

// 		value := byte(d.buff[0]>>d.bits) & mask

// 		d.bits += d.bitWidth
// 		d.value = int64(value)

// 		if d.bits >= 8 {
// 			_, err = d.r.Discard(1)

// 			if err != nil {
// 				d.setErr(err)
// 				return false
// 			}

// 			if missingBits > 0 {
// 				// we need to read more to complete the current value
// 				d.bits = missingBits

// 				// read next buffer
// 				d.buff, err = d.r.Peek(int(d.byteWidth))
// 				if err != nil {
// 					d.setErr(err)
// 					return false
// 				}

// 				// mask the relevant bits that we need
// 				// only the first missing bits will be selected
// 				mask = byte(0xff >> (8 - missingBits))

// 				// (d.buff[0] & mask) # select the first missing bits
// 				//  << (d.bitWidth - missingBits)  # shift left to make room
// 				//  | value  # set the previous carry over bits
// 				value = ((d.buff[0] & mask) << (d.bitWidth - missingBits)) | value
// 				d.value = int64(value)
// 			} else {
// 				d.bits = 0
// 				d.buff = nil
// 			}
// 		}
// 		return true
// 	case 2:
// 		d.value = int64(binary.LittleEndian.Uint16(d.buff))
// 	case 3:
// 		b3 := d.buff[0]
// 		b2 := d.buff[1]
// 		b1 := d.buff[2]
// 		d.value = int64(int32(b3) + int32(b2)<<8 + int32(b1)<<16)
// 	case 4:
// 		d.value = int64(binary.LittleEndian.Uint32(d.buff))
// 	default:
// 		panic("unsupported case")
// 	}

// 	return true
// }

// func (d *Decoder) setErr(err error) {
// 	if d.err == nil || d.err == io.EOF {
// 		d.err = err
// 	}
// }

// // Value returns the current value decoded
// func (d *Decoder) Value() int64 {
// 	return d.value
// }

// // Err returns the first non-EOF error that was encountered by the Decoder.
// func (d *Decoder) Err() error {
// 	if d.err == io.EOF {
// 		return nil
// 	}
// 	return d.err
// }
