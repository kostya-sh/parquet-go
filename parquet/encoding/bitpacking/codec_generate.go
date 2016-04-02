// Generated Code do not edit.
package bitpacking

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type format int

const (
	RLE format = iota
	BitPacked
)

type f func([8]int32) []byte

type Encoder struct {
	b                [32]byte
	encodeRLE        f
	encodeBitPacking f
	format           format
}

func NewEncoder(bitWidth uint, format format) *Encoder {

	if bitWidth == 0 || bitWidth > 32 {
		panic("invalid 0 > bitWidth <= 32")
	}

	e := &Encoder{format: format}
	switch bitWidth {

	case 1:
		e.encodeRLE = e.encode1RLE
	case 2:
		e.encodeRLE = e.encode2RLE
	case 3:
		e.encodeRLE = e.encode3RLE
	case 4:
		e.encodeRLE = e.encode4RLE
	case 5:
		e.encodeRLE = e.encode5RLE
	case 6:
		e.encodeRLE = e.encode6RLE
	case 7:
		e.encodeRLE = e.encode7RLE
	case 8:
		e.encodeRLE = e.encode8RLE
	case 9:
		e.encodeRLE = e.encode9RLE
	case 10:
		e.encodeRLE = e.encode10RLE
	case 11:
		e.encodeRLE = e.encode11RLE
	case 12:
		e.encodeRLE = e.encode12RLE
	case 13:
		e.encodeRLE = e.encode13RLE
	case 14:
		e.encodeRLE = e.encode14RLE
	case 15:
		e.encodeRLE = e.encode15RLE
	case 16:
		e.encodeRLE = e.encode16RLE
	case 17:
		e.encodeRLE = e.encode17RLE
	case 18:
		e.encodeRLE = e.encode18RLE
	case 19:
		e.encodeRLE = e.encode19RLE
	case 20:
		e.encodeRLE = e.encode20RLE
	case 21:
		e.encodeRLE = e.encode21RLE
	case 22:
		e.encodeRLE = e.encode22RLE
	case 23:
		e.encodeRLE = e.encode23RLE
	case 24:
		e.encodeRLE = e.encode24RLE
	case 25:
		e.encodeRLE = e.encode25RLE
	case 26:
		e.encodeRLE = e.encode26RLE
	case 27:
		e.encodeRLE = e.encode27RLE
	case 28:
		e.encodeRLE = e.encode28RLE
	case 29:
		e.encodeRLE = e.encode29RLE
	case 30:
		e.encodeRLE = e.encode30RLE
	case 31:
		e.encodeRLE = e.encode31RLE
	case 32:
		e.encodeRLE = e.encode32RLE

	default:
		panic("invalid bitWidth")
	}
	return e
}

// WriteHeader
func (e *Encoder) WriteHeader(w io.Writer, size uint) error {
	byteWidth := (size + 7) / 8
	return binary.Write(w, binary.LittleEndian, (byteWidth << 1))
}

// Write writes in io.Writer all the values
func (e *Encoder) Write(w io.Writer, values []int32) (int, error) {
	total := 0

	var buffer [8]int32
	chunks := (len(values) + 7) / 8

	if e.format == RLE {
		for i := 0; i < chunks; i++ {
			extra := 0
			if (i+1)*8 > len(values) {
				extra = ((i + 1) * 8) - len(values)
			}

			for j := 0; j < 8-extra; j++ {
				buffer[j] = values[(i*8)+j]
			}
			for j := extra; j > 0; j-- {
				buffer[j] = 0
			}

			n, err := w.Write(e.encodeRLE(buffer))
			total += n
			if err != nil {
				return total, err
			}
		}

		return total, nil
	}

	return -1, fmt.Errorf("Unsupported")
}

func (e *Encoder) encode1RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x1))
	b[0] |= byte((n[1] & 0x1) << 1)
	b[0] |= byte((n[2] & 0x1) << 2)
	b[0] |= byte((n[3] & 0x1) << 3)
	b[0] |= byte((n[4] & 0x1) << 4)
	b[0] |= byte((n[5] & 0x1) << 5)
	b[0] |= byte((n[6] & 0x1) << 6)
	b[0] |= byte((n[7] & 0x1) << 7)

	return b[:1]
}

func (e *Encoder) encode2RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x3))
	b[0] |= byte((n[1] & 0x3) << 2)
	b[0] |= byte((n[2] & 0x3) << 4)
	b[0] |= byte((n[3] & 0x3) << 6)
	b[1] = byte((n[4] & 0x3))
	b[1] |= byte((n[5] & 0x3) << 2)
	b[1] |= byte((n[6] & 0x3) << 4)
	b[1] |= byte((n[7] & 0x3) << 6)

	return b[:2]
}

func (e *Encoder) encode3RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x7))
	b[0] |= byte((n[1] & 0x7) << 3)
	b[0] |= byte((n[2] & 0x7) << 6)
	b[1] |= byte((n[2] & 0x7) >> 2)
	b[1] |= byte((n[3] & 0x7) << 1)
	b[1] |= byte((n[4] & 0x7) << 4)
	b[1] |= byte((n[5] & 0x7) << 7)
	b[2] |= byte((n[5] & 0x7) >> 1)
	b[2] |= byte((n[6] & 0x7) << 2)
	b[2] |= byte((n[7] & 0x7) << 5)

	return b[:3]
}

func (e *Encoder) encode4RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xF))
	b[0] |= byte((n[1] & 0xF) << 4)
	b[1] = byte((n[2] & 0xF))
	b[1] |= byte((n[3] & 0xF) << 4)
	b[2] = byte((n[4] & 0xF))
	b[2] |= byte((n[5] & 0xF) << 4)
	b[3] = byte((n[6] & 0xF))
	b[3] |= byte((n[7] & 0xF) << 4)

	return b[:4]
}

func (e *Encoder) encode5RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x1F))
	b[0] |= byte((n[1] & 0x1F) << 5)
	b[1] |= byte((n[1] & 0x1F) >> 3)
	b[1] |= byte((n[2] & 0x1F) << 2)
	b[1] |= byte((n[3] & 0x1F) << 7)
	b[2] |= byte((n[3] & 0x1F) >> 1)
	b[2] |= byte((n[4] & 0x1F) << 4)
	b[3] |= byte((n[4] & 0x1F) >> 4)
	b[3] |= byte((n[5] & 0x1F) << 1)
	b[3] |= byte((n[6] & 0x1F) << 6)
	b[4] |= byte((n[6] & 0x1F) >> 2)
	b[4] |= byte((n[7] & 0x1F) << 3)

	return b[:5]
}

func (e *Encoder) encode6RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x3F))
	b[0] |= byte((n[1] & 0x3F) << 6)
	b[1] |= byte((n[1] & 0x3F) >> 2)
	b[1] |= byte((n[2] & 0x3F) << 4)
	b[2] |= byte((n[2] & 0x3F) >> 4)
	b[2] |= byte((n[3] & 0x3F) << 2)
	b[3] = byte((n[4] & 0x3F))
	b[3] |= byte((n[5] & 0x3F) << 6)
	b[4] |= byte((n[5] & 0x3F) >> 2)
	b[4] |= byte((n[6] & 0x3F) << 4)
	b[5] |= byte((n[6] & 0x3F) >> 4)
	b[5] |= byte((n[7] & 0x3F) << 2)

	return b[:6]
}

func (e *Encoder) encode7RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0x7F))
	b[0] |= byte((n[1] & 0x7F) << 7)
	b[1] |= byte((n[1] & 0x7F) >> 1)
	b[1] |= byte((n[2] & 0x7F) << 6)
	b[2] |= byte((n[2] & 0x7F) >> 2)
	b[2] |= byte((n[3] & 0x7F) << 5)
	b[3] |= byte((n[3] & 0x7F) >> 3)
	b[3] |= byte((n[4] & 0x7F) << 4)
	b[4] |= byte((n[4] & 0x7F) >> 4)
	b[4] |= byte((n[5] & 0x7F) << 3)
	b[5] |= byte((n[5] & 0x7F) >> 5)
	b[5] |= byte((n[6] & 0x7F) << 2)
	b[6] |= byte((n[6] & 0x7F) >> 6)
	b[6] |= byte((n[7] & 0x7F) << 1)

	return b[:7]
}

func (e *Encoder) encode8RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte((n[1] & 0xFF))
	b[2] = byte((n[2] & 0xFF))
	b[3] = byte((n[3] & 0xFF))
	b[4] = byte((n[4] & 0xFF))
	b[5] = byte((n[5] & 0xFF))
	b[6] = byte((n[6] & 0xFF))
	b[7] = byte((n[7] & 0xFF))

	return b[:8]
}

func (e *Encoder) encode9RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 1)
	b[2] |= byte((n[1] & 0xFF) >> 7)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 1)
	b[2] |= byte((n[2] & 0xFF) << 2)
	b[3] |= byte((n[2] & 0xFF) >> 6)
	b[3] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[3] |= byte((n[3] & 0xFF) << 3)
	b[4] |= byte((n[3] & 0xFF) >> 5)
	b[4] |= byte(((n[3] >> 8) & 0xFF) << 3)
	b[4] |= byte((n[4] & 0xFF) << 4)
	b[5] |= byte((n[4] & 0xFF) >> 4)
	b[5] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[5] |= byte((n[5] & 0xFF) << 5)
	b[6] |= byte((n[5] & 0xFF) >> 3)
	b[6] |= byte(((n[5] >> 8) & 0xFF) << 5)
	b[6] |= byte((n[6] & 0xFF) << 6)
	b[7] |= byte((n[6] & 0xFF) >> 2)
	b[7] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[7] |= byte((n[7] & 0xFF) << 7)
	b[8] |= byte((n[7] & 0xFF) >> 1)
	b[8] |= byte(((n[7] >> 8) & 0xFF) << 7)

	return b[:9]
}

func (e *Encoder) encode10RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 2)
	b[2] |= byte((n[1] & 0xFF) >> 6)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 2)
	b[2] |= byte((n[2] & 0xFF) << 4)
	b[3] |= byte((n[2] & 0xFF) >> 4)
	b[3] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[3] |= byte((n[3] & 0xFF) << 6)
	b[4] |= byte((n[3] & 0xFF) >> 2)
	b[4] |= byte(((n[3] >> 8) & 0xFF) << 6)
	b[5] = byte((n[4] & 0xFF))
	b[6] = byte(((n[4] >> 8) & 0xFF))
	b[6] |= byte((n[5] & 0xFF) << 2)
	b[7] |= byte((n[5] & 0xFF) >> 6)
	b[7] |= byte(((n[5] >> 8) & 0xFF) << 2)
	b[7] |= byte((n[6] & 0xFF) << 4)
	b[8] |= byte((n[6] & 0xFF) >> 4)
	b[8] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[8] |= byte((n[7] & 0xFF) << 6)
	b[9] |= byte((n[7] & 0xFF) >> 2)
	b[9] |= byte(((n[7] >> 8) & 0xFF) << 6)

	return b[:10]
}

func (e *Encoder) encode11RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 3)
	b[2] |= byte((n[1] & 0xFF) >> 5)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 3)
	b[2] |= byte((n[2] & 0xFF) << 6)
	b[3] |= byte((n[2] & 0xFF) >> 2)
	b[3] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[4] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[4] |= byte((n[3] & 0xFF) << 1)
	b[5] |= byte((n[3] & 0xFF) >> 7)
	b[5] |= byte(((n[3] >> 8) & 0xFF) << 1)
	b[5] |= byte((n[4] & 0xFF) << 4)
	b[6] |= byte((n[4] & 0xFF) >> 4)
	b[6] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[6] |= byte((n[5] & 0xFF) << 7)
	b[7] |= byte((n[5] & 0xFF) >> 1)
	b[7] |= byte(((n[5] >> 8) & 0xFF) << 7)
	b[8] |= byte(((n[5] >> 8) & 0xFF) >> 1)
	b[8] |= byte((n[6] & 0xFF) << 2)
	b[9] |= byte((n[6] & 0xFF) >> 6)
	b[9] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[9] |= byte((n[7] & 0xFF) << 5)
	b[10] |= byte((n[7] & 0xFF) >> 3)
	b[10] |= byte(((n[7] >> 8) & 0xFF) << 5)

	return b[:11]
}

func (e *Encoder) encode12RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 4)
	b[2] |= byte((n[1] & 0xFF) >> 4)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 4)
	b[3] = byte((n[2] & 0xFF))
	b[4] = byte(((n[2] >> 8) & 0xFF))
	b[4] |= byte((n[3] & 0xFF) << 4)
	b[5] |= byte((n[3] & 0xFF) >> 4)
	b[5] |= byte(((n[3] >> 8) & 0xFF) << 4)
	b[6] = byte((n[4] & 0xFF))
	b[7] = byte(((n[4] >> 8) & 0xFF))
	b[7] |= byte((n[5] & 0xFF) << 4)
	b[8] |= byte((n[5] & 0xFF) >> 4)
	b[8] |= byte(((n[5] >> 8) & 0xFF) << 4)
	b[9] = byte((n[6] & 0xFF))
	b[10] = byte(((n[6] >> 8) & 0xFF))
	b[10] |= byte((n[7] & 0xFF) << 4)
	b[11] |= byte((n[7] & 0xFF) >> 4)
	b[11] |= byte(((n[7] >> 8) & 0xFF) << 4)

	return b[:12]
}

func (e *Encoder) encode13RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 5)
	b[2] |= byte((n[1] & 0xFF) >> 3)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 5)
	b[3] |= byte(((n[1] >> 8) & 0xFF) >> 3)
	b[3] |= byte((n[2] & 0xFF) << 2)
	b[4] |= byte((n[2] & 0xFF) >> 6)
	b[4] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[4] |= byte((n[3] & 0xFF) << 7)
	b[5] |= byte((n[3] & 0xFF) >> 1)
	b[5] |= byte(((n[3] >> 8) & 0xFF) << 7)
	b[6] |= byte(((n[3] >> 8) & 0xFF) >> 1)
	b[6] |= byte((n[4] & 0xFF) << 4)
	b[7] |= byte((n[4] & 0xFF) >> 4)
	b[7] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[8] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[8] |= byte((n[5] & 0xFF) << 1)
	b[9] |= byte((n[5] & 0xFF) >> 7)
	b[9] |= byte(((n[5] >> 8) & 0xFF) << 1)
	b[9] |= byte((n[6] & 0xFF) << 6)
	b[10] |= byte((n[6] & 0xFF) >> 2)
	b[10] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[11] |= byte(((n[6] >> 8) & 0xFF) >> 2)
	b[11] |= byte((n[7] & 0xFF) << 3)
	b[12] |= byte((n[7] & 0xFF) >> 5)
	b[12] |= byte(((n[7] >> 8) & 0xFF) << 3)

	return b[:13]
}

func (e *Encoder) encode14RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 6)
	b[2] |= byte((n[1] & 0xFF) >> 2)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 6)
	b[3] |= byte(((n[1] >> 8) & 0xFF) >> 2)
	b[3] |= byte((n[2] & 0xFF) << 4)
	b[4] |= byte((n[2] & 0xFF) >> 4)
	b[4] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[5] |= byte(((n[2] >> 8) & 0xFF) >> 4)
	b[5] |= byte((n[3] & 0xFF) << 2)
	b[6] |= byte((n[3] & 0xFF) >> 6)
	b[6] |= byte(((n[3] >> 8) & 0xFF) << 2)
	b[7] = byte((n[4] & 0xFF))
	b[8] = byte(((n[4] >> 8) & 0xFF))
	b[8] |= byte((n[5] & 0xFF) << 6)
	b[9] |= byte((n[5] & 0xFF) >> 2)
	b[9] |= byte(((n[5] >> 8) & 0xFF) << 6)
	b[10] |= byte(((n[5] >> 8) & 0xFF) >> 2)
	b[10] |= byte((n[6] & 0xFF) << 4)
	b[11] |= byte((n[6] & 0xFF) >> 4)
	b[11] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[12] |= byte(((n[6] >> 8) & 0xFF) >> 4)
	b[12] |= byte((n[7] & 0xFF) << 2)
	b[13] |= byte((n[7] & 0xFF) >> 6)
	b[13] |= byte(((n[7] >> 8) & 0xFF) << 2)

	return b[:14]
}

func (e *Encoder) encode15RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[1] |= byte((n[1] & 0xFF) << 7)
	b[2] |= byte((n[1] & 0xFF) >> 1)
	b[2] |= byte(((n[1] >> 8) & 0xFF) << 7)
	b[3] |= byte(((n[1] >> 8) & 0xFF) >> 1)
	b[3] |= byte((n[2] & 0xFF) << 6)
	b[4] |= byte((n[2] & 0xFF) >> 2)
	b[4] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[5] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[5] |= byte((n[3] & 0xFF) << 5)
	b[6] |= byte((n[3] & 0xFF) >> 3)
	b[6] |= byte(((n[3] >> 8) & 0xFF) << 5)
	b[7] |= byte(((n[3] >> 8) & 0xFF) >> 3)
	b[7] |= byte((n[4] & 0xFF) << 4)
	b[8] |= byte((n[4] & 0xFF) >> 4)
	b[8] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[9] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[9] |= byte((n[5] & 0xFF) << 3)
	b[10] |= byte((n[5] & 0xFF) >> 5)
	b[10] |= byte(((n[5] >> 8) & 0xFF) << 3)
	b[11] |= byte(((n[5] >> 8) & 0xFF) >> 5)
	b[11] |= byte((n[6] & 0xFF) << 2)
	b[12] |= byte((n[6] & 0xFF) >> 6)
	b[12] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[13] |= byte(((n[6] >> 8) & 0xFF) >> 6)
	b[13] |= byte((n[7] & 0xFF) << 1)
	b[14] |= byte((n[7] & 0xFF) >> 7)
	b[14] |= byte(((n[7] >> 8) & 0xFF) << 1)

	return b[:15]
}

func (e *Encoder) encode16RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte((n[1] & 0xFF))
	b[3] = byte(((n[1] >> 8) & 0xFF))
	b[4] = byte((n[2] & 0xFF))
	b[5] = byte(((n[2] >> 8) & 0xFF))
	b[6] = byte((n[3] & 0xFF))
	b[7] = byte(((n[3] >> 8) & 0xFF))
	b[8] = byte((n[4] & 0xFF))
	b[9] = byte(((n[4] >> 8) & 0xFF))
	b[10] = byte((n[5] & 0xFF))
	b[11] = byte(((n[5] >> 8) & 0xFF))
	b[12] = byte((n[6] & 0xFF))
	b[13] = byte(((n[6] >> 8) & 0xFF))
	b[14] = byte((n[7] & 0xFF))
	b[15] = byte(((n[7] >> 8) & 0xFF))

	return b[:16]
}

func (e *Encoder) encode17RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 1)
	b[3] |= byte((n[1] & 0xFF) >> 7)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 1)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 7)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 1)
	b[4] |= byte((n[2] & 0xFF) << 2)
	b[5] |= byte((n[2] & 0xFF) >> 6)
	b[5] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[6] |= byte(((n[2] >> 8) & 0xFF) >> 6)
	b[6] |= byte(((n[2] >> 16) & 0xFF) << 2)
	b[6] |= byte((n[3] & 0xFF) << 3)
	b[7] |= byte((n[3] & 0xFF) >> 5)
	b[7] |= byte(((n[3] >> 8) & 0xFF) << 3)
	b[8] |= byte(((n[3] >> 8) & 0xFF) >> 5)
	b[8] |= byte(((n[3] >> 16) & 0xFF) << 3)
	b[8] |= byte((n[4] & 0xFF) << 4)
	b[9] |= byte((n[4] & 0xFF) >> 4)
	b[9] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[10] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[10] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[10] |= byte((n[5] & 0xFF) << 5)
	b[11] |= byte((n[5] & 0xFF) >> 3)
	b[11] |= byte(((n[5] >> 8) & 0xFF) << 5)
	b[12] |= byte(((n[5] >> 8) & 0xFF) >> 3)
	b[12] |= byte(((n[5] >> 16) & 0xFF) << 5)
	b[12] |= byte((n[6] & 0xFF) << 6)
	b[13] |= byte((n[6] & 0xFF) >> 2)
	b[13] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[14] |= byte(((n[6] >> 8) & 0xFF) >> 2)
	b[14] |= byte(((n[6] >> 16) & 0xFF) << 6)
	b[14] |= byte((n[7] & 0xFF) << 7)
	b[15] |= byte((n[7] & 0xFF) >> 1)
	b[15] |= byte(((n[7] >> 8) & 0xFF) << 7)
	b[16] |= byte(((n[7] >> 8) & 0xFF) >> 1)
	b[16] |= byte(((n[7] >> 16) & 0xFF) << 7)

	return b[:17]
}

func (e *Encoder) encode18RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 2)
	b[3] |= byte((n[1] & 0xFF) >> 6)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 2)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 6)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 2)
	b[4] |= byte((n[2] & 0xFF) << 4)
	b[5] |= byte((n[2] & 0xFF) >> 4)
	b[5] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[6] |= byte(((n[2] >> 8) & 0xFF) >> 4)
	b[6] |= byte(((n[2] >> 16) & 0xFF) << 4)
	b[6] |= byte((n[3] & 0xFF) << 6)
	b[7] |= byte((n[3] & 0xFF) >> 2)
	b[7] |= byte(((n[3] >> 8) & 0xFF) << 6)
	b[8] |= byte(((n[3] >> 8) & 0xFF) >> 2)
	b[8] |= byte(((n[3] >> 16) & 0xFF) << 6)
	b[9] = byte((n[4] & 0xFF))
	b[10] = byte(((n[4] >> 8) & 0xFF))
	b[11] = byte(((n[4] >> 16) & 0xFF))
	b[11] |= byte((n[5] & 0xFF) << 2)
	b[12] |= byte((n[5] & 0xFF) >> 6)
	b[12] |= byte(((n[5] >> 8) & 0xFF) << 2)
	b[13] |= byte(((n[5] >> 8) & 0xFF) >> 6)
	b[13] |= byte(((n[5] >> 16) & 0xFF) << 2)
	b[13] |= byte((n[6] & 0xFF) << 4)
	b[14] |= byte((n[6] & 0xFF) >> 4)
	b[14] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[15] |= byte(((n[6] >> 8) & 0xFF) >> 4)
	b[15] |= byte(((n[6] >> 16) & 0xFF) << 4)
	b[15] |= byte((n[7] & 0xFF) << 6)
	b[16] |= byte((n[7] & 0xFF) >> 2)
	b[16] |= byte(((n[7] >> 8) & 0xFF) << 6)
	b[17] |= byte(((n[7] >> 8) & 0xFF) >> 2)
	b[17] |= byte(((n[7] >> 16) & 0xFF) << 6)

	return b[:18]
}

func (e *Encoder) encode19RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 3)
	b[3] |= byte((n[1] & 0xFF) >> 5)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 3)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 5)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 3)
	b[4] |= byte((n[2] & 0xFF) << 6)
	b[5] |= byte((n[2] & 0xFF) >> 2)
	b[5] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[6] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[6] |= byte(((n[2] >> 16) & 0xFF) << 6)
	b[7] |= byte(((n[2] >> 16) & 0xFF) >> 2)
	b[7] |= byte((n[3] & 0xFF) << 1)
	b[8] |= byte((n[3] & 0xFF) >> 7)
	b[8] |= byte(((n[3] >> 8) & 0xFF) << 1)
	b[9] |= byte(((n[3] >> 8) & 0xFF) >> 7)
	b[9] |= byte(((n[3] >> 16) & 0xFF) << 1)
	b[9] |= byte((n[4] & 0xFF) << 4)
	b[10] |= byte((n[4] & 0xFF) >> 4)
	b[10] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[11] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[11] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[11] |= byte((n[5] & 0xFF) << 7)
	b[12] |= byte((n[5] & 0xFF) >> 1)
	b[12] |= byte(((n[5] >> 8) & 0xFF) << 7)
	b[13] |= byte(((n[5] >> 8) & 0xFF) >> 1)
	b[13] |= byte(((n[5] >> 16) & 0xFF) << 7)
	b[14] |= byte(((n[5] >> 16) & 0xFF) >> 1)
	b[14] |= byte((n[6] & 0xFF) << 2)
	b[15] |= byte((n[6] & 0xFF) >> 6)
	b[15] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[16] |= byte(((n[6] >> 8) & 0xFF) >> 6)
	b[16] |= byte(((n[6] >> 16) & 0xFF) << 2)
	b[16] |= byte((n[7] & 0xFF) << 5)
	b[17] |= byte((n[7] & 0xFF) >> 3)
	b[17] |= byte(((n[7] >> 8) & 0xFF) << 5)
	b[18] |= byte(((n[7] >> 8) & 0xFF) >> 3)
	b[18] |= byte(((n[7] >> 16) & 0xFF) << 5)

	return b[:19]
}

func (e *Encoder) encode20RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 4)
	b[3] |= byte((n[1] & 0xFF) >> 4)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 4)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 4)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 4)
	b[5] = byte((n[2] & 0xFF))
	b[6] = byte(((n[2] >> 8) & 0xFF))
	b[7] = byte(((n[2] >> 16) & 0xFF))
	b[7] |= byte((n[3] & 0xFF) << 4)
	b[8] |= byte((n[3] & 0xFF) >> 4)
	b[8] |= byte(((n[3] >> 8) & 0xFF) << 4)
	b[9] |= byte(((n[3] >> 8) & 0xFF) >> 4)
	b[9] |= byte(((n[3] >> 16) & 0xFF) << 4)
	b[10] = byte((n[4] & 0xFF))
	b[11] = byte(((n[4] >> 8) & 0xFF))
	b[12] = byte(((n[4] >> 16) & 0xFF))
	b[12] |= byte((n[5] & 0xFF) << 4)
	b[13] |= byte((n[5] & 0xFF) >> 4)
	b[13] |= byte(((n[5] >> 8) & 0xFF) << 4)
	b[14] |= byte(((n[5] >> 8) & 0xFF) >> 4)
	b[14] |= byte(((n[5] >> 16) & 0xFF) << 4)
	b[15] = byte((n[6] & 0xFF))
	b[16] = byte(((n[6] >> 8) & 0xFF))
	b[17] = byte(((n[6] >> 16) & 0xFF))
	b[17] |= byte((n[7] & 0xFF) << 4)
	b[18] |= byte((n[7] & 0xFF) >> 4)
	b[18] |= byte(((n[7] >> 8) & 0xFF) << 4)
	b[19] |= byte(((n[7] >> 8) & 0xFF) >> 4)
	b[19] |= byte(((n[7] >> 16) & 0xFF) << 4)

	return b[:20]
}

func (e *Encoder) encode21RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 5)
	b[3] |= byte((n[1] & 0xFF) >> 3)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 5)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 3)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 5)
	b[5] |= byte(((n[1] >> 16) & 0xFF) >> 3)
	b[5] |= byte((n[2] & 0xFF) << 2)
	b[6] |= byte((n[2] & 0xFF) >> 6)
	b[6] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[7] |= byte(((n[2] >> 8) & 0xFF) >> 6)
	b[7] |= byte(((n[2] >> 16) & 0xFF) << 2)
	b[7] |= byte((n[3] & 0xFF) << 7)
	b[8] |= byte((n[3] & 0xFF) >> 1)
	b[8] |= byte(((n[3] >> 8) & 0xFF) << 7)
	b[9] |= byte(((n[3] >> 8) & 0xFF) >> 1)
	b[9] |= byte(((n[3] >> 16) & 0xFF) << 7)
	b[10] |= byte(((n[3] >> 16) & 0xFF) >> 1)
	b[10] |= byte((n[4] & 0xFF) << 4)
	b[11] |= byte((n[4] & 0xFF) >> 4)
	b[11] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[12] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[12] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[13] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[13] |= byte((n[5] & 0xFF) << 1)
	b[14] |= byte((n[5] & 0xFF) >> 7)
	b[14] |= byte(((n[5] >> 8) & 0xFF) << 1)
	b[15] |= byte(((n[5] >> 8) & 0xFF) >> 7)
	b[15] |= byte(((n[5] >> 16) & 0xFF) << 1)
	b[15] |= byte((n[6] & 0xFF) << 6)
	b[16] |= byte((n[6] & 0xFF) >> 2)
	b[16] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[17] |= byte(((n[6] >> 8) & 0xFF) >> 2)
	b[17] |= byte(((n[6] >> 16) & 0xFF) << 6)
	b[18] |= byte(((n[6] >> 16) & 0xFF) >> 2)
	b[18] |= byte((n[7] & 0xFF) << 3)
	b[19] |= byte((n[7] & 0xFF) >> 5)
	b[19] |= byte(((n[7] >> 8) & 0xFF) << 3)
	b[20] |= byte(((n[7] >> 8) & 0xFF) >> 5)
	b[20] |= byte(((n[7] >> 16) & 0xFF) << 3)

	return b[:21]
}

func (e *Encoder) encode22RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 6)
	b[3] |= byte((n[1] & 0xFF) >> 2)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 6)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 2)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 6)
	b[5] |= byte(((n[1] >> 16) & 0xFF) >> 2)
	b[5] |= byte((n[2] & 0xFF) << 4)
	b[6] |= byte((n[2] & 0xFF) >> 4)
	b[6] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[7] |= byte(((n[2] >> 8) & 0xFF) >> 4)
	b[7] |= byte(((n[2] >> 16) & 0xFF) << 4)
	b[8] |= byte(((n[2] >> 16) & 0xFF) >> 4)
	b[8] |= byte((n[3] & 0xFF) << 2)
	b[9] |= byte((n[3] & 0xFF) >> 6)
	b[9] |= byte(((n[3] >> 8) & 0xFF) << 2)
	b[10] |= byte(((n[3] >> 8) & 0xFF) >> 6)
	b[10] |= byte(((n[3] >> 16) & 0xFF) << 2)
	b[11] = byte((n[4] & 0xFF))
	b[12] = byte(((n[4] >> 8) & 0xFF))
	b[13] = byte(((n[4] >> 16) & 0xFF))
	b[13] |= byte((n[5] & 0xFF) << 6)
	b[14] |= byte((n[5] & 0xFF) >> 2)
	b[14] |= byte(((n[5] >> 8) & 0xFF) << 6)
	b[15] |= byte(((n[5] >> 8) & 0xFF) >> 2)
	b[15] |= byte(((n[5] >> 16) & 0xFF) << 6)
	b[16] |= byte(((n[5] >> 16) & 0xFF) >> 2)
	b[16] |= byte((n[6] & 0xFF) << 4)
	b[17] |= byte((n[6] & 0xFF) >> 4)
	b[17] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[18] |= byte(((n[6] >> 8) & 0xFF) >> 4)
	b[18] |= byte(((n[6] >> 16) & 0xFF) << 4)
	b[19] |= byte(((n[6] >> 16) & 0xFF) >> 4)
	b[19] |= byte((n[7] & 0xFF) << 2)
	b[20] |= byte((n[7] & 0xFF) >> 6)
	b[20] |= byte(((n[7] >> 8) & 0xFF) << 2)
	b[21] |= byte(((n[7] >> 8) & 0xFF) >> 6)
	b[21] |= byte(((n[7] >> 16) & 0xFF) << 2)

	return b[:22]
}

func (e *Encoder) encode23RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[2] |= byte((n[1] & 0xFF) << 7)
	b[3] |= byte((n[1] & 0xFF) >> 1)
	b[3] |= byte(((n[1] >> 8) & 0xFF) << 7)
	b[4] |= byte(((n[1] >> 8) & 0xFF) >> 1)
	b[4] |= byte(((n[1] >> 16) & 0xFF) << 7)
	b[5] |= byte(((n[1] >> 16) & 0xFF) >> 1)
	b[5] |= byte((n[2] & 0xFF) << 6)
	b[6] |= byte((n[2] & 0xFF) >> 2)
	b[6] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[7] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[7] |= byte(((n[2] >> 16) & 0xFF) << 6)
	b[8] |= byte(((n[2] >> 16) & 0xFF) >> 2)
	b[8] |= byte((n[3] & 0xFF) << 5)
	b[9] |= byte((n[3] & 0xFF) >> 3)
	b[9] |= byte(((n[3] >> 8) & 0xFF) << 5)
	b[10] |= byte(((n[3] >> 8) & 0xFF) >> 3)
	b[10] |= byte(((n[3] >> 16) & 0xFF) << 5)
	b[11] |= byte(((n[3] >> 16) & 0xFF) >> 3)
	b[11] |= byte((n[4] & 0xFF) << 4)
	b[12] |= byte((n[4] & 0xFF) >> 4)
	b[12] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[13] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[13] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[14] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[14] |= byte((n[5] & 0xFF) << 3)
	b[15] |= byte((n[5] & 0xFF) >> 5)
	b[15] |= byte(((n[5] >> 8) & 0xFF) << 3)
	b[16] |= byte(((n[5] >> 8) & 0xFF) >> 5)
	b[16] |= byte(((n[5] >> 16) & 0xFF) << 3)
	b[17] |= byte(((n[5] >> 16) & 0xFF) >> 5)
	b[17] |= byte((n[6] & 0xFF) << 2)
	b[18] |= byte((n[6] & 0xFF) >> 6)
	b[18] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[19] |= byte(((n[6] >> 8) & 0xFF) >> 6)
	b[19] |= byte(((n[6] >> 16) & 0xFF) << 2)
	b[20] |= byte(((n[6] >> 16) & 0xFF) >> 6)
	b[20] |= byte((n[7] & 0xFF) << 1)
	b[21] |= byte((n[7] & 0xFF) >> 7)
	b[21] |= byte(((n[7] >> 8) & 0xFF) << 1)
	b[22] |= byte(((n[7] >> 8) & 0xFF) >> 7)
	b[22] |= byte(((n[7] >> 16) & 0xFF) << 1)

	return b[:23]
}

func (e *Encoder) encode24RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte((n[1] & 0xFF))
	b[4] = byte(((n[1] >> 8) & 0xFF))
	b[5] = byte(((n[1] >> 16) & 0xFF))
	b[6] = byte((n[2] & 0xFF))
	b[7] = byte(((n[2] >> 8) & 0xFF))
	b[8] = byte(((n[2] >> 16) & 0xFF))
	b[9] = byte((n[3] & 0xFF))
	b[10] = byte(((n[3] >> 8) & 0xFF))
	b[11] = byte(((n[3] >> 16) & 0xFF))
	b[12] = byte((n[4] & 0xFF))
	b[13] = byte(((n[4] >> 8) & 0xFF))
	b[14] = byte(((n[4] >> 16) & 0xFF))
	b[15] = byte((n[5] & 0xFF))
	b[16] = byte(((n[5] >> 8) & 0xFF))
	b[17] = byte(((n[5] >> 16) & 0xFF))
	b[18] = byte((n[6] & 0xFF))
	b[19] = byte(((n[6] >> 8) & 0xFF))
	b[20] = byte(((n[6] >> 16) & 0xFF))
	b[21] = byte((n[7] & 0xFF))
	b[22] = byte(((n[7] >> 8) & 0xFF))
	b[23] = byte(((n[7] >> 16) & 0xFF))

	return b[:24]
}

func (e *Encoder) encode25RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 1)
	b[4] |= byte((n[1] & 0xFF) >> 7)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 1)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 7)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 1)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 7)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 1)
	b[6] |= byte((n[2] & 0xFF) << 2)
	b[7] |= byte((n[2] & 0xFF) >> 6)
	b[7] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[8] |= byte(((n[2] >> 8) & 0xFF) >> 6)
	b[8] |= byte(((n[2] >> 16) & 0xFF) << 2)
	b[9] |= byte(((n[2] >> 16) & 0xFF) >> 6)
	b[9] |= byte(((n[2] >> 24) & 0xFF) << 2)
	b[9] |= byte((n[3] & 0xFF) << 3)
	b[10] |= byte((n[3] & 0xFF) >> 5)
	b[10] |= byte(((n[3] >> 8) & 0xFF) << 3)
	b[11] |= byte(((n[3] >> 8) & 0xFF) >> 5)
	b[11] |= byte(((n[3] >> 16) & 0xFF) << 3)
	b[12] |= byte(((n[3] >> 16) & 0xFF) >> 5)
	b[12] |= byte(((n[3] >> 24) & 0xFF) << 3)
	b[12] |= byte((n[4] & 0xFF) << 4)
	b[13] |= byte((n[4] & 0xFF) >> 4)
	b[13] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[14] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[14] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[15] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[15] |= byte(((n[4] >> 24) & 0xFF) << 4)
	b[15] |= byte((n[5] & 0xFF) << 5)
	b[16] |= byte((n[5] & 0xFF) >> 3)
	b[16] |= byte(((n[5] >> 8) & 0xFF) << 5)
	b[17] |= byte(((n[5] >> 8) & 0xFF) >> 3)
	b[17] |= byte(((n[5] >> 16) & 0xFF) << 5)
	b[18] |= byte(((n[5] >> 16) & 0xFF) >> 3)
	b[18] |= byte(((n[5] >> 24) & 0xFF) << 5)
	b[18] |= byte((n[6] & 0xFF) << 6)
	b[19] |= byte((n[6] & 0xFF) >> 2)
	b[19] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[20] |= byte(((n[6] >> 8) & 0xFF) >> 2)
	b[20] |= byte(((n[6] >> 16) & 0xFF) << 6)
	b[21] |= byte(((n[6] >> 16) & 0xFF) >> 2)
	b[21] |= byte(((n[6] >> 24) & 0xFF) << 6)
	b[21] |= byte((n[7] & 0xFF) << 7)
	b[22] |= byte((n[7] & 0xFF) >> 1)
	b[22] |= byte(((n[7] >> 8) & 0xFF) << 7)
	b[23] |= byte(((n[7] >> 8) & 0xFF) >> 1)
	b[23] |= byte(((n[7] >> 16) & 0xFF) << 7)
	b[24] |= byte(((n[7] >> 16) & 0xFF) >> 1)
	b[24] |= byte(((n[7] >> 24) & 0xFF) << 7)

	return b[:25]
}

func (e *Encoder) encode26RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 2)
	b[4] |= byte((n[1] & 0xFF) >> 6)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 2)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 6)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 2)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 6)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 2)
	b[6] |= byte((n[2] & 0xFF) << 4)
	b[7] |= byte((n[2] & 0xFF) >> 4)
	b[7] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[8] |= byte(((n[2] >> 8) & 0xFF) >> 4)
	b[8] |= byte(((n[2] >> 16) & 0xFF) << 4)
	b[9] |= byte(((n[2] >> 16) & 0xFF) >> 4)
	b[9] |= byte(((n[2] >> 24) & 0xFF) << 4)
	b[9] |= byte((n[3] & 0xFF) << 6)
	b[10] |= byte((n[3] & 0xFF) >> 2)
	b[10] |= byte(((n[3] >> 8) & 0xFF) << 6)
	b[11] |= byte(((n[3] >> 8) & 0xFF) >> 2)
	b[11] |= byte(((n[3] >> 16) & 0xFF) << 6)
	b[12] |= byte(((n[3] >> 16) & 0xFF) >> 2)
	b[12] |= byte(((n[3] >> 24) & 0xFF) << 6)
	b[13] = byte((n[4] & 0xFF))
	b[14] = byte(((n[4] >> 8) & 0xFF))
	b[15] = byte(((n[4] >> 16) & 0xFF))
	b[16] = byte(((n[4] >> 24) & 0xFF))
	b[16] |= byte((n[5] & 0xFF) << 2)
	b[17] |= byte((n[5] & 0xFF) >> 6)
	b[17] |= byte(((n[5] >> 8) & 0xFF) << 2)
	b[18] |= byte(((n[5] >> 8) & 0xFF) >> 6)
	b[18] |= byte(((n[5] >> 16) & 0xFF) << 2)
	b[19] |= byte(((n[5] >> 16) & 0xFF) >> 6)
	b[19] |= byte(((n[5] >> 24) & 0xFF) << 2)
	b[19] |= byte((n[6] & 0xFF) << 4)
	b[20] |= byte((n[6] & 0xFF) >> 4)
	b[20] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[21] |= byte(((n[6] >> 8) & 0xFF) >> 4)
	b[21] |= byte(((n[6] >> 16) & 0xFF) << 4)
	b[22] |= byte(((n[6] >> 16) & 0xFF) >> 4)
	b[22] |= byte(((n[6] >> 24) & 0xFF) << 4)
	b[22] |= byte((n[7] & 0xFF) << 6)
	b[23] |= byte((n[7] & 0xFF) >> 2)
	b[23] |= byte(((n[7] >> 8) & 0xFF) << 6)
	b[24] |= byte(((n[7] >> 8) & 0xFF) >> 2)
	b[24] |= byte(((n[7] >> 16) & 0xFF) << 6)
	b[25] |= byte(((n[7] >> 16) & 0xFF) >> 2)
	b[25] |= byte(((n[7] >> 24) & 0xFF) << 6)

	return b[:26]
}

func (e *Encoder) encode27RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 3)
	b[4] |= byte((n[1] & 0xFF) >> 5)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 3)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 5)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 3)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 5)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 3)
	b[6] |= byte((n[2] & 0xFF) << 6)
	b[7] |= byte((n[2] & 0xFF) >> 2)
	b[7] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[8] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[8] |= byte(((n[2] >> 16) & 0xFF) << 6)
	b[9] |= byte(((n[2] >> 16) & 0xFF) >> 2)
	b[9] |= byte(((n[2] >> 24) & 0xFF) << 6)
	b[10] |= byte(((n[2] >> 24) & 0xFF) >> 2)
	b[10] |= byte((n[3] & 0xFF) << 1)
	b[11] |= byte((n[3] & 0xFF) >> 7)
	b[11] |= byte(((n[3] >> 8) & 0xFF) << 1)
	b[12] |= byte(((n[3] >> 8) & 0xFF) >> 7)
	b[12] |= byte(((n[3] >> 16) & 0xFF) << 1)
	b[13] |= byte(((n[3] >> 16) & 0xFF) >> 7)
	b[13] |= byte(((n[3] >> 24) & 0xFF) << 1)
	b[13] |= byte((n[4] & 0xFF) << 4)
	b[14] |= byte((n[4] & 0xFF) >> 4)
	b[14] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[15] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[15] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[16] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[16] |= byte(((n[4] >> 24) & 0xFF) << 4)
	b[16] |= byte((n[5] & 0xFF) << 7)
	b[17] |= byte((n[5] & 0xFF) >> 1)
	b[17] |= byte(((n[5] >> 8) & 0xFF) << 7)
	b[18] |= byte(((n[5] >> 8) & 0xFF) >> 1)
	b[18] |= byte(((n[5] >> 16) & 0xFF) << 7)
	b[19] |= byte(((n[5] >> 16) & 0xFF) >> 1)
	b[19] |= byte(((n[5] >> 24) & 0xFF) << 7)
	b[20] |= byte(((n[5] >> 24) & 0xFF) >> 1)
	b[20] |= byte((n[6] & 0xFF) << 2)
	b[21] |= byte((n[6] & 0xFF) >> 6)
	b[21] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[22] |= byte(((n[6] >> 8) & 0xFF) >> 6)
	b[22] |= byte(((n[6] >> 16) & 0xFF) << 2)
	b[23] |= byte(((n[6] >> 16) & 0xFF) >> 6)
	b[23] |= byte(((n[6] >> 24) & 0xFF) << 2)
	b[23] |= byte((n[7] & 0xFF) << 5)
	b[24] |= byte((n[7] & 0xFF) >> 3)
	b[24] |= byte(((n[7] >> 8) & 0xFF) << 5)
	b[25] |= byte(((n[7] >> 8) & 0xFF) >> 3)
	b[25] |= byte(((n[7] >> 16) & 0xFF) << 5)
	b[26] |= byte(((n[7] >> 16) & 0xFF) >> 3)
	b[26] |= byte(((n[7] >> 24) & 0xFF) << 5)

	return b[:27]
}

func (e *Encoder) encode28RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 4)
	b[4] |= byte((n[1] & 0xFF) >> 4)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 4)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 4)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 4)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 4)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 4)
	b[7] = byte((n[2] & 0xFF))
	b[8] = byte(((n[2] >> 8) & 0xFF))
	b[9] = byte(((n[2] >> 16) & 0xFF))
	b[10] = byte(((n[2] >> 24) & 0xFF))
	b[10] |= byte((n[3] & 0xFF) << 4)
	b[11] |= byte((n[3] & 0xFF) >> 4)
	b[11] |= byte(((n[3] >> 8) & 0xFF) << 4)
	b[12] |= byte(((n[3] >> 8) & 0xFF) >> 4)
	b[12] |= byte(((n[3] >> 16) & 0xFF) << 4)
	b[13] |= byte(((n[3] >> 16) & 0xFF) >> 4)
	b[13] |= byte(((n[3] >> 24) & 0xFF) << 4)
	b[14] = byte((n[4] & 0xFF))
	b[15] = byte(((n[4] >> 8) & 0xFF))
	b[16] = byte(((n[4] >> 16) & 0xFF))
	b[17] = byte(((n[4] >> 24) & 0xFF))
	b[17] |= byte((n[5] & 0xFF) << 4)
	b[18] |= byte((n[5] & 0xFF) >> 4)
	b[18] |= byte(((n[5] >> 8) & 0xFF) << 4)
	b[19] |= byte(((n[5] >> 8) & 0xFF) >> 4)
	b[19] |= byte(((n[5] >> 16) & 0xFF) << 4)
	b[20] |= byte(((n[5] >> 16) & 0xFF) >> 4)
	b[20] |= byte(((n[5] >> 24) & 0xFF) << 4)
	b[21] = byte((n[6] & 0xFF))
	b[22] = byte(((n[6] >> 8) & 0xFF))
	b[23] = byte(((n[6] >> 16) & 0xFF))
	b[24] = byte(((n[6] >> 24) & 0xFF))
	b[24] |= byte((n[7] & 0xFF) << 4)
	b[25] |= byte((n[7] & 0xFF) >> 4)
	b[25] |= byte(((n[7] >> 8) & 0xFF) << 4)
	b[26] |= byte(((n[7] >> 8) & 0xFF) >> 4)
	b[26] |= byte(((n[7] >> 16) & 0xFF) << 4)
	b[27] |= byte(((n[7] >> 16) & 0xFF) >> 4)
	b[27] |= byte(((n[7] >> 24) & 0xFF) << 4)

	return b[:28]
}

func (e *Encoder) encode29RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 5)
	b[4] |= byte((n[1] & 0xFF) >> 3)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 5)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 3)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 5)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 3)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 5)
	b[7] |= byte(((n[1] >> 24) & 0xFF) >> 3)
	b[7] |= byte((n[2] & 0xFF) << 2)
	b[8] |= byte((n[2] & 0xFF) >> 6)
	b[8] |= byte(((n[2] >> 8) & 0xFF) << 2)
	b[9] |= byte(((n[2] >> 8) & 0xFF) >> 6)
	b[9] |= byte(((n[2] >> 16) & 0xFF) << 2)
	b[10] |= byte(((n[2] >> 16) & 0xFF) >> 6)
	b[10] |= byte(((n[2] >> 24) & 0xFF) << 2)
	b[10] |= byte((n[3] & 0xFF) << 7)
	b[11] |= byte((n[3] & 0xFF) >> 1)
	b[11] |= byte(((n[3] >> 8) & 0xFF) << 7)
	b[12] |= byte(((n[3] >> 8) & 0xFF) >> 1)
	b[12] |= byte(((n[3] >> 16) & 0xFF) << 7)
	b[13] |= byte(((n[3] >> 16) & 0xFF) >> 1)
	b[13] |= byte(((n[3] >> 24) & 0xFF) << 7)
	b[14] |= byte(((n[3] >> 24) & 0xFF) >> 1)
	b[14] |= byte((n[4] & 0xFF) << 4)
	b[15] |= byte((n[4] & 0xFF) >> 4)
	b[15] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[16] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[16] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[17] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[17] |= byte(((n[4] >> 24) & 0xFF) << 4)
	b[18] |= byte(((n[4] >> 24) & 0xFF) >> 4)
	b[18] |= byte((n[5] & 0xFF) << 1)
	b[19] |= byte((n[5] & 0xFF) >> 7)
	b[19] |= byte(((n[5] >> 8) & 0xFF) << 1)
	b[20] |= byte(((n[5] >> 8) & 0xFF) >> 7)
	b[20] |= byte(((n[5] >> 16) & 0xFF) << 1)
	b[21] |= byte(((n[5] >> 16) & 0xFF) >> 7)
	b[21] |= byte(((n[5] >> 24) & 0xFF) << 1)
	b[21] |= byte((n[6] & 0xFF) << 6)
	b[22] |= byte((n[6] & 0xFF) >> 2)
	b[22] |= byte(((n[6] >> 8) & 0xFF) << 6)
	b[23] |= byte(((n[6] >> 8) & 0xFF) >> 2)
	b[23] |= byte(((n[6] >> 16) & 0xFF) << 6)
	b[24] |= byte(((n[6] >> 16) & 0xFF) >> 2)
	b[24] |= byte(((n[6] >> 24) & 0xFF) << 6)
	b[25] |= byte(((n[6] >> 24) & 0xFF) >> 2)
	b[25] |= byte((n[7] & 0xFF) << 3)
	b[26] |= byte((n[7] & 0xFF) >> 5)
	b[26] |= byte(((n[7] >> 8) & 0xFF) << 3)
	b[27] |= byte(((n[7] >> 8) & 0xFF) >> 5)
	b[27] |= byte(((n[7] >> 16) & 0xFF) << 3)
	b[28] |= byte(((n[7] >> 16) & 0xFF) >> 5)
	b[28] |= byte(((n[7] >> 24) & 0xFF) << 3)

	return b[:29]
}

func (e *Encoder) encode30RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 6)
	b[4] |= byte((n[1] & 0xFF) >> 2)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 6)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 2)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 6)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 2)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 6)
	b[7] |= byte(((n[1] >> 24) & 0xFF) >> 2)
	b[7] |= byte((n[2] & 0xFF) << 4)
	b[8] |= byte((n[2] & 0xFF) >> 4)
	b[8] |= byte(((n[2] >> 8) & 0xFF) << 4)
	b[9] |= byte(((n[2] >> 8) & 0xFF) >> 4)
	b[9] |= byte(((n[2] >> 16) & 0xFF) << 4)
	b[10] |= byte(((n[2] >> 16) & 0xFF) >> 4)
	b[10] |= byte(((n[2] >> 24) & 0xFF) << 4)
	b[11] |= byte(((n[2] >> 24) & 0xFF) >> 4)
	b[11] |= byte((n[3] & 0xFF) << 2)
	b[12] |= byte((n[3] & 0xFF) >> 6)
	b[12] |= byte(((n[3] >> 8) & 0xFF) << 2)
	b[13] |= byte(((n[3] >> 8) & 0xFF) >> 6)
	b[13] |= byte(((n[3] >> 16) & 0xFF) << 2)
	b[14] |= byte(((n[3] >> 16) & 0xFF) >> 6)
	b[14] |= byte(((n[3] >> 24) & 0xFF) << 2)
	b[15] = byte((n[4] & 0xFF))
	b[16] = byte(((n[4] >> 8) & 0xFF))
	b[17] = byte(((n[4] >> 16) & 0xFF))
	b[18] = byte(((n[4] >> 24) & 0xFF))
	b[18] |= byte((n[5] & 0xFF) << 6)
	b[19] |= byte((n[5] & 0xFF) >> 2)
	b[19] |= byte(((n[5] >> 8) & 0xFF) << 6)
	b[20] |= byte(((n[5] >> 8) & 0xFF) >> 2)
	b[20] |= byte(((n[5] >> 16) & 0xFF) << 6)
	b[21] |= byte(((n[5] >> 16) & 0xFF) >> 2)
	b[21] |= byte(((n[5] >> 24) & 0xFF) << 6)
	b[22] |= byte(((n[5] >> 24) & 0xFF) >> 2)
	b[22] |= byte((n[6] & 0xFF) << 4)
	b[23] |= byte((n[6] & 0xFF) >> 4)
	b[23] |= byte(((n[6] >> 8) & 0xFF) << 4)
	b[24] |= byte(((n[6] >> 8) & 0xFF) >> 4)
	b[24] |= byte(((n[6] >> 16) & 0xFF) << 4)
	b[25] |= byte(((n[6] >> 16) & 0xFF) >> 4)
	b[25] |= byte(((n[6] >> 24) & 0xFF) << 4)
	b[26] |= byte(((n[6] >> 24) & 0xFF) >> 4)
	b[26] |= byte((n[7] & 0xFF) << 2)
	b[27] |= byte((n[7] & 0xFF) >> 6)
	b[27] |= byte(((n[7] >> 8) & 0xFF) << 2)
	b[28] |= byte(((n[7] >> 8) & 0xFF) >> 6)
	b[28] |= byte(((n[7] >> 16) & 0xFF) << 2)
	b[29] |= byte(((n[7] >> 16) & 0xFF) >> 6)
	b[29] |= byte(((n[7] >> 24) & 0xFF) << 2)

	return b[:30]
}

func (e *Encoder) encode31RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[3] |= byte((n[1] & 0xFF) << 7)
	b[4] |= byte((n[1] & 0xFF) >> 1)
	b[4] |= byte(((n[1] >> 8) & 0xFF) << 7)
	b[5] |= byte(((n[1] >> 8) & 0xFF) >> 1)
	b[5] |= byte(((n[1] >> 16) & 0xFF) << 7)
	b[6] |= byte(((n[1] >> 16) & 0xFF) >> 1)
	b[6] |= byte(((n[1] >> 24) & 0xFF) << 7)
	b[7] |= byte(((n[1] >> 24) & 0xFF) >> 1)
	b[7] |= byte((n[2] & 0xFF) << 6)
	b[8] |= byte((n[2] & 0xFF) >> 2)
	b[8] |= byte(((n[2] >> 8) & 0xFF) << 6)
	b[9] |= byte(((n[2] >> 8) & 0xFF) >> 2)
	b[9] |= byte(((n[2] >> 16) & 0xFF) << 6)
	b[10] |= byte(((n[2] >> 16) & 0xFF) >> 2)
	b[10] |= byte(((n[2] >> 24) & 0xFF) << 6)
	b[11] |= byte(((n[2] >> 24) & 0xFF) >> 2)
	b[11] |= byte((n[3] & 0xFF) << 5)
	b[12] |= byte((n[3] & 0xFF) >> 3)
	b[12] |= byte(((n[3] >> 8) & 0xFF) << 5)
	b[13] |= byte(((n[3] >> 8) & 0xFF) >> 3)
	b[13] |= byte(((n[3] >> 16) & 0xFF) << 5)
	b[14] |= byte(((n[3] >> 16) & 0xFF) >> 3)
	b[14] |= byte(((n[3] >> 24) & 0xFF) << 5)
	b[15] |= byte(((n[3] >> 24) & 0xFF) >> 3)
	b[15] |= byte((n[4] & 0xFF) << 4)
	b[16] |= byte((n[4] & 0xFF) >> 4)
	b[16] |= byte(((n[4] >> 8) & 0xFF) << 4)
	b[17] |= byte(((n[4] >> 8) & 0xFF) >> 4)
	b[17] |= byte(((n[4] >> 16) & 0xFF) << 4)
	b[18] |= byte(((n[4] >> 16) & 0xFF) >> 4)
	b[18] |= byte(((n[4] >> 24) & 0xFF) << 4)
	b[19] |= byte(((n[4] >> 24) & 0xFF) >> 4)
	b[19] |= byte((n[5] & 0xFF) << 3)
	b[20] |= byte((n[5] & 0xFF) >> 5)
	b[20] |= byte(((n[5] >> 8) & 0xFF) << 3)
	b[21] |= byte(((n[5] >> 8) & 0xFF) >> 5)
	b[21] |= byte(((n[5] >> 16) & 0xFF) << 3)
	b[22] |= byte(((n[5] >> 16) & 0xFF) >> 5)
	b[22] |= byte(((n[5] >> 24) & 0xFF) << 3)
	b[23] |= byte(((n[5] >> 24) & 0xFF) >> 5)
	b[23] |= byte((n[6] & 0xFF) << 2)
	b[24] |= byte((n[6] & 0xFF) >> 6)
	b[24] |= byte(((n[6] >> 8) & 0xFF) << 2)
	b[25] |= byte(((n[6] >> 8) & 0xFF) >> 6)
	b[25] |= byte(((n[6] >> 16) & 0xFF) << 2)
	b[26] |= byte(((n[6] >> 16) & 0xFF) >> 6)
	b[26] |= byte(((n[6] >> 24) & 0xFF) << 2)
	b[27] |= byte(((n[6] >> 24) & 0xFF) >> 6)
	b[27] |= byte((n[7] & 0xFF) << 1)
	b[28] |= byte((n[7] & 0xFF) >> 7)
	b[28] |= byte(((n[7] >> 8) & 0xFF) << 1)
	b[29] |= byte(((n[7] >> 8) & 0xFF) >> 7)
	b[29] |= byte(((n[7] >> 16) & 0xFF) << 1)
	b[30] |= byte(((n[7] >> 16) & 0xFF) >> 7)
	b[30] |= byte(((n[7] >> 24) & 0xFF) << 1)

	return b[:31]
}

func (e *Encoder) encode32RLE(n [8]int32) []byte {

	b := e.b

	b[0] = byte((n[0] & 0xFF))
	b[1] = byte(((n[0] >> 8) & 0xFF))
	b[2] = byte(((n[0] >> 16) & 0xFF))
	b[3] = byte(((n[0] >> 24) & 0xFF))
	b[4] = byte((n[1] & 0xFF))
	b[5] = byte(((n[1] >> 8) & 0xFF))
	b[6] = byte(((n[1] >> 16) & 0xFF))
	b[7] = byte(((n[1] >> 24) & 0xFF))
	b[8] = byte((n[2] & 0xFF))
	b[9] = byte(((n[2] >> 8) & 0xFF))
	b[10] = byte(((n[2] >> 16) & 0xFF))
	b[11] = byte(((n[2] >> 24) & 0xFF))
	b[12] = byte((n[3] & 0xFF))
	b[13] = byte(((n[3] >> 8) & 0xFF))
	b[14] = byte(((n[3] >> 16) & 0xFF))
	b[15] = byte(((n[3] >> 24) & 0xFF))
	b[16] = byte((n[4] & 0xFF))
	b[17] = byte(((n[4] >> 8) & 0xFF))
	b[18] = byte(((n[4] >> 16) & 0xFF))
	b[19] = byte(((n[4] >> 24) & 0xFF))
	b[20] = byte((n[5] & 0xFF))
	b[21] = byte(((n[5] >> 8) & 0xFF))
	b[22] = byte(((n[5] >> 16) & 0xFF))
	b[23] = byte(((n[5] >> 24) & 0xFF))
	b[24] = byte((n[6] & 0xFF))
	b[25] = byte(((n[6] >> 8) & 0xFF))
	b[26] = byte(((n[6] >> 16) & 0xFF))
	b[27] = byte(((n[6] >> 24) & 0xFF))
	b[28] = byte((n[7] & 0xFF))
	b[29] = byte(((n[7] >> 8) & 0xFF))
	b[30] = byte(((n[7] >> 16) & 0xFF))
	b[31] = byte(((n[7] >> 24) & 0xFF))

	return b[:32]
}

type decodef func([]byte, []int32) error

type Decoder struct {
	b      [32]byte
	decode decodef
}

func NewDecoder(bitWidth uint) *Decoder {
	d := &Decoder{}

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
	for i := 0; i < (len(out)+7)/8; i++ {
		n, err := r.Read(d.b[:])
		if err != nil {
			return fmt.Errorf("decodeRLE:%s", err)
		}
		if err := d.decode(d.b[:n], buffer); err != nil {
			return fmt.Errorf("decodeRLE:%s", err)
		}

		extra := 8
		if ((i + 1) * 8) > len(out) {
			extra = len(out) - (i * 8)
		}

		for j := 0; j+1 < extra; j++ {
			out[i*8+j] = buffer[j]
		}

	}

	return nil
}

func (d *Decoder) decode1RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x1))
	out[1] = int32((b[0] & 0x2) >> 1)
	out[2] = int32((b[0] & 0x4) >> 2)
	out[3] = int32((b[0] & 0x8) >> 3)
	out[4] = int32((b[0] & 0x10) >> 4)
	out[5] = int32((b[0] & 0x20) >> 5)
	out[6] = int32((b[0] & 0x40) >> 6)
	out[7] = int32((b[0] & 0x80) >> 7)

	return nil
}

func (d *Decoder) decode2RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x3))
	out[1] = int32((b[0] & 0xc) >> 2)
	out[2] = int32((b[0] & 0x30) >> 4)
	out[3] = int32((b[0] & 0xc0) >> 6)
	out[4] = int32((b[1] & 0x3))
	out[5] = int32((b[1] & 0xc) >> 2)
	out[6] = int32((b[1] & 0x30) >> 4)
	out[7] = int32((b[1] & 0xc0) >> 6)

	return nil
}

func (d *Decoder) decode3RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x7))
	out[1] = int32((b[0] & 0x38) >> 3)
	out[2] = int32((b[0]&0xc0)>>6 | (b[1] & 0x1))
	out[3] = int32((b[1] & 0xe) >> 1)
	out[4] = int32((b[1] & 0x70) >> 4)
	out[5] = int32((b[1]&0x80)>>7 | (b[2]&0x3)<<1)
	out[6] = int32((b[2] & 0x1c) >> 2)
	out[7] = int32((b[2] & 0xe0) >> 5)

	return nil
}

func (d *Decoder) decode4RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0xf))
	out[1] = int32((b[0] & 0xf0) >> 4)
	out[2] = int32((b[1] & 0xf))
	out[3] = int32((b[1] & 0xf0) >> 4)
	out[4] = int32((b[2] & 0xf))
	out[5] = int32((b[2] & 0xf0) >> 4)
	out[6] = int32((b[3] & 0xf))
	out[7] = int32((b[3] & 0xf0) >> 4)

	return nil
}

func (d *Decoder) decode5RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x1f))
	out[1] = int32((b[0]&0xe0)>>5 | (b[1]&0x3)<<3)
	out[2] = int32((b[1] & 0x7c) >> 2)
	out[3] = int32((b[1]&0x80)>>7 | (b[2]&0xf)<<1)
	out[4] = int32((b[2]&0xf0)>>4 | (b[3] & 0x1))
	out[5] = int32((b[3] & 0x3e) >> 1)
	out[6] = int32((b[3]&0xc0)>>6 | (b[4]&0x7)<<2)
	out[7] = int32((b[4] & 0xf8) >> 3)

	return nil
}

func (d *Decoder) decode6RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x3f))
	out[1] = int32((b[0]&0xc0)>>6 | (b[1]&0xf)<<2)
	out[2] = int32((b[1]&0xf0)>>4 | (b[2]&0x3)<<4)
	out[3] = int32((b[2] & 0xfc) >> 2)
	out[4] = int32((b[3] & 0x3f))
	out[5] = int32((b[3]&0xc0)>>6 | (b[4]&0xf)<<2)
	out[6] = int32((b[4]&0xf0)>>4 | (b[5]&0x3)<<4)
	out[7] = int32((b[5] & 0xfc) >> 2)

	return nil
}

func (d *Decoder) decode7RLE(b []byte, out []int32) error {

	out[0] = int32((b[0] & 0x7f))
	out[1] = int32((b[0]&0x80)>>7 | (b[1]&0x3f)<<1)
	out[2] = int32((b[1]&0xc0)>>6 | (b[2]&0x1f)<<2)
	out[3] = int32((b[2]&0xe0)>>5 | (b[3]&0xf)<<3)
	out[4] = int32((b[3]&0xf0)>>4 | (b[4]&0x7)<<4)
	out[5] = int32((b[4]&0xf8)>>3 | (b[5]&0x3)<<5)
	out[6] = int32((b[5]&0xfc)>>2 | (b[6] & 0x1))
	out[7] = int32((b[6] & 0xfe) >> 1)

	return nil
}

func (d *Decoder) decode8RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]))
	out[1] = int32(int32(b[1]))
	out[2] = int32(int32(b[2]))
	out[3] = int32(int32(b[3]))
	out[4] = int32(int32(b[4]))
	out[5] = int32(int32(b[5]))
	out[6] = int32(int32(b[6]))
	out[7] = int32(int32(b[7]))

	return nil
}

func (d *Decoder) decode9RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x1)) << 8))
	out[1] = int32(int32((b[1]&0xfe)>>1) + (int32((b[2]&0x3)<<7) << 8))
	out[2] = int32(int32((b[2]&0xfc)>>2) + (int32((b[3]&0x7)<<6) << 8))
	out[3] = int32(int32((b[3]&0xf8)>>3) + (int32((b[4]&0xf)<<5) << 8))
	out[4] = int32(int32((b[4]&0xf0)>>4) + (int32((b[5]&0x1f)<<4) << 8))
	out[5] = int32(int32((b[5]&0xe0)>>5) + (int32((b[6]&0x3f)<<3) << 8))
	out[6] = int32(int32((b[6]&0xc0)>>6) + (int32((b[7]&0x7f)<<2) << 8))
	out[7] = int32(int32((b[7]&0x80)>>7) + (int32((b[8])<<1) << 8))

	return nil
}

func (d *Decoder) decode10RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x3)) << 8))
	out[1] = int32(int32((b[1]&0xfc)>>2) + (int32((b[2]&0xf)<<6) << 8))
	out[2] = int32(int32((b[2]&0xf0)>>4) + (int32((b[3]&0x3f)<<4) << 8))
	out[3] = int32(int32((b[3]&0xc0)>>6) + (int32((b[4])<<2) << 8))
	out[4] = int32(int32(b[5]) + (int32((b[6] & 0x3)) << 8))
	out[5] = int32(int32((b[6]&0xfc)>>2) + (int32((b[7]&0xf)<<6) << 8))
	out[6] = int32(int32((b[7]&0xf0)>>4) + (int32((b[8]&0x3f)<<4) << 8))
	out[7] = int32(int32((b[8]&0xc0)>>6) + (int32((b[9])<<2) << 8))

	return nil
}

func (d *Decoder) decode11RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x7)) << 8))
	out[1] = int32(int32((b[1]&0xf8)>>3) + (int32((b[2]&0x3f)<<5) << 8))
	out[2] = int32(int32((b[2]&0xc0)>>6) + (int32((b[3])<<2) << 8) + (int32((b[4] & 0x1)) << 16))
	out[3] = int32(int32((b[4]&0xfe)>>1) + (int32((b[5]&0xf)<<7) << 8))
	out[4] = int32(int32((b[5]&0xf0)>>4) + (int32((b[6]&0x7f)<<4) << 8))
	out[5] = int32(int32((b[6]&0x80)>>7) + (int32((b[7])<<1) << 8) + (int32((b[8] & 0x3)) << 16))
	out[6] = int32(int32((b[8]&0xfc)>>2) + (int32((b[9]&0x1f)<<6) << 8))
	out[7] = int32(int32((b[9]&0xe0)>>5) + (int32((b[10])<<3) << 8))

	return nil
}

func (d *Decoder) decode12RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0xf)) << 8))
	out[1] = int32(int32((b[1]&0xf0)>>4) + (int32((b[2])<<4) << 8))
	out[2] = int32(int32(b[3]) + (int32((b[4] & 0xf)) << 8))
	out[3] = int32(int32((b[4]&0xf0)>>4) + (int32((b[5])<<4) << 8))
	out[4] = int32(int32(b[6]) + (int32((b[7] & 0xf)) << 8))
	out[5] = int32(int32((b[7]&0xf0)>>4) + (int32((b[8])<<4) << 8))
	out[6] = int32(int32(b[9]) + (int32((b[10] & 0xf)) << 8))
	out[7] = int32(int32((b[10]&0xf0)>>4) + (int32((b[11])<<4) << 8))

	return nil
}

func (d *Decoder) decode13RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x1f)) << 8))
	out[1] = int32(int32((b[1]&0xe0)>>5) + (int32((b[2])<<3) << 8) + (int32((b[3] & 0x3)) << 16))
	out[2] = int32(int32((b[3]&0xfc)>>2) + (int32((b[4]&0x7f)<<6) << 8))
	out[3] = int32(int32((b[4]&0x80)>>7) + (int32((b[5])<<1) << 8) + (int32((b[6] & 0xf)) << 16))
	out[4] = int32(int32((b[6]&0xf0)>>4) + (int32((b[7])<<4) << 8) + (int32((b[8] & 0x1)) << 16))
	out[5] = int32(int32((b[8]&0xfe)>>1) + (int32((b[9]&0x3f)<<7) << 8))
	out[6] = int32(int32((b[9]&0xc0)>>6) + (int32((b[10])<<2) << 8) + (int32((b[11] & 0x7)) << 16))
	out[7] = int32(int32((b[11]&0xf8)>>3) + (int32((b[12])<<5) << 8))

	return nil
}

func (d *Decoder) decode14RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x3f)) << 8))
	out[1] = int32(int32((b[1]&0xc0)>>6) + (int32((b[2])<<2) << 8) + (int32((b[3] & 0xf)) << 16))
	out[2] = int32(int32((b[3]&0xf0)>>4) + (int32((b[4])<<4) << 8) + (int32((b[5] & 0x3)) << 16))
	out[3] = int32(int32((b[5]&0xfc)>>2) + (int32((b[6])<<6) << 8))
	out[4] = int32(int32(b[7]) + (int32((b[8] & 0x3f)) << 8))
	out[5] = int32(int32((b[8]&0xc0)>>6) + (int32((b[9])<<2) << 8) + (int32((b[10] & 0xf)) << 16))
	out[6] = int32(int32((b[10]&0xf0)>>4) + (int32((b[11])<<4) << 8) + (int32((b[12] & 0x3)) << 16))
	out[7] = int32(int32((b[12]&0xfc)>>2) + (int32((b[13])<<6) << 8))

	return nil
}

func (d *Decoder) decode15RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32((b[1] & 0x7f)) << 8))
	out[1] = int32(int32((b[1]&0x80)>>7) + (int32((b[2])<<1) << 8) + (int32((b[3] & 0x3f)) << 16))
	out[2] = int32(int32((b[3]&0xc0)>>6) + (int32((b[4])<<2) << 8) + (int32((b[5] & 0x1f)) << 16))
	out[3] = int32(int32((b[5]&0xe0)>>5) + (int32((b[6])<<3) << 8) + (int32((b[7] & 0xf)) << 16))
	out[4] = int32(int32((b[7]&0xf0)>>4) + (int32((b[8])<<4) << 8) + (int32((b[9] & 0x7)) << 16))
	out[5] = int32(int32((b[9]&0xf8)>>3) + (int32((b[10])<<5) << 8) + (int32((b[11] & 0x3)) << 16))
	out[6] = int32(int32((b[11]&0xfc)>>2) + (int32((b[12])<<6) << 8) + (int32((b[13] & 0x1)) << 16))
	out[7] = int32(int32((b[13]&0xfe)>>1) + (int32((b[14])<<7) << 8))

	return nil
}

func (d *Decoder) decode16RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8))
	out[1] = int32(int32(b[2]) + (int32(b[3]) << 8))
	out[2] = int32(int32(b[4]) + (int32(b[5]) << 8))
	out[3] = int32(int32(b[6]) + (int32(b[7]) << 8))
	out[4] = int32(int32(b[8]) + (int32(b[9]) << 8))
	out[5] = int32(int32(b[10]) + (int32(b[11]) << 8))
	out[6] = int32(int32(b[12]) + (int32(b[13]) << 8))
	out[7] = int32(int32(b[14]) + (int32(b[15]) << 8))

	return nil
}

func (d *Decoder) decode17RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x1)) << 16))
	out[1] = int32(int32((b[2]&0xfe)>>1) + (int32((b[3])<<7) << 8) + (int32((b[4] & 0x3)) << 16))
	out[2] = int32(int32((b[4]&0xfc)>>2) + (int32((b[5])<<6) << 8) + (int32((b[6] & 0x7)) << 16))
	out[3] = int32(int32((b[6]&0xf8)>>3) + (int32((b[7])<<5) << 8) + (int32((b[8] & 0xf)) << 16))
	out[4] = int32(int32((b[8]&0xf0)>>4) + (int32((b[9])<<4) << 8) + (int32((b[10] & 0x1f)) << 16))
	out[5] = int32(int32((b[10]&0xe0)>>5) + (int32((b[11])<<3) << 8) + (int32((b[12] & 0x3f)) << 16))
	out[6] = int32(int32((b[12]&0xc0)>>6) + (int32((b[13])<<2) << 8) + (int32((b[14] & 0x7f)) << 16))
	out[7] = int32(int32((b[14]&0x80)>>7) + (int32((b[15])<<1) << 8) + (int32(b[16]) << 16))

	return nil
}

func (d *Decoder) decode18RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x3)) << 16))
	out[1] = int32(int32((b[2]&0xfc)>>2) + (int32((b[3])<<6) << 8) + (int32((b[4] & 0xf)) << 16))
	out[2] = int32(int32((b[4]&0xf0)>>4) + (int32((b[5])<<4) << 8) + (int32((b[6] & 0x3f)) << 16))
	out[3] = int32(int32((b[6]&0xc0)>>6) + (int32((b[7])<<2) << 8) + (int32(b[8]) << 16))
	out[4] = int32(int32(b[9]) + (int32(b[10]) << 8) + (int32((b[11] & 0x3)) << 16))
	out[5] = int32(int32((b[11]&0xfc)>>2) + (int32((b[12])<<6) << 8) + (int32((b[13] & 0xf)) << 16))
	out[6] = int32(int32((b[13]&0xf0)>>4) + (int32((b[14])<<4) << 8) + (int32((b[15] & 0x3f)) << 16))
	out[7] = int32(int32((b[15]&0xc0)>>6) + (int32((b[16])<<2) << 8) + (int32(b[17]) << 16))

	return nil
}

func (d *Decoder) decode19RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x7)) << 16))
	out[1] = int32(int32((b[2]&0xf8)>>3) + (int32((b[3])<<5) << 8) + (int32((b[4] & 0x3f)) << 16))
	out[2] = int32(int32((b[4]&0xc0)>>6) + (int32((b[5])<<2) << 8) + (int32(b[6]) << 16) + (int32((b[7] & 0x1)) << 24))
	out[3] = int32(int32((b[7]&0xfe)>>1) + (int32((b[8])<<7) << 8) + (int32((b[9] & 0xf)) << 16))
	out[4] = int32(int32((b[9]&0xf0)>>4) + (int32((b[10])<<4) << 8) + (int32((b[11] & 0x7f)) << 16))
	out[5] = int32(int32((b[11]&0x80)>>7) + (int32((b[12])<<1) << 8) + (int32(b[13]) << 16) + (int32((b[14] & 0x3)) << 24))
	out[6] = int32(int32((b[14]&0xfc)>>2) + (int32((b[15])<<6) << 8) + (int32((b[16] & 0x1f)) << 16))
	out[7] = int32(int32((b[16]&0xe0)>>5) + (int32((b[17])<<3) << 8) + (int32(b[18]) << 16))

	return nil
}

func (d *Decoder) decode20RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0xf)) << 16))
	out[1] = int32(int32((b[2]&0xf0)>>4) + (int32((b[3])<<4) << 8) + (int32(b[4]) << 16))
	out[2] = int32(int32(b[5]) + (int32(b[6]) << 8) + (int32((b[7] & 0xf)) << 16))
	out[3] = int32(int32((b[7]&0xf0)>>4) + (int32((b[8])<<4) << 8) + (int32(b[9]) << 16))
	out[4] = int32(int32(b[10]) + (int32(b[11]) << 8) + (int32((b[12] & 0xf)) << 16))
	out[5] = int32(int32((b[12]&0xf0)>>4) + (int32((b[13])<<4) << 8) + (int32(b[14]) << 16))
	out[6] = int32(int32(b[15]) + (int32(b[16]) << 8) + (int32((b[17] & 0xf)) << 16))
	out[7] = int32(int32((b[17]&0xf0)>>4) + (int32((b[18])<<4) << 8) + (int32(b[19]) << 16))

	return nil
}

func (d *Decoder) decode21RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x1f)) << 16))
	out[1] = int32(int32((b[2]&0xe0)>>5) + (int32((b[3])<<3) << 8) + (int32(b[4]) << 16) + (int32((b[5] & 0x3)) << 24))
	out[2] = int32(int32((b[5]&0xfc)>>2) + (int32((b[6])<<6) << 8) + (int32((b[7] & 0x7f)) << 16))
	out[3] = int32(int32((b[7]&0x80)>>7) + (int32((b[8])<<1) << 8) + (int32(b[9]) << 16) + (int32((b[10] & 0xf)) << 24))
	out[4] = int32(int32((b[10]&0xf0)>>4) + (int32((b[11])<<4) << 8) + (int32(b[12]) << 16) + (int32((b[13] & 0x1)) << 24))
	out[5] = int32(int32((b[13]&0xfe)>>1) + (int32((b[14])<<7) << 8) + (int32((b[15] & 0x3f)) << 16))
	out[6] = int32(int32((b[15]&0xc0)>>6) + (int32((b[16])<<2) << 8) + (int32(b[17]) << 16) + (int32((b[18] & 0x7)) << 24))
	out[7] = int32(int32((b[18]&0xf8)>>3) + (int32((b[19])<<5) << 8) + (int32(b[20]) << 16))

	return nil
}

func (d *Decoder) decode22RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x3f)) << 16))
	out[1] = int32(int32((b[2]&0xc0)>>6) + (int32((b[3])<<2) << 8) + (int32(b[4]) << 16) + (int32((b[5] & 0xf)) << 24))
	out[2] = int32(int32((b[5]&0xf0)>>4) + (int32((b[6])<<4) << 8) + (int32(b[7]) << 16) + (int32((b[8] & 0x3)) << 24))
	out[3] = int32(int32((b[8]&0xfc)>>2) + (int32((b[9])<<6) << 8) + (int32(b[10]) << 16))
	out[4] = int32(int32(b[11]) + (int32(b[12]) << 8) + (int32((b[13] & 0x3f)) << 16))
	out[5] = int32(int32((b[13]&0xc0)>>6) + (int32((b[14])<<2) << 8) + (int32(b[15]) << 16) + (int32((b[16] & 0xf)) << 24))
	out[6] = int32(int32((b[16]&0xf0)>>4) + (int32((b[17])<<4) << 8) + (int32(b[18]) << 16) + (int32((b[19] & 0x3)) << 24))
	out[7] = int32(int32((b[19]&0xfc)>>2) + (int32((b[20])<<6) << 8) + (int32(b[21]) << 16))

	return nil
}

func (d *Decoder) decode23RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32((b[2] & 0x7f)) << 16))
	out[1] = int32(int32((b[2]&0x80)>>7) + (int32((b[3])<<1) << 8) + (int32(b[4]) << 16) + (int32((b[5] & 0x3f)) << 24))
	out[2] = int32(int32((b[5]&0xc0)>>6) + (int32((b[6])<<2) << 8) + (int32(b[7]) << 16) + (int32((b[8] & 0x1f)) << 24))
	out[3] = int32(int32((b[8]&0xe0)>>5) + (int32((b[9])<<3) << 8) + (int32(b[10]) << 16) + (int32((b[11] & 0xf)) << 24))
	out[4] = int32(int32((b[11]&0xf0)>>4) + (int32((b[12])<<4) << 8) + (int32(b[13]) << 16) + (int32((b[14] & 0x7)) << 24))
	out[5] = int32(int32((b[14]&0xf8)>>3) + (int32((b[15])<<5) << 8) + (int32(b[16]) << 16) + (int32((b[17] & 0x3)) << 24))
	out[6] = int32(int32((b[17]&0xfc)>>2) + (int32((b[18])<<6) << 8) + (int32(b[19]) << 16) + (int32((b[20] & 0x1)) << 24))
	out[7] = int32(int32((b[20]&0xfe)>>1) + (int32((b[21])<<7) << 8) + (int32(b[22]) << 16))

	return nil
}

func (d *Decoder) decode24RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16))
	out[1] = int32(int32(b[3]) + (int32(b[4]) << 8) + (int32(b[5]) << 16))
	out[2] = int32(int32(b[6]) + (int32(b[7]) << 8) + (int32(b[8]) << 16))
	out[3] = int32(int32(b[9]) + (int32(b[10]) << 8) + (int32(b[11]) << 16))
	out[4] = int32(int32(b[12]) + (int32(b[13]) << 8) + (int32(b[14]) << 16))
	out[5] = int32(int32(b[15]) + (int32(b[16]) << 8) + (int32(b[17]) << 16))
	out[6] = int32(int32(b[18]) + (int32(b[19]) << 8) + (int32(b[20]) << 16))
	out[7] = int32(int32(b[21]) + (int32(b[22]) << 8) + (int32(b[23]) << 16))

	return nil
}

func (d *Decoder) decode25RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x1)) << 24))
	out[1] = int32(int32((b[3]&0xfe)>>1) + (int32((b[4])<<7) << 8) + (int32(b[5]) << 16) + (int32((b[6] & 0x3)) << 24))
	out[2] = int32(int32((b[6]&0xfc)>>2) + (int32((b[7])<<6) << 8) + (int32(b[8]) << 16) + (int32((b[9] & 0x7)) << 24))
	out[3] = int32(int32((b[9]&0xf8)>>3) + (int32((b[10])<<5) << 8) + (int32(b[11]) << 16) + (int32((b[12] & 0xf)) << 24))
	out[4] = int32(int32((b[12]&0xf0)>>4) + (int32((b[13])<<4) << 8) + (int32(b[14]) << 16) + (int32((b[15] & 0x1f)) << 24))
	out[5] = int32(int32((b[15]&0xe0)>>5) + (int32((b[16])<<3) << 8) + (int32(b[17]) << 16) + (int32((b[18] & 0x3f)) << 24))
	out[6] = int32(int32((b[18]&0xc0)>>6) + (int32((b[19])<<2) << 8) + (int32(b[20]) << 16) + (int32((b[21] & 0x7f)) << 24))
	out[7] = int32(int32((b[21]&0x80)>>7) + (int32((b[22])<<1) << 8) + (int32(b[23]) << 16) + (int32(b[24]) << 24))

	return nil
}

func (d *Decoder) decode26RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x3)) << 24))
	out[1] = int32(int32((b[3]&0xfc)>>2) + (int32((b[4])<<6) << 8) + (int32(b[5]) << 16) + (int32((b[6] & 0xf)) << 24))
	out[2] = int32(int32((b[6]&0xf0)>>4) + (int32((b[7])<<4) << 8) + (int32(b[8]) << 16) + (int32((b[9] & 0x3f)) << 24))
	out[3] = int32(int32((b[9]&0xc0)>>6) + (int32((b[10])<<2) << 8) + (int32(b[11]) << 16) + (int32(b[12]) << 24))
	out[4] = int32(int32(b[13]) + (int32(b[14]) << 8) + (int32(b[15]) << 16) + (int32((b[16] & 0x3)) << 24))
	out[5] = int32(int32((b[16]&0xfc)>>2) + (int32((b[17])<<6) << 8) + (int32(b[18]) << 16) + (int32((b[19] & 0xf)) << 24))
	out[6] = int32(int32((b[19]&0xf0)>>4) + (int32((b[20])<<4) << 8) + (int32(b[21]) << 16) + (int32((b[22] & 0x3f)) << 24))
	out[7] = int32(int32((b[22]&0xc0)>>6) + (int32((b[23])<<2) << 8) + (int32(b[24]) << 16) + (int32(b[25]) << 24))

	return nil
}

func (d *Decoder) decode27RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x7)) << 24))
	out[1] = int32(int32((b[3]&0xf8)>>3) + (int32((b[4])<<5) << 8) + (int32(b[5]) << 16) + (int32((b[6] & 0x3f)) << 24))
	out[2] = int32(int32((b[6]&0xc0)>>6) + (int32((b[7])<<2) << 8) + (int32(b[8]) << 16) + (int32(b[9]) << 24) + (int32((b[10] & 0x1)) << 32))
	out[3] = int32(int32((b[10]&0xfe)>>1) + (int32((b[11])<<7) << 8) + (int32(b[12]) << 16) + (int32((b[13] & 0xf)) << 24))
	out[4] = int32(int32((b[13]&0xf0)>>4) + (int32((b[14])<<4) << 8) + (int32(b[15]) << 16) + (int32((b[16] & 0x7f)) << 24))
	out[5] = int32(int32((b[16]&0x80)>>7) + (int32((b[17])<<1) << 8) + (int32(b[18]) << 16) + (int32(b[19]) << 24) + (int32((b[20] & 0x3)) << 32))
	out[6] = int32(int32((b[20]&0xfc)>>2) + (int32((b[21])<<6) << 8) + (int32(b[22]) << 16) + (int32((b[23] & 0x1f)) << 24))
	out[7] = int32(int32((b[23]&0xe0)>>5) + (int32((b[24])<<3) << 8) + (int32(b[25]) << 16) + (int32(b[26]) << 24))

	return nil
}

func (d *Decoder) decode28RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0xf)) << 24))
	out[1] = int32(int32((b[3]&0xf0)>>4) + (int32((b[4])<<4) << 8) + (int32(b[5]) << 16) + (int32(b[6]) << 24))
	out[2] = int32(int32(b[7]) + (int32(b[8]) << 8) + (int32(b[9]) << 16) + (int32((b[10] & 0xf)) << 24))
	out[3] = int32(int32((b[10]&0xf0)>>4) + (int32((b[11])<<4) << 8) + (int32(b[12]) << 16) + (int32(b[13]) << 24))
	out[4] = int32(int32(b[14]) + (int32(b[15]) << 8) + (int32(b[16]) << 16) + (int32((b[17] & 0xf)) << 24))
	out[5] = int32(int32((b[17]&0xf0)>>4) + (int32((b[18])<<4) << 8) + (int32(b[19]) << 16) + (int32(b[20]) << 24))
	out[6] = int32(int32(b[21]) + (int32(b[22]) << 8) + (int32(b[23]) << 16) + (int32((b[24] & 0xf)) << 24))
	out[7] = int32(int32((b[24]&0xf0)>>4) + (int32((b[25])<<4) << 8) + (int32(b[26]) << 16) + (int32(b[27]) << 24))

	return nil
}

func (d *Decoder) decode29RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x1f)) << 24))
	out[1] = int32(int32((b[3]&0xe0)>>5) + (int32((b[4])<<3) << 8) + (int32(b[5]) << 16) + (int32(b[6]) << 24) + (int32((b[7] & 0x3)) << 32))
	out[2] = int32(int32((b[7]&0xfc)>>2) + (int32((b[8])<<6) << 8) + (int32(b[9]) << 16) + (int32((b[10] & 0x7f)) << 24))
	out[3] = int32(int32((b[10]&0x80)>>7) + (int32((b[11])<<1) << 8) + (int32(b[12]) << 16) + (int32(b[13]) << 24) + (int32((b[14] & 0xf)) << 32))
	out[4] = int32(int32((b[14]&0xf0)>>4) + (int32((b[15])<<4) << 8) + (int32(b[16]) << 16) + (int32(b[17]) << 24) + (int32((b[18] & 0x1)) << 32))
	out[5] = int32(int32((b[18]&0xfe)>>1) + (int32((b[19])<<7) << 8) + (int32(b[20]) << 16) + (int32((b[21] & 0x3f)) << 24))
	out[6] = int32(int32((b[21]&0xc0)>>6) + (int32((b[22])<<2) << 8) + (int32(b[23]) << 16) + (int32(b[24]) << 24) + (int32((b[25] & 0x7)) << 32))
	out[7] = int32(int32((b[25]&0xf8)>>3) + (int32((b[26])<<5) << 8) + (int32(b[27]) << 16) + (int32(b[28]) << 24))

	return nil
}

func (d *Decoder) decode30RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x3f)) << 24))
	out[1] = int32(int32((b[3]&0xc0)>>6) + (int32((b[4])<<2) << 8) + (int32(b[5]) << 16) + (int32(b[6]) << 24) + (int32((b[7] & 0xf)) << 32))
	out[2] = int32(int32((b[7]&0xf0)>>4) + (int32((b[8])<<4) << 8) + (int32(b[9]) << 16) + (int32(b[10]) << 24) + (int32((b[11] & 0x3)) << 32))
	out[3] = int32(int32((b[11]&0xfc)>>2) + (int32((b[12])<<6) << 8) + (int32(b[13]) << 16) + (int32(b[14]) << 24))
	out[4] = int32(int32(b[15]) + (int32(b[16]) << 8) + (int32(b[17]) << 16) + (int32((b[18] & 0x3f)) << 24))
	out[5] = int32(int32((b[18]&0xc0)>>6) + (int32((b[19])<<2) << 8) + (int32(b[20]) << 16) + (int32(b[21]) << 24) + (int32((b[22] & 0xf)) << 32))
	out[6] = int32(int32((b[22]&0xf0)>>4) + (int32((b[23])<<4) << 8) + (int32(b[24]) << 16) + (int32(b[25]) << 24) + (int32((b[26] & 0x3)) << 32))
	out[7] = int32(int32((b[26]&0xfc)>>2) + (int32((b[27])<<6) << 8) + (int32(b[28]) << 16) + (int32(b[29]) << 24))

	return nil
}

func (d *Decoder) decode31RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32((b[3] & 0x7f)) << 24))
	out[1] = int32(int32((b[3]&0x80)>>7) + (int32((b[4])<<1) << 8) + (int32(b[5]) << 16) + (int32(b[6]) << 24) + (int32((b[7] & 0x3f)) << 32))
	out[2] = int32(int32((b[7]&0xc0)>>6) + (int32((b[8])<<2) << 8) + (int32(b[9]) << 16) + (int32(b[10]) << 24) + (int32((b[11] & 0x1f)) << 32))
	out[3] = int32(int32((b[11]&0xe0)>>5) + (int32((b[12])<<3) << 8) + (int32(b[13]) << 16) + (int32(b[14]) << 24) + (int32((b[15] & 0xf)) << 32))
	out[4] = int32(int32((b[15]&0xf0)>>4) + (int32((b[16])<<4) << 8) + (int32(b[17]) << 16) + (int32(b[18]) << 24) + (int32((b[19] & 0x7)) << 32))
	out[5] = int32(int32((b[19]&0xf8)>>3) + (int32((b[20])<<5) << 8) + (int32(b[21]) << 16) + (int32(b[22]) << 24) + (int32((b[23] & 0x3)) << 32))
	out[6] = int32(int32((b[23]&0xfc)>>2) + (int32((b[24])<<6) << 8) + (int32(b[25]) << 16) + (int32(b[26]) << 24) + (int32((b[27] & 0x1)) << 32))
	out[7] = int32(int32((b[27]&0xfe)>>1) + (int32((b[28])<<7) << 8) + (int32(b[29]) << 16) + (int32(b[30]) << 24))

	return nil
}

func (d *Decoder) decode32RLE(b []byte, out []int32) error {

	out[0] = int32(int32(b[0]) + (int32(b[1]) << 8) + (int32(b[2]) << 16) + (int32(b[3]) << 24))
	out[1] = int32(int32(b[4]) + (int32(b[5]) << 8) + (int32(b[6]) << 16) + (int32(b[7]) << 24))
	out[2] = int32(int32(b[8]) + (int32(b[9]) << 8) + (int32(b[10]) << 16) + (int32(b[11]) << 24))
	out[3] = int32(int32(b[12]) + (int32(b[13]) << 8) + (int32(b[14]) << 16) + (int32(b[15]) << 24))
	out[4] = int32(int32(b[16]) + (int32(b[17]) << 8) + (int32(b[18]) << 16) + (int32(b[19]) << 24))
	out[5] = int32(int32(b[20]) + (int32(b[21]) << 8) + (int32(b[22]) << 16) + (int32(b[23]) << 24))
	out[6] = int32(int32(b[24]) + (int32(b[25]) << 8) + (int32(b[26]) << 16) + (int32(b[27]) << 24))
	out[7] = int32(int32(b[28]) + (int32(b[29]) << 8) + (int32(b[30]) << 16) + (int32(b[31]) << 24))

	return nil
}
