package bitpacking

//go:generate python gen.py
//go:generate gofmt -w codec_generate.go

import (
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

	if e.format == RLE {
		j := 0
		for i := 0; i < len(values); i++ {

			buffer[j] = values[i]
			j++
			if j == 8 {
				n, err := w.Write(e.encodeRLE(buffer))
				total += n
				if err != nil {
					return total, err
				}
				j = 0
			}
		}

		if j > 0 {
			for i := j; i < 8; i++ {
				buffer[i] = 0
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
