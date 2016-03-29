package bitpacking

import (
	"bufio"
	"io"
)

// Encoder handles 1-8 bit encoded int values smaller than 2^32
type Encoder struct {
	w        *bufio.Writer
	bitWidth uint
	buff     byte // the current byte being written
	bits     uint // track how many bits were set in the current buffer
}

// NewEncoder returns a new encoder that will write on the io.Writer.
// bitWidth is a number between 1 and 8.
func NewEncoder(w io.Writer, bitWidth int) *Encoder {
	if bitWidth > 8 {
		panic("bitWidth greater than 8 is not supported")
	}
	return &Encoder{bufio.NewWriter(w), uint(bitWidth), byte(0), 0}
}

// Write writes the value inside the current byte.
// it might or might not write to the underlying io.Writer.
// call flush to ensure all the data is handled properly
func (e *Encoder) Write(value int64) (err error) {
	e.buff |= (byte(value) << e.bits)

	e.bits += e.bitWidth

	if e.bits >= 8 {
		err = e.w.WriteByte(e.buff)
		e.bits -= 8
		if e.bits > 0 {
			e.buff = byte(value) >> (e.bitWidth - e.bits)
		} else {
			e.buff = 0x00
		}

		return
	}

	return nil
}

// Flush writes to io.Writer all the bending bytes
func (e *Encoder) Flush() (err error) {
	if e.bits > 0 {
		err = e.w.WriteByte(e.buff)
		e.bits = 0
		e.buff = 0x00
	}
	return e.w.Flush()
}
