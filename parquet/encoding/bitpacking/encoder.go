package bitpacking

import (
	"bufio"
	"io"
)

type format int

const (
	RLE format = iota
	BitPacked
)

// Encoder handles 1-8 bit encoded int values smaller than 2^32
type Encoder struct {
	w        *bufio.Writer
	bitWidth uint
	buff     [8]byte // the current byte being written
	bits     uint    // track how many bits were set in the current buffer
	format   format
	count    uint
}

// NewEncoder returns a new encoder that will write on the io.Writer.
// bitWidth is a number between 1 and 32.
func NewEncoder(w io.Writer, bitWidth uint, format format) *Encoder {
	if bitWidth == 0 || bitWidth > 32 {
		panic("invalid 0 > bitWidth <= 32")
	}
	return &Encoder{w: bufio.NewWriter(w), bitWidth: uint(bitWidth), bits: 0, format: format}
}

// Write writes the value inside the current byte.
// it might or might not write to the underlying io.Writer.
// call flush to ensure all the data is handled properly
func (e *Encoder) Write(value int64) (err error) {

	// e.buff |= (byte(value) << e.bits)

	// e.bits += e.bitWidth

	// if e.bits >= 8 {
	// 	err = e.w.WriteByte(e.buff)
	// 	e.bits -= 8
	// 	if e.bits > 0 {
	// 		e.buff = byte(value) >> (e.bitWidth - e.bits)
	// 	} else {
	// 		e.buff = 0x00
	// 	}

	// 	return
	//}

	return nil
}

// Flush writes to io.Writer all the pending bytes
func (e *Encoder) Flush() (err error) {
	if e.bits > 0 {
		// err = e.w.WriteByte(e.buff)
		// e.bits = 0
		// e.buff = 0x00
	}
	return e.w.Flush()
}
