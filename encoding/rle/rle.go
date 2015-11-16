package rle

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Decoder is a simple RLE decoder
type Decoder struct {
	r         *bufio.Reader
	count     uint64
	value     int64
	err       error
	hasHeader bool
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReader(r)}
}

func (d *Decoder) Scan() bool {
	if d.err != nil {
		return false
	}

	if !d.hasHeader {
		// read count
		count, err := binary.ReadUvarint(d.r)
		if err != nil {
			d.setErr(err)
			return false
		}

		d.count = count >> 1
		d.hasHeader = true

		value, err := d.r.ReadByte()
		if err != nil {
			d.setErr(err)
			return false
		}

		d.value = int64(value)

	}

	if d.count == 0 {
		return false
	}

	d.count--
	return true
}

func (d *Decoder) Value() int64 {
	return d.value
}

func (d *Decoder) setErr(err error) {
	if d.err == nil || d.err == io.EOF {
		d.err = err
	}
}

func (d *Decoder) Err() error {
	if d.err == io.EOF {
		return nil
	}

	return d.err
}

// An Encoder serializes data in the RLE format.
type Encoder struct {
	w      io.Writer // where to send the data
	buffer []int64   // last seen values
	err    error
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w, make([]int64, 8), nil}
}

func (e *Encoder) Encode(value int64) error {
	if len(e.buffer) > 0 {
		lastSeenValue := e.buffer[len(e.buffer)-1]
		if value == lastSeenValue {
			return nil
		} else {

		}
	} else {
		e.buffer[0] = value
	}

	return nil
}

func (e *Encoder) Flush() error {
	// encode

	return nil
}
