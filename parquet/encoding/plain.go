package encoding

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// Plain
type plainDecoder struct {
	r     io.Reader
	count uint
}

// NewPlainDecoder creates a new Decoder that uses the PLAIN=0 encoding
func NewPlainDecoder(r io.Reader, numValues uint) Decoder {
	return &plainDecoder{r, numValues}
}

// DecodeBool
func (d *plainDecoder) DecodeBool(out []bool) (uint, error) {
	dec := bitpacking.NewDecoder(d.r, 1)

	for i := uint(0); i < d.count; i++ {
		if dec.Scan() {
			out[i] = dec.Value() == 1
		}

		if err := dec.Err(); err != nil {
			return i, err
		}
	}

	return d.count, nil
}

// DecodeInt32
func (d *plainDecoder) DecodeInt32(out []int32) (uint, error) {
	count := d.count

	for i := uint(0); i < count; i++ {
		var value int32
		err := binary.Read(d.r, binary.LittleEndian, &value)
		if err != nil {
			return i, fmt.Errorf("expected %d int32 but got only %d: %s", count, i, err) // FIXME
		}

		out[i] = value
	}

	return count, nil
}

// DecodeInt64
func (d *plainDecoder) DecodeInt64(out []int64) (uint, error) {
	count := d.count
	var value int64

	for i := uint(0); i < min(count, uint(len(out))); i++ {
		err := binary.Read(d.r, binary.LittleEndian, &value)
		if err != nil {
			return i, fmt.Errorf("expected %d int64 but got only %d: %s", count, i, err) // FIXME
		}

		out[i] = value
	}

	return count, nil
}

// DecodeStr , returns the number of element read, or error
func (d *plainDecoder) DecodeString(out []string) (uint, error) {
	var count uint
	var size int32

	for i := uint(0); i < min(d.count, uint(len(out))); i++ {
		err := binary.Read(d.r, binary.LittleEndian, &size)
		if err != nil {
			return 0, err
		}
		p := make([]byte, size)
		n, err := d.r.Read(p)
		if err != nil {
			return i, fmt.Errorf("plain decoder: short read: %s", err)
		}

		out[i] = string(p[:n])
		count++
	}

	return count, nil
}

// DecodeStr , returns the number of element read, or error
func (d *plainDecoder) DecodeByteArray(out [][]byte) (uint, error) {
	var count uint

	var size int32

	for i := uint(0); i < min(d.count, uint(len(out))); i++ {
		err := binary.Read(d.r, binary.LittleEndian, &size)
		if err != nil {
			return 0, err
		}
		p := make([]byte, size)
		n, err := d.r.Read(p)
		if err != nil {
			return i, fmt.Errorf("plain decoder: short read: %s", err)
		}
		out[i] = p[:n]
		count++
	}

	return count, nil
}

// DecodeFloat32 returns the number of elements read, or error
// The data has to be 4 bytes IEEE little endian back to back
func (d *plainDecoder) DecodeFloat32(out []float32) (uint, error) {
	var count uint

	var value float32

	for i := uint(0); i < min(d.count, uint(len(out))); i++ {
		err := binary.Read(d.r, binary.LittleEndian, &value)
		if err != nil {
			return i, fmt.Errorf("plain decoder: binary.Read: %s", err)
		}

		out[i] = value
		count++
	}

	return count, nil
}

// DecodeFloat64 returns the number of elements read, or error
// The data has to be 8 bytes IEEE little endian back to back
func (d *plainDecoder) DecodeFloat64(out []float64) (uint, error) {
	var count uint

	var value float64

	for i := uint(0); i < min(d.count, uint(len(out))); i++ {
		err := binary.Read(d.r, binary.LittleEndian, &value)
		if err != nil {
			return 0, fmt.Errorf("plain decoder: binary.Read: %s", err)
		}
		out[i] = value
		count++
	}

	return count, nil
}

// plain Encoder
type plainEncoder struct {
	numValues int
}

// NewPlainEncoder creates an encoder that uses the Plain encoding to store data
// inside a DataPage
func NewPlainEncoder() Encoder {
	return &plainEncoder{}
}

func (p *plainEncoder) Flush() error {
	return nil
}

func (p *plainEncoder) NumValues() int {
	return p.numValues
}

func (p *plainEncoder) Type() thrift.Encoding {
	return thrift.Encoding_PLAIN
}

// WriteBool
func (e *plainEncoder) WriteBool(w io.Writer, v []bool) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteInt32
func (e *plainEncoder) WriteInt32(w io.Writer, v []int32) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteInt64
func (e *plainEncoder) WriteInt64(w io.Writer, v []int64) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteFloat32
func (e *plainEncoder) WriteFloat32(w io.Writer, v []float32) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteFloat64
func (e *plainEncoder) WriteFloat64(w io.Writer, v []float64) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteByteArray
func (e *plainEncoder) WriteByteArray(w io.Writer, v [][]byte) error {
	e.numValues += len(v)
	for _, b := range v {
		err := binary.Write(w, binary.LittleEndian, len(b))
		if err != nil {
			return fmt.Errorf("could not write byte array len: %s", err)
		}
		err = binary.Write(w, binary.LittleEndian, b)
		if err != nil {
			return fmt.Errorf("could not write byte array: %s", err)
		}
	}

	return nil
}
