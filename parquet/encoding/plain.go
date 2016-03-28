package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// type Decoder interface {
// 	Bool() bool
// 	Int32() int32
// 	Int64() int64
// 	//	Float() float
// 	//	Double() double
// 	Byte() []byte
// }

// Plain

// Dictionary Encoding

// Delta Bit Packing

// Delta Length Byte Array

// Delta Byte Array

type Decoder struct {
	r     io.Reader
	t     thrift.Type
	count int
}

// NewPlainDecoder creates a new PageDecoder
func NewPlainDecoder(r io.Reader, t thrift.Type, numValues int) *Decoder {
	return &Decoder{r, t, numValues}
}

// DecodeInt32
func (d *Decoder) DecodeInt32(out []int32) (int, error) {
	count := d.count

	switch d.t {

	case thrift.Type_INT32:
		var err error = nil

		for i := 0; i < count; i++ {
			var value int32 = 0
			err = binary.Read(d.r, binary.LittleEndian, &value)
			if err != nil {
				panic(fmt.Sprintf("expected %d int32 but got only %d: %s", count, i, err)) // FIXME
			}

			out = append(out, value)
		}
	default:
		return -1, fmt.Errorf("unsupported string format: %s for type int32", d.t)
	}

	return count, nil
}

// DecodeInt64
func (d *Decoder) DecodeInt64(out []int64) (int, error) {
	count := d.count

	switch d.t {

	case thrift.Type_INT64:
		var err error

		for i := 0; i < count; i++ {
			var value int64
			err = binary.Read(d.r, binary.LittleEndian, &value)
			if err != nil {
				panic(fmt.Sprintf("expected %d int64 but got only %d: %s", count, i, err)) // FIXME
			}

			out = append(out, value)
		}

	default:
		log.Println("unsupported string format: ", d.t, " for type int64")
	}

	return count, nil
}

// DecodeStr , returns the number of element read, or error
func (d *Decoder) DecodeStr(out []string) (int, error) {
	count := d.count

	switch d.t {
	case thrift.Type_BYTE_ARRAY:
		var size int32

		for i := 0; i < count; i++ {
			err := binary.Read(d.r, binary.LittleEndian, &size)
			if err != nil {
				panic(err)
			}
			p := make([]byte, size)
			n, err := d.r.Read(p)
			if err != nil {
				return i, fmt.Errorf("plain decoder: short read: %s", err)
			}

			value := string(p[:n])
			out = append(out, value)
		}

	default:
		log.Println("unsupported string format: ", d.t, " for type string")
	}

	return count, nil
}

// plain Encoder
type plain struct {
	numValues int
}

// NewPlainEncoder creates an encoder that uses the Plain encoding to store data
// inside a DataPage
func NewPlainEncoder() Encoder {
	return &plain{}
}

func (p *plain) Flush() error {
	return nil
}

func (p *plain) NumValues() int {
	return p.numValues
}

func (p *plain) Type() thrift.Encoding {
	return thrift.Encoding_PLAIN
}

/*
- BOOLEAN: 1 bit boolean
- INT32: 32 bit signed int
- INT64: 64 bit signed int
- INT96: 96 bit signed int
- FLOAT: IEEE 32-bit floating point values
- DOUBLE: IEEE 64-bit floating point values
- BYTE_ARRAY: arbitrarily long byte arrays
*/
func (e *plain) WriteBool(w io.Writer, v []bool) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

func (e *plain) WriteInt32(w io.Writer, v []int32) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

func (e *plain) WriteInt64(w io.Writer, v []int64) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

func (e *plain) WriteFloat32(w io.Writer, v []float32) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

func (e *plain) WriteFloat64(w io.Writer, v []float64) error {
	e.numValues += len(v)
	return binary.Write(w, binary.LittleEndian, v)
}

func (e *plain) WriteByteArray(w io.Writer, v [][]byte) error {
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
