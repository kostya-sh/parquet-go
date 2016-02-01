package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/kostya-sh/parquet-go/parquetformat"
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
	t     parquetformat.Type
	count int
}

func NewPlainDecoder(r io.Reader, t parquetformat.Type, numValues int) *Decoder {
	return &Decoder{r, t, numValues}
}

// DecodeInt32
func (d *Decoder) DecodeInt32(out []int32) (int, error) {
	count := d.count

	switch d.t {

	case parquetformat.Type_INT32:
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
		log.Println("unsupported string format: ", d.t, " for type int32")
	}

	return count, nil
}

// DecodeInt64
func (d *Decoder) DecodeInt64(out []int64) (int, error) {
	count := d.count

	switch d.t {

	case parquetformat.Type_INT64:
		var err error = nil

		for i := 0; i < count; i++ {
			var value int64 = 0
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
	case parquetformat.Type_BYTE_ARRAY:
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
			log.Println("plain:str:", value)
			out = append(out, value)
		}

	default:
		log.Println("unsupported string format: ", d.t, " for type string")
	}
	return count, nil
}

// Encoder
type Encoder interface {
	WriteInt32(int32) error
	WriteInt64(int64) error
	Flush() error
	NumValues() int
}

type plain struct {
	w         io.Writer
	numValues int
}

func NewPlainEncoder(w io.Writer) Encoder {
	return &plain{w: w}
}

func (p *plain) Flush() error {
	return nil
}

func (p *plain) NumValues() int {
	return p.numValues
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
func (e *plain) WriteBoolean(v bool) error {

	return nil
}

func (e *plain) WriteInt32(v int32) error {
	return binary.Write(e.w, binary.LittleEndian, v)
}

func (e *plain) WriteInt64(v int64) error {
	return binary.Write(e.w, binary.LittleEndian, v)
}

func (e *plain) WriteFloat(v float32) error {
	return nil
}

func (e *plain) WriteDouble(v float64) error {
	return nil
}

func (e *plain) WriteByteArray(v []byte) error {
	return nil
}
