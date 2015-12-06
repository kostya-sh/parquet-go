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

// DecodeInt
func (d *Decoder) DecodeInt(out []int) (int, error) {
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

			log.Println("plain:int32:", value)
			out = append(out, int(value))
		}
	default:
		log.Println("unsupported string format: ", d.t, " for type int")
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
