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

func (d *Decoder) Decode() {
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

			log.Println(value)
		}

	case parquetformat.Type_BYTE_ARRAY:
		var numArrays int32
		var size int32

		err := binary.Read(d.r, binary.LittleEndian, &numArrays)
		if err != nil {
			panic(err)
		}
		log.Println("num array", numArrays)

		for i := 0; i < int(numArrays); i++ {
			err := binary.Read(d.r, binary.LittleEndian, &size)
			if err != nil {
				panic(err)
			}
			p := make([]byte, size)
			n, err := d.r.Read(p)
			if err != nil {
				panic(err)
			}
			log.Println(string(p[:n]))
		}

	default:
		log.Println("unsupported plain format: ", d.t)
	}

}
