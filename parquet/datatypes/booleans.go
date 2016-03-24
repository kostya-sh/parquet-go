package datatypes

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
)

// PLAIN encoding for BOOLEAN type: bit-packed, LSB first
type booleanPlainDecoder struct {
	data []byte

	pos    int
	values [8]int32
}

func newBooleanPlainDecoder() *booleanPlainDecoder {
	return &booleanPlainDecoder{}
}

func (d *booleanPlainDecoder) init(data []byte) {
	d.data = data
	d.pos = 0
}

func (d *booleanPlainDecoder) next() (value bool, err error) {
	if d.pos >= len(d.data)*8 { // TODO: this can overflow, re-implement
		return false, fmt.Errorf("boolean/plain: no more data")
	}
	if d.pos%8 == 0 {
		d.values = encoding.Unpack8Int32FuncForWidth(1)(d.data[d.pos/8 : d.pos/8+1])
	}
	value = false
	if d.values[d.pos%8] == 1 {
		value = true
	}
	d.pos++
	return
}
