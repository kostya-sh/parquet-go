package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
)

type floatPlainDecoder struct {
	data []byte

	pos int
}

func (d *floatPlainDecoder) init(data []byte) {
	d.data = data
	d.pos = 0
}

func (d *floatPlainDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []float32:
		return d.decodeFloat32(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *floatPlainDecoder) decodeFloat32(buf []float32) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+4 > len(d.data) {
			err = fmt.Errorf("float/plain: not enough data")
		}
		buf[i] = math.Float32frombits(binary.LittleEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		i++
	}
	if i == 0 {
		err = fmt.Errorf("float/plain: no more data")
	}
	return i, err
}

func (d *floatPlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]float32, len(buf), len(buf))
	n, err = d.decodeFloat32(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
