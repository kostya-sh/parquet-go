package parquet

import (
	"encoding/binary"
	"fmt"
)

type int64PlainDecoder struct {
	data []byte

	pos int
}

func (d *int64PlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int64PlainDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int64:
		return d.decodeInt64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int64PlainDecoder) decodeInt64(buf []int64) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+8 > len(d.data) {
			err = fmt.Errorf("int64/plain: not enough data")
		}
		buf[i] = int64(binary.LittleEndian.Uint64(d.data[d.pos:]))
		d.pos += 8
		i++
	}
	if i == 0 {
		err = fmt.Errorf("int64/plain: no more data")
	}
	return i, err
}

func (d *int64PlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int64, len(buf), len(buf))
	n, err = d.decodeInt64(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
