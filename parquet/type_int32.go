package parquet

import (
	"encoding/binary"
	"fmt"
)

type int32PlainDecoder struct {
	data []byte

	pos int
}

func (d *int32PlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int32PlainDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int32:
		return d.decodeInt32(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int32PlainDecoder) decodeInt32(buf []int32) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+4 > len(d.data) {
			err = fmt.Errorf("int32/plain: not enough data")
		}
		buf[i] = int32(binary.LittleEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		i++
	}
	if i == 0 {
		err = fmt.Errorf("int32/plain: no more data")
	}
	return i, err
}

func (d *int32PlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int32, len(buf), len(buf))
	n, err = d.decodeInt32(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type int32DictDecoder struct {
	dictDecoder

	values []int32
}

func (d *int32DictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]int32, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *int32DictDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int32:
		return d.decodeInt32(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int32DictDecoder) decodeInt32(buf []int32) (n int, err error) {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return 0, err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return len(keys), nil
}

func (d *int32DictDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int32, len(buf), len(buf))
	n, err = d.decodeInt32(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
