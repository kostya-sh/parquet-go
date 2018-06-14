package parquet

import (
	"fmt"
)

type Int96 [12]byte

type int96PlainDecoder struct {
	data []byte

	pos int
}

func (d *int96PlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int96PlainDecoder) next() (value Int96, err error) {
	if d.pos > len(d.data)-12 {
		return value, fmt.Errorf("int96/plain: not enough data")
	}
	copy(value[:12], d.data[d.pos:d.pos+12])
	d.pos += 12
	return value, err
}

func (d *int96PlainDecoder) decode(slice interface{}) (n int, err error) {
	// TODO: support string
	switch buf := slice.(type) {
	case []Int96:
		return d.decodeInt96(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int96PlainDecoder) decodeInt96(buf []Int96) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		buf[i], err = d.next()
		if err != nil {
			break
		}
		i++
	}
	if i == 0 {
		err = fmt.Errorf("bytearray/plain: no more data")
	}
	return i, err
}

func (d *int96PlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]Int96, len(buf), len(buf))
	n, err = d.decodeInt96(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type int96DictDecoder struct {
	dictDecoder

	values []Int96
}

func (d *int96DictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]Int96, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *int96DictDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []Int96:
		return d.decodeInt96(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int96DictDecoder) decodeInt96(buf []Int96) (n int, err error) {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return 0, err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return len(keys), nil
}

func (d *int96DictDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]Int96, len(buf), len(buf))
	n, err = d.decodeInt96(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
