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

func (d *floatPlainDecoder) init(data []byte) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *floatPlainDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []float32:
		return d.decodeFloat32(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *floatPlainDecoder) decodeFloat32(buf []float32) error {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+4 > len(d.data) {
			return fmt.Errorf("float/plain: not enough data")
		}
		buf[i] = math.Float32frombits(binary.LittleEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		i++
	}
	if i == 0 {
		return fmt.Errorf("float/plain: no more data")
	}
	return nil
}

func (d *floatPlainDecoder) decodeE(buf []interface{}) error {
	b := make([]float32, len(buf), len(buf))
	err := d.decodeFloat32(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}

type floatDictDecoder struct {
	dictDecoder

	values []float32
}

func (d *floatDictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]float32, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *floatDictDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []float32:
		return d.decodeFloat32(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *floatDictDecoder) decodeFloat32(buf []float32) error {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return nil
}

func (d *floatDictDecoder) decodeE(buf []interface{}) error {
	b := make([]float32, len(buf), len(buf))
	err := d.decodeFloat32(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}
