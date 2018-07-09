package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
)

type doublePlainDecoder struct {
	data []byte

	pos int
}

func (d *doublePlainDecoder) init(data []byte) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *doublePlainDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []float64:
		return d.decodeFloat64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *doublePlainDecoder) decodeFloat64(buf []float64) error {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+8 > len(d.data) {
			return fmt.Errorf("double/plain: not enough data")
		}
		buf[i] = math.Float64frombits(binary.LittleEndian.Uint64(d.data[d.pos:]))
		d.pos += 8
		i++
	}
	if i == 0 {
		return fmt.Errorf("double/plain: no more data")
	}
	return nil
}

func (d *doublePlainDecoder) decodeE(buf []interface{}) error {
	b := make([]float64, len(buf), len(buf))
	err := d.decodeFloat64(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}

type doubleDictDecoder struct {
	dictDecoder

	values []float64
}

func (d *doubleDictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]float64, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *doubleDictDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []float64:
		return d.decodeFloat64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *doubleDictDecoder) decodeFloat64(buf []float64) error {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return nil
}

func (d *doubleDictDecoder) decodeE(buf []interface{}) error {
	b := make([]float64, len(buf), len(buf))
	err := d.decodeFloat64(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}
