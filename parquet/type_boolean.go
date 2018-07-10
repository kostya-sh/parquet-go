package parquet

import (
	"encoding/binary"
	"fmt"
)

type booleanPlainDecoder struct {
	data []byte

	i      int
	values [8]int32
}

func (d *booleanPlainDecoder) init(data []byte) error {
	d.data = data
	d.i = 0
	return nil
}

func (d *booleanPlainDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []bool:
		return d.decodeBool(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *booleanPlainDecoder) decodeBool(buf []bool) error {
	i := 0
	for i < len(buf) && d.i/8 < len(d.data) {
		if d.i%8 == 0 {
			d.values = unpack8int32_1(d.data[d.i/8 : d.i/8+1])
		}
		buf[i] = d.values[d.i%8] == 1
		d.i++
		i++
	}
	if i != len(buf) {
		return fmt.Errorf("boolean/plain: no more data")
	}
	return nil
}

func (d *booleanPlainDecoder) decodeE(buf []interface{}) error {
	b := make([]bool, len(buf), len(buf))
	err := d.decodeBool(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}

type booleanRLEDecoder struct {
	rle *rleDecoder
}

func (d *booleanRLEDecoder) init(data []byte) error {
	if len(data) <= 4 {
		return fmt.Errorf("boolean/rle: not enough data")
	}
	n := int(binary.LittleEndian.Uint32(data[:4])) // TODO: overflow?
	if n < 1 || n > len(data)-4 {
		return fmt.Errorf("boolean/rle: invalid data length")
	}
	d.rle = newRLEDecoder(1)
	d.rle.init(data[4 : n+4]) // TODO: overflow?
	return nil
}

func (d *booleanRLEDecoder) decode(slice interface{}) error {
	switch buf := slice.(type) {
	case []bool:
		return d.decodeBool(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *booleanRLEDecoder) decodeBool(buf []bool) error {
	for i := 0; i < len(buf); i++ {
		b, err := d.rle.next()
		if err != nil {
			return err
		}
		buf[i] = b == 1
	}
	return nil
}

func (d *booleanRLEDecoder) decodeE(buf []interface{}) error {
	b := make([]bool, len(buf), len(buf))
	err := d.decodeBool(b)
	for i := 0; i < len(buf); i++ {
		buf[i] = b[i]
	}
	return err
}
