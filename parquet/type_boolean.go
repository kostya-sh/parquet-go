package parquet

import (
	"encoding/binary"
	"fmt"
)

type booleanPlainDecoder struct {
	count int
	data  []byte

	i      int
	values [8]int32
}

func (d *booleanPlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.count = count
	d.i = 0
	return nil
}

func (d *booleanPlainDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []bool:
		return d.decodeBool(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *booleanPlainDecoder) decodeBool(buf []bool) (n int, err error) {
	i := 0
	for i < len(buf) && d.i < d.count {
		if d.i%8 == 0 {
			d.values = unpack8int32_1(d.data[d.i/8 : d.i/8+1])
		}
		buf[i] = d.values[d.i%8] == 1
		d.i++
		i++
	}
	if i == 0 {
		err = fmt.Errorf("boolean/plain: no more data")
	}
	return i, err
}

func (d *booleanPlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]bool, len(buf), len(buf))
	n, err = d.decodeBool(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type booleanRLEDecoder struct {
	rle *rle32Decoder
}

func (d *booleanRLEDecoder) init(data []byte, count int) error {
	if len(data) <= 4 {
		return fmt.Errorf("boolean/rle: not enough data")
	}
	n := int(binary.LittleEndian.Uint32(data[:4])) // TODO: overflow?
	if n < 1 || n > len(data)-4 {
		return fmt.Errorf("boolean/rle: invalid data length")
	}
	d.rle = newRLE32Decoder(1)
	d.rle.init(data[4:n+4], count)
	return nil
}

func (d *booleanRLEDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []bool:
		return d.decodeBool(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *booleanRLEDecoder) decodeBool(buf []bool) (n int, err error) {
	n = len(buf)
	if d.rle.count-d.rle.i < n {
		n = d.rle.count - d.rle.i
	}
	if n == 0 {
		return 0, fmt.Errorf("boolean/rle: no more data")
	}
	for i := 0; i < n; i++ {
		b, err := d.rle.next()
		if err != nil {
			return i, err
		}
		d.rle.i++
		buf[i] = b == 1
	}
	return n, nil
}

func (d *booleanRLEDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]bool, len(buf), len(buf))
	n, err = d.decodeBool(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
