package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type booleanDecoder interface {
	decodeBool(dst []bool) error
}

func decodeBoolean(d booleanDecoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []bool:
		return d.decodeBool(dst)
	case []interface{}:
		b := make([]bool, len(dst))
		err := d.decodeBool(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type booleanPlainDecoder struct {
	data []byte

	i      uint8
	values [8]int32
}

func (d *booleanPlainDecoder) init(data []byte) error {
	d.data = data
	d.i = 0
	return nil
}

func (d *booleanPlainDecoder) decode(dst interface{}) error {
	return decodeBoolean(d, dst)
}

func (d *booleanPlainDecoder) decodeBool(dst []bool) error {
	for i := 0; i < len(dst); i++ {
		if d.i == 0 {
			if len(d.data) == 0 {
				return errNED
			}
			d.values = unpack8int32_1(d.data[:1])
			d.data = d.data[1:]
		}
		dst[i] = d.values[d.i] == 1
		d.i = (d.i + 1) % 8
	}
	return nil
}

type booleanRLEDecoder struct {
	rle *rleDecoder
}

func (d *booleanRLEDecoder) init(data []byte) error {
	if len(data) < 4 {
		return errors.New("boolean/rle: not enough data to read data length")
	}
	n := uint(binary.LittleEndian.Uint32(data[:4]))
	if n < 1 || n > uint(len(data)-4) {
		return fmt.Errorf("boolean/rle: invalid data length")
	}
	d.rle = newRLEDecoder(1)
	d.rle.init(data[4 : 4+n])
	return nil
}

func (d *booleanRLEDecoder) decode(dst interface{}) error {
	return decodeBoolean(d, dst)
}

func (d *booleanRLEDecoder) decodeBool(dst []bool) error {
	for i := 0; i < len(dst); i++ {
		v, err := d.rle.next()
		if err != nil {
			return err
		}
		dst[i] = v == 1
	}
	return nil
}
