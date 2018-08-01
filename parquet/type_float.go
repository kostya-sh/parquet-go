package parquet

import (
	"encoding/binary"
	"errors"
	"math"
)

type floatDecoder interface {
	decodeFloat32(dst []float32) error
}

func decodeFloat(d floatDecoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []float32:
		return d.decodeFloat32(dst)
	case []interface{}:
		b := make([]float32, len(dst))
		err := d.decodeFloat32(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type floatPlainDecoder struct {
	data []byte
}

func (d *floatPlainDecoder) init(data []byte) error {
	d.data = data
	return nil
}

func (d *floatPlainDecoder) decode(dst interface{}) error {
	return decodeFloat(d, dst)
}

func (d *floatPlainDecoder) decodeFloat32(dst []float32) error {
	for i := 0; i < len(dst); i++ {
		if len(d.data) == 0 {
			return errNED
		}
		if len(d.data) < 4 {
			return errors.New("float/plain: not enough bytes to decode a float number")
		}
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(d.data))
		d.data = d.data[4:]
	}
	return nil
}

type floatDictDecoder struct {
	dictDecoder

	values []float32
}

func (d *floatDictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]float32, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *floatDictDecoder) decode(dst interface{}) error {
	return decodeFloat(d, dst)
}

func (d *floatDictDecoder) decodeFloat32(dst []float32) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}
