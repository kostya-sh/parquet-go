package parquet

import (
	"encoding/binary"
	"errors"
	"math"
)

type doubleDecoder interface {
	decodeFloat64(dst []float64) error
}

func decodeDouble(d doubleDecoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []float64:
		return d.decodeFloat64(dst)
	case []interface{}:
		b := make([]float64, len(dst), len(dst))
		err := d.decodeFloat64(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type doublePlainDecoder struct {
	data []byte

	pos int
}

func (d *doublePlainDecoder) init(data []byte) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *doublePlainDecoder) decode(dst interface{}) error {
	return decodeDouble(d, dst)
}

func (d *doublePlainDecoder) decodeFloat64(dst []float64) error {
	for i := 0; i < len(dst); i++ {
		if d.pos >= len(d.data) {
			return errNED
		}
		if uint(d.pos+8) > uint(len(d.data)) {
			return errors.New("double/plain: not enough bytes to decode a double number")
		}
		dst[i] = math.Float64frombits(binary.LittleEndian.Uint64(d.data[d.pos:]))
		d.pos += 8
	}
	return nil
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

func (d *doubleDictDecoder) decode(dst interface{}) error {
	return decodeDouble(d, dst)
}

func (d *doubleDictDecoder) decodeFloat64(dst []float64) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}
