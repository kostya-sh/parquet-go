package parquet

import (
	"errors"
	"fmt"
)

type Int96 [12]byte

type int96Decoder interface {
	decodeInt96(dst []Int96) error
}

func decodeInt96(d int96Decoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []Int96:
		return d.decodeInt96(dst)
	case []interface{}:
		b := make([]Int96, len(dst), len(dst))
		err := d.decodeInt96(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type int96PlainDecoder struct {
	data []byte

	pos int
}

func (d *int96PlainDecoder) init(data []byte) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int96PlainDecoder) decode(dst interface{}) error {
	return decodeInt96(d, dst)
}

func (d *int96PlainDecoder) next() (value Int96, err error) {
	if d.pos > len(d.data)-12 {
		return value, fmt.Errorf("int96/plain: not enough data")
	}
	copy(value[:12], d.data[d.pos:d.pos+12])
	d.pos += 12
	return value, err
}

func (d *int96PlainDecoder) decodeInt96(dst []Int96) error {
	for i := 0; i < len(dst); i++ {
		if d.pos >= len(d.data) {
			return errNED
		}
		if copy(dst[i][:12], d.data[d.pos:]) != 12 {
			return errors.New("int96/plain: not enough bytes to decode an Int96 value")
		}
		d.pos += 12
	}
	return nil
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

func (d *int96DictDecoder) decode(dst interface{}) error {
	return decodeInt96(d, dst)
}

func (d *int96DictDecoder) decodeInt96(dst []Int96) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}
