package parquet

import (
	"errors"
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
}

func (d *int96PlainDecoder) init(data []byte) error {
	d.data = data
	return nil
}

func (d *int96PlainDecoder) decode(dst interface{}) error {
	return decodeInt96(d, dst)
}

func (d *int96PlainDecoder) decodeInt96(dst []Int96) error {
	for i := 0; i < len(dst); i++ {
		if len(d.data) == 0 {
			return errNED
		}
		if copy(dst[i][:12], d.data) != 12 {
			return errors.New("int96/plain: not enough bytes to decode an Int96 value")
		}
		d.data = d.data[12:]
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
