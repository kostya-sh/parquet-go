package parquet

import (
	"errors"
	"fmt"
)

// TODO: store values as []interface{} when decoding to []interface{}

type dictDecoder struct {
	vd valuesDecoder

	numValues int
	data      []byte

	values interface{}
	ind    []int32

	keysDecoder *rleDecoder
}

func (d *dictDecoder) init(data []byte) error {
	if len(data) < 1 {
		return errors.New("dict: not enough data to read bit width")
	}
	d.data = data
	w := int(data[0])
	if w < 0 || w > 32 {
		return errors.New("dict: invalid bit width")
	}
	if w != 0 {
		d.keysDecoder = newRLEDecoder(w)
		d.keysDecoder.init(data[1:])
	} else if d.numValues != 0 {
		return errors.New("dict: bit-width = 0 for non-empty dictionary")
	}
	return nil
}

func (d *dictDecoder) initValues(values interface{}, dictData []byte) error {
	if d.numValues == 0 {
		return nil
	}
	if err := d.vd.init(dictData); err != nil {
		return err
	}
	if err := d.vd.decode(values); err != nil {
		return err
	}
	d.values = values
	return nil
}

func (d *dictDecoder) decodeKeys(n int) (keys []int32, err error) {
	if d.numValues == 0 {
		return nil, errors.New("dict: no values can be decoded from an empty dictionary")
	}
	if n > cap(d.ind) {
		d.ind = make([]int32, n, n) // TODO: uint32
	}
	for i := 0; i < n; i++ {
		k, err := d.keysDecoder.next()
		if err != nil {
			return nil, err
		}
		if k < 0 || int(k) >= d.numValues {
			return nil, fmt.Errorf("dict: invalid index %d, len(values) = %d", k, d.numValues)
		}
		d.ind[i] = k
	}
	return d.ind[:n], nil
}
