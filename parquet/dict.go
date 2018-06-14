package parquet

import (
	"fmt"
)

type dictDecoder struct {
	vd valuesDecoder

	numValues int
	data      []byte

	values interface{}
	ind    []int32

	keysDecoder *rle32Decoder
}

func (d *dictDecoder) init(data []byte, count int) error {
	if len(data) < 3 {
		return fmt.Errorf("dict: not enough data")
	}
	d.data = data
	w := int(data[0])
	if w <= 0 || w > 32 {
		return fmt.Errorf("invalid bit width: %d", w)
	}
	d.keysDecoder = newRLE32Decoder(w)
	d.keysDecoder.init(data[1:], count)
	return nil
}

func (d *dictDecoder) initValues(values interface{}, dictData []byte) error {
	if err := d.vd.init(dictData, d.numValues); err != nil {
		return err
	}
	d.values = values
	n, err := d.vd.decode(d.values)
	if err != nil {
		return err
	}
	if n != d.numValues {
		return fmt.Errorf("read %d values from dictionary page, expected %d", n, d.numValues)
	}
	return nil
}

func (d *dictDecoder) decodeKeys(n int) (keys []int32, err error) {
	if rem := d.keysDecoder.count - d.keysDecoder.i; rem < n {
		n = rem
	}
	if n > cap(d.ind) {
		d.ind = make([]int32, n, n)
	}
	for i := 0; i < n; i++ {
		k, err := d.keysDecoder.next()

		if err != nil {
			return nil, err
		}
		if k < 0 || int(k) >= d.numValues {
			return nil, fmt.Errorf("read %d, len(values) = %d", k, d.numValues)
		}
		d.keysDecoder.i++
		d.ind[i] = k
	}
	return d.ind[:n], nil
}
