package parquet

import "fmt"

// PLAIN encoding for BOOLEAN type: bit-packed, LSB first

type booleanPlainDecoder struct {
	data      []byte
	numValues int

	value  bool
	err    error
	n      int
	values [8]int32
}

func (d *booleanPlainDecoder) init(data []byte, numValues int32) {
	d.data = data
	d.numValues = int(numValues)
	d.err = nil
	d.n = 0

	// TODO: test for this
	if d.numValues > 8*len(data) {
		d.err = fmt.Errorf("overflow: cannot store %d booleans in %d bytes", numValues, len(data))
	}
}

func (d *booleanPlainDecoder) next() bool {
	if d.err != nil {
		return false
	}
	if d.n >= d.numValues {
		return false
	}
	if d.n%8 == 0 {
		d.values = unpack8int32_1(d.data[d.n/8 : d.n/8+1])
	}
	d.value = false
	if d.values[d.n%8] == 1 {
		d.value = true
	}
	d.n++
	return true
}
