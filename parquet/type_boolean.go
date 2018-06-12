package parquet

import "fmt"

type booleanPlainDecoder struct {
	count int
	data  []byte

	i      int
	values [8]int32
}

func (d *booleanPlainDecoder) init(data []byte, count int) error {
	if count > len(data)*8 { // TODO: think overflow (*8)
		return fmt.Errorf("not enough data (%d bytes) for %d values", len(data), count)
	}
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
