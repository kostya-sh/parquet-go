package parquet

import (
	"encoding/binary"
	"fmt"
)

type byteArrayPlainDecoder struct {
	// length > 0 for FIXED_BYTE_ARRAY type
	length int

	data []byte

	pos int
}

func (d *byteArrayPlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *byteArrayPlainDecoder) next() (value []byte, err error) {
	size := d.length
	if d.length == 0 {
		if d.pos > len(d.data)-4 {
			return nil, fmt.Errorf("bytearray/plain: no more data")
		}
		size = int(binary.LittleEndian.Uint32(d.data[d.pos:])) // TODO: think about int overflow here
		d.pos += 4
	}
	if d.pos > len(d.data)-size {
		return nil, fmt.Errorf("bytearray/plain: not enough data")
	}
	// TODO: configure copy or not
	value = make([]byte, size)
	copy(value, d.data[d.pos:d.pos+size])
	d.pos += size
	return value, err
}

func (d *byteArrayPlainDecoder) decode(slice interface{}) (n int, err error) {
	// TODO: support string
	switch buf := slice.(type) {
	case [][]byte:
		return d.decodeByteSlice(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *byteArrayPlainDecoder) decodeByteSlice(buf [][]byte) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		buf[i], err = d.next()
		if err != nil {
			break
		}
		i++
	}
	if i == 0 {
		err = fmt.Errorf("bytearray/plain: no more data")
	}
	return i, err
}

func (d *byteArrayPlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([][]byte, len(buf), len(buf))
	n, err = d.decodeByteSlice(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type byteArrayDictDecoder struct {
	dictDecoder

	values [][]byte
}

func (d *byteArrayDictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([][]byte, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *byteArrayDictDecoder) decode(slice interface{}) (n int, err error) {
	// TODO: support string
	switch buf := slice.(type) {
	case [][]byte:
		return d.decodeByteSlice(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *byteArrayDictDecoder) decodeByteSlice(buf [][]byte) (n int, err error) {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return 0, err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return len(keys), nil
}

func (d *byteArrayDictDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([][]byte, len(buf), len(buf))
	n, err = d.decodeByteSlice(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type byteArrayDeltaLengthDecoder struct {
	data []byte
	lens []int32

	i   int
	pos int
}

func (d *byteArrayDeltaLengthDecoder) init(data []byte, count int) error {
	d.data = data

	lensDecoder := int32DeltaBinaryPackedDecoder{}
	if err := lensDecoder.init(data, count); err != nil {
		return err
	}

	d.lens = make([]int32, lensDecoder.numValues, lensDecoder.numValues)
	n, err := lensDecoder.decodeInt32(d.lens)
	if err != nil {
		return err
	}
	if n != len(d.lens) {
		return fmt.Errorf("bytearray/deltalength: faield to read all lengtgs")
	}
	d.pos = lensDecoder.pos
	d.i = 0
	return nil
}

func (d *byteArrayDeltaLengthDecoder) next() (value []byte, err error) {
	size := int(d.lens[d.i])
	if d.pos > len(d.data)-size {
		return nil, fmt.Errorf("bytearray/deltalength: not enough data")
	}
	// TODO: configure copy or not
	value = make([]byte, size)
	copy(value, d.data[d.pos:d.pos+size])
	d.pos += size
	d.i++
	return value, err
}

func (d *byteArrayDeltaLengthDecoder) decode(slice interface{}) (n int, err error) {
	// TODO: support string
	switch buf := slice.(type) {
	case [][]byte:
		return d.decodeByteSlice(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *byteArrayDeltaLengthDecoder) decodeByteSlice(buf [][]byte) (n int, err error) {
	i := 0
	for i < len(buf) && d.i < len(d.lens) {
		buf[i], err = d.next()
		if err != nil {
			break
		}
		i++
	}
	if i == 0 {
		err = fmt.Errorf("bytearray/plain: no more data")
	}
	return i, err
}

func (d *byteArrayDeltaLengthDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([][]byte, len(buf), len(buf))
	n, err = d.decodeByteSlice(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type byteArrayDeltaDecoder struct {
	suffixDecoder byteArrayDeltaLengthDecoder

	prefixLens []int32

	value []byte
}

func (d *byteArrayDeltaDecoder) init(data []byte, count int) error {
	lensDecoder := int32DeltaBinaryPackedDecoder{}
	if err := lensDecoder.init(data, count); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.numValues, lensDecoder.numValues)
	n, err := lensDecoder.decodeInt32(d.prefixLens)
	if err != nil {
		return err
	}
	if n != len(d.prefixLens) {
		return fmt.Errorf("bytearray/delta: faield to read all prefix lengths")
	}

	if err := d.suffixDecoder.init(data[lensDecoder.pos:], count); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return fmt.Errorf("bytearray/delta: different number of suffixes and prefixes")
	}

	d.value = make([]byte, 0)

	return nil
}

func (d *byteArrayDeltaDecoder) decode(slice interface{}) (n int, err error) {
	// TODO: support string
	switch buf := slice.(type) {
	case [][]byte:
		return d.decodeByteSlice(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *byteArrayDeltaDecoder) decodeByteSlice(buf [][]byte) (n int, err error) {
	i := 0
	for i < len(buf) && d.suffixDecoder.i < len(d.prefixLens) {
		prefixLen := int(d.prefixLens[d.suffixDecoder.i])
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			break
		}
		value := make([]byte, 0, prefixLen+len(suffix))
		value = append(value, d.value[:prefixLen]...)
		value = append(value, suffix...)
		d.value = value
		buf[i] = value
		i++
	}
	if i == 0 {
		err = fmt.Errorf("bytearray/delta: no more data")
	}
	return i, err
}

func (d *byteArrayDeltaDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([][]byte, len(buf), len(buf))
	n, err = d.decodeByteSlice(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
