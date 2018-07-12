package parquet

import (
	"encoding/binary"
	"fmt"
)

type byteArrayDecoder interface {
	decodeByteSlice(dst [][]byte) error
}

func decodeByteArray(d byteArrayDecoder, dst interface{}) error {
	switch dst := dst.(type) {
	case [][]byte:
		return d.decodeByteSlice(dst)
	case []interface{}:
		b := make([][]byte, len(dst), len(dst))
		err := d.decodeByteSlice(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type byteArrayPlainDecoder struct {
	// length > 0 for FIXED_BYTE_ARRAY type
	length int

	data []byte

	pos int
}

func (d *byteArrayPlainDecoder) init(data []byte) error {
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

func (d *byteArrayPlainDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayPlainDecoder) decodeByteSlice(dst [][]byte) error {
	i := 0
	for i < len(dst) && d.pos < len(d.data) {
		v, err := d.next()
		if err != nil {
			break
		}
		dst[i] = v
		i++
	}
	if i == 0 {
		return fmt.Errorf("bytearray/plain: no more data")
	}
	return nil
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

func (d *byteArrayDictDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayDictDecoder) decodeByteSlice(dst [][]byte) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}

type byteArrayDeltaLengthDecoder struct {
	data []byte
	lens []int32

	i   int
	pos int
}

func (d *byteArrayDeltaLengthDecoder) init(data []byte) error {
	d.data = data

	lensDecoder := int32DeltaBinaryPackedDecoder{}
	if err := lensDecoder.init(data); err != nil {
		return err
	}

	d.lens = make([]int32, lensDecoder.numValues, lensDecoder.numValues)
	err := lensDecoder.decodeInt32(d.lens)
	if err != nil {
		return err
	}
	d.pos = lensDecoder.pos
	d.i = 0
	return nil
}

func (d *byteArrayDeltaLengthDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
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

func (d *byteArrayDeltaLengthDecoder) decodeByteSlice(dst [][]byte) error {
	i := 0
	for i < len(dst) && d.i < len(d.lens) {
		v, err := d.next()
		if err != nil {
			break
		}
		dst[i] = v
		i++
	}
	if i == 0 {
		return fmt.Errorf("bytearray/plain: no more data")
	}
	return nil
}

type byteArrayDeltaDecoder struct {
	suffixDecoder byteArrayDeltaLengthDecoder

	prefixLens []int32

	value []byte
}

func (d *byteArrayDeltaDecoder) init(data []byte) error {
	lensDecoder := int32DeltaBinaryPackedDecoder{}
	if err := lensDecoder.init(data); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.numValues, lensDecoder.numValues)
	if err := lensDecoder.decodeInt32(d.prefixLens); err != nil {
		return err
	}
	if err := d.suffixDecoder.init(data[lensDecoder.pos:]); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return fmt.Errorf("bytearray/delta: different number of suffixes and prefixes")
	}

	d.value = make([]byte, 0)

	return nil
}

func (d *byteArrayDeltaDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayDeltaDecoder) decodeByteSlice(dst [][]byte) error {
	i := 0
	for i < len(dst) && d.suffixDecoder.i < len(d.prefixLens) {
		prefixLen := int(d.prefixLens[d.suffixDecoder.i])
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			break
		}
		value := make([]byte, 0, prefixLen+len(suffix))
		value = append(value, d.value[:prefixLen]...)
		value = append(value, suffix...)
		d.value = value
		dst[i] = value
		i++
	}
	if i == 0 {
		return fmt.Errorf("bytearray/delta: no more data")
	}
	return nil
}
