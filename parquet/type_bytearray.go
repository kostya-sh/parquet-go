package parquet

import (
	"encoding/binary"
	"errors"
)

type byteArrayDecoder interface {
	decodeByteSlice(dst [][]byte) error
}

func decodeByteArray(d byteArrayDecoder, dst interface{}) error {
	switch dst := dst.(type) {
	case [][]byte:
		return d.decodeByteSlice(dst)
	case []interface{}:
		b := make([][]byte, len(dst))
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
}

func (d *byteArrayPlainDecoder) init(data []byte) error {
	d.data = data
	return nil
}

func (d *byteArrayPlainDecoder) next() (value []byte, err error) {
	if len(d.data) == 0 {
		return nil, errNED
	}
	size := d.length
	if d.length == 0 {
		if len(d.data) < 4 {
			return nil, errors.New("bytearray/plain: not enough data to read length")
		}
		size = int(int32(binary.LittleEndian.Uint32(d.data)))
		if size < 0 {
			return nil, errors.New("bytearray/plain: negative length")
		}
		d.data = d.data[4:]
	}
	if len(d.data) < size {
		return nil, errors.New("bytearray/plain: not enough data to read value")
	}
	// TODO: configure copy or not
	value = make([]byte, size)
	copy(value, d.data)
	d.data = d.data[size:]
	return value, err
}

func (d *byteArrayPlainDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayPlainDecoder) decodeByteSlice(dst [][]byte) error {
	for i := 0; i < len(dst); i++ {
		v, err := d.next()
		if err != nil {
			return err
		}
		dst[i] = v
	}
	return nil
}

type byteArrayDictDecoder struct {
	dictDecoder

	values [][]byte
}

func (d *byteArrayDictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([][]byte, count)
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

	i int
}

func (d *byteArrayDeltaLengthDecoder) init(data []byte) error {
	lensDecoder := int32DeltaBinaryPackedDecoder{}
	if err := lensDecoder.init(data); err != nil {
		return err
	}

	d.lens = make([]int32, lensDecoder.numValues)
	err := lensDecoder.decodeInt32(d.lens)
	if err != nil {
		return err
	}

	d.data = lensDecoder.data
	d.i = 0
	return nil
}

func (d *byteArrayDeltaLengthDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayDeltaLengthDecoder) next() (value []byte, err error) {
	if d.i >= len(d.lens) {
		return nil, errNED
	}
	size := int(d.lens[d.i])
	if len(d.data) < size {
		return nil, errors.New("bytearray/deltalength: not enough data to read value")
	}
	// TODO: configure copy or not
	value = make([]byte, size)
	copy(value, d.data)
	d.data = d.data[size:]
	d.i++
	return value, err
}

func (d *byteArrayDeltaLengthDecoder) decodeByteSlice(dst [][]byte) error {
	for i := 0; i < len(dst); i++ {
		v, err := d.next()
		if err != nil {
			return err
		}
		dst[i] = v
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

	d.prefixLens = make([]int32, lensDecoder.numValues)
	if err := lensDecoder.decodeInt32(d.prefixLens); err != nil {
		return err
	}
	if err := d.suffixDecoder.init(lensDecoder.data); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return errors.New("bytearray/delta: different number of suffixes and prefixes")
	}

	d.value = make([]byte, 0)

	return nil
}

func (d *byteArrayDeltaDecoder) decode(dst interface{}) error {
	return decodeByteArray(d, dst)
}

func (d *byteArrayDeltaDecoder) decodeByteSlice(dst [][]byte) error {
	for i := 0; i < len(dst); i++ {
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			return err
		}
		prefixLen := int(d.prefixLens[d.suffixDecoder.i-1])
		value := make([]byte, 0, prefixLen+len(suffix))
		value = append(value, d.value[:prefixLen]...)
		value = append(value, suffix...)
		d.value = value
		dst[i] = value
	}
	return nil
}
