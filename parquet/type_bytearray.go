package parquet

import (
	"encoding/binary"
	"fmt"

	"github.com/kostya-sh/parquet-go/parquetformat"
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
	values [][]byte

	data []byte

	rleDecoder *rle32Decoder
}

func (d *byteArrayDictDecoder) init(data []byte, count int) error {
	if len(data) < 3 {
		return fmt.Errorf("not enough data")
	}
	d.data = data
	bw := int(data[0])
	if bw <= 0 || bw > 32 {
		return fmt.Errorf("invalid bit width: %d", bw)
	}
	d.rleDecoder = newRLE32Decoder(bw)
	d.rleDecoder.init(data[1:], count)
	return nil
}

func (d *byteArrayDictDecoder) initValues(dictPageHeader *parquetformat.DictionaryPageHeader, dictData []byte) error {
	switch dictPageHeader.Encoding {
	case parquetformat.Encoding_PLAIN, parquetformat.Encoding_PLAIN_DICTIONARY:
		vd := &byteArrayPlainDecoder{}
		count := int(dictPageHeader.NumValues)
		if err := vd.init(dictData, count); err != nil {
			return err
		}
		d.values = make([][]byte, count, count)
		n, err := vd.decodeByteSlice(d.values)
		if err != nil {
			return err
		}
		if n != count {
			return fmt.Errorf("read %d values from dictionary page, expected %d", n, count)
		}

		return nil
	default:
		return fmt.Errorf("unsupported encoding for dictionary page: %s", dictPageHeader.Encoding)
	}
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
	n = len(buf)
	if rem := d.rleDecoder.count - d.rleDecoder.i; rem < n {
		n = rem
	}
	for i := 0; i < n; i++ {
		k, err := d.rleDecoder.next()

		if err != nil {
			return i, err
		}
		if k < 0 || int(k) >= len(d.values) {
			return i, fmt.Errorf("read %d, len(values) = %d", k, len(d.values))
		}
		d.rleDecoder.i++
		buf[i] = d.values[k]
	}
	return n, nil
}

func (d *byteArrayDictDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([][]byte, len(buf), len(buf))
	n, err = d.decodeByteSlice(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
