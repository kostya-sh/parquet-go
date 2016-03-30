package page

import (
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// DictionaryPage
type DictionaryPage struct {
	t               thrift.Type
	header          *thrift.DictionaryPageHeader
	valuesBool      []bool
	valuesInt32     []int32
	valuesInt64     []int64
	valuesByteArray [][]byte
	valuesFloat32   []float32
	valuesFloat64   []float64
	count           uint
}

// NewDictionaryPage
func NewDictionaryPage(t thrift.Type, header *thrift.DictionaryPageHeader) *DictionaryPage {
	count := uint(header.NumValues)
	switch t {
	case thrift.Type_BOOLEAN:
		return &DictionaryPage{
			t:          t,
			header:     header,
			valuesBool: make([]bool, count),
			count:      count,
		}
	case thrift.Type_INT32:
		return &DictionaryPage{
			t:           t,
			header:      header,
			valuesInt32: make([]int32, count),
			count:       count,
		}
	case thrift.Type_INT64:
		return &DictionaryPage{t: t, header: header, valuesInt64: make([]int64, count), count: count}
	case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return &DictionaryPage{t: t, header: header, valuesByteArray: make([][]byte, count), count: count}
	case thrift.Type_FLOAT:
		return &DictionaryPage{t: t, header: header, valuesFloat32: make([]float32, count), count: count}
	case thrift.Type_DOUBLE:
		return &DictionaryPage{t: t, header: header, valuesFloat64: make([]float64, count), count: count}
	case thrift.Type_INT96:
		return &DictionaryPage{
			t:           t,
			header:      header,
			valuesInt64: make([]int64, count),
			valuesInt32: make([]int32, count),
			count:       count,
		}
	default:
		panic("Warning: not supported type " + t.String() + " in plain encoding dictionaryPage")
	}

	return nil
}

func (p *DictionaryPage) NumValues() int32 {
	return int32(p.count)
}

//Decode Read a dictionary page. There is only one dictionary page for each column chunk
func (p *DictionaryPage) Decode(r io.Reader) error {

	// r = dump(r)

	count := p.count
	_type := p.t

	switch p.header.GetEncoding() {

	case thrift.Encoding_PLAIN_DICTIONARY:
		decoder := encoding.NewPlainDecoder(r, count)
		switch _type {
		case thrift.Type_BOOLEAN:
			read, err := decoder.DecodeBool(p.valuesBool)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_INT32:
			read, err := decoder.DecodeInt32(p.valuesInt32)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_INT64:
			read, err := decoder.DecodeInt64(p.valuesInt64)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
			read, err := decoder.DecodeByteArray(p.valuesByteArray)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_DOUBLE:
			read, err := decoder.DecodeFloat64(p.valuesFloat64)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_FLOAT:
			read, err := decoder.DecodeFloat32(p.valuesFloat32)
			if err != nil || read != count {
				return fmt.Errorf("could not read all dataPage encoded values")
			}
		case thrift.Type_INT96:
		default:
			return fmt.Errorf("dictionary type " + _type.String() + "not yet supported") // FIXME
		}
	default:
		return fmt.Errorf("dictionary encoding " + p.header.GetEncoding().String() + "not yet supported") // FIXME
	}

	return nil
}

func (p *DictionaryPage) MapBool(keys []uint64, out []bool) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesBool)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesBool))
		}
		out[i] = p.valuesBool[k]
	}

	return nil
}

func (p *DictionaryPage) MapInt32(keys []uint64, out []int32) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesInt32)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesInt32))
		}
		out[i] = p.valuesInt32[k]
	}

	return nil
}

func (p *DictionaryPage) MapInt64(keys []uint64, out []int64) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesInt64)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesInt64))
		}
		out[i] = p.valuesInt64[k]
	}

	return nil
}

func (p *DictionaryPage) MapFloat32(keys []uint64, out []float32) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesFloat32)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesFloat32))
		}
		out[i] = p.valuesFloat32[k]
	}

	return nil
}

func (p *DictionaryPage) MapFloat64(keys []uint64, out []float64) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesFloat64)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesFloat64))
		}
		out[i] = p.valuesFloat64[k]
	}

	return nil
}

func (p *DictionaryPage) MapByteArray(keys []uint64, out [][]byte) error {
	for i := 0; i < len(out); i++ {
		k := keys[i]
		if k >= uint64(len(p.valuesByteArray)) {
			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesByteArray))
		}
		out[i] = p.valuesByteArray[k]
	}

	return nil
}

// func (p *DictionaryPage) MapString(keys []uint64, out []string) error {
// 	for i := 0; i < len(out); i++ {
// 		k := keys[i]
// 		if k >= uint64(len(p.valuesString)) {
// 			return fmt.Errorf("key out of bounds %d max: %d", k, len(p.valuesString))
// 		}
// 		out[i] = p.valuesString[k]
// 	}

// 	return nil
// }
