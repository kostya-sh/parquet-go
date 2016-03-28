package page

import (
	"fmt"
	"io"
	"log"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// DictionaryPage
type DictionaryPage struct {
	t            thrift.Type
	header       *thrift.DictionaryPageHeader
	valuesBool   []bool
	valuesInt32  []int32
	valuesInt64  []int64
	valuesString []string
	count        int
}

// NewDictionaryPage
func NewDictionaryPage(t thrift.Type, header *thrift.DictionaryPageHeader) *DictionaryPage {
	count := int(header.NumValues)
	switch t {
	case thrift.Type_INT32:
		return &DictionaryPage{
			t:           t,
			header:      header,
			valuesInt32: make([]int32, 0, count),
			count:       count,
		}
	case thrift.Type_INT64:
		return &DictionaryPage{t: t, header: header, valuesInt64: make([]int64, 0, count), count: count}
	case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return &DictionaryPage{t: t, header: header, valuesString: make([]string, 0, count), count: count}
	default:
		log.Println("Warning: skipping not supported type ", t, " in plain encoding dictionaryPage")
		return nil
	}

	return nil
}

func (p *DictionaryPage) NumValues() int32 {
	return int32(p.count)
}

//Decode Read a dictionary page. There is only one dictionary page for each column chunk
func (p *DictionaryPage) Decode(r io.Reader) error {

	count := p.count
	_type := p.t

	switch p.header.GetEncoding() {

	case thrift.Encoding_PLAIN_DICTIONARY:
		decoder := encoding.NewPlainDecoder(r, _type, count)
		switch _type {
		case thrift.Type_INT32:
			read, err := decoder.DecodeInt32(p.valuesInt32)
			if err != nil || read != count {
				return fmt.Errorf("could not read all values")
			}
		case thrift.Type_INT64:
			read, err := decoder.DecodeInt64(p.valuesInt64)
			if err != nil || read != count {
				return fmt.Errorf("could not read all values")
			}
		case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
			read, err := decoder.DecodeStr(p.valuesString)
			if err != nil || read != count {
				return fmt.Errorf("could not read all values")
			}
		case thrift.Type_DOUBLE:
		case thrift.Type_FLOAT:
		case thrift.Type_INT96:
		default:
			return fmt.Errorf("dictionary type " + _type.String() + "not yet supported") // FIXME
		}
	default:
		return fmt.Errorf("dictionary encoding " + p.header.GetEncoding().String() + "not yet supported") // FIXME
	}

	return nil
}
