package page

import (
	"log"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// DictionaryPage
type DictionaryPage struct {
	valuesBool   []bool
	valuesInt32  []int32
	valuesInt64  []int64
	valuesString []string
	count        int
}

// NewDictionaryPage
func NewDictionaryPage(t thrift.Type, count int) *DictionaryPage {
	switch t {
	case thrift.Type_INT32:
		return &DictionaryPage{valuesInt32: make([]int32, 0, count), count: count}
	case thrift.Type_INT64:
		return &DictionaryPage{valuesInt64: make([]int64, 0, count), count: count}
	case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return &DictionaryPage{valuesInt64: make([]string, 0, count), count: count}
	default:
		log.Println("Warning: skipping not supported type ", t, " in plain encoding dictionaryPage")
		return nil
	}

	return nil
}

func (dict *DictionaryPage) NumValues() int32 {
	return dict.count
}
