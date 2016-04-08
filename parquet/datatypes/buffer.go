package datatypes

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// BufferWriter generic type
type BufferWriter interface {
	WriteBuffer(name string, b *Buffer) error
}

// All wraps to all the types supported by parquet
type Buffer struct {
	t               thrift.Type
	valuesBool      []bool
	valuesInt32     []int32
	valuesInt64     []int64
	valuesInt96     []Int96
	valuesByteArray [][]byte
	valuesFloat32   []float32
	valuesFloat64   []float64
	typeLength      uint
}

func NewBuffer(values interface{}) *Buffer {
	switch v := values.(type) {
	case []bool:
		return &Buffer{t: thrift.Type_BOOLEAN, valuesBool: v}
	case []int32:
		return &Buffer{t: thrift.Type_INT32, valuesInt32: v}
	case []int64:
		return &Buffer{t: thrift.Type_INT64, valuesInt64: v}
	case [][]byte:
		return &Buffer{t: thrift.Type_BYTE_ARRAY, valuesByteArray: v}
	// case thrift.Type_FIXED_LEN_BYTE_ARRAY:
	// 	return &Buffer{valuesByteArray: values.([][]byte), typeLength: uint(e.GetTypeLength())}
	case []float32:
		return &Buffer{t: thrift.Type_FLOAT, valuesFloat32: v}
	case []float64:
		return &Buffer{t: thrift.Type_DOUBLE, valuesFloat64: v}
	case []Int96:
		return &Buffer{t: thrift.Type_INT96, valuesInt96: v}
	default:
		panic(fmt.Sprintf("Warning: not supported type %#v in plain encoding dictionaryPage", values))
	}
}

func NewBufferWithType(e *thrift.SchemaElement, size int) *Buffer {
	t := e.GetType()

	switch t {
	case thrift.Type_BOOLEAN:
		return &Buffer{t: thrift.Type_BOOLEAN, valuesBool: make([]bool, 0, size)}
	case thrift.Type_INT32:
		return &Buffer{t: thrift.Type_INT32, valuesInt32: make([]int32, 0, size)}
	case thrift.Type_INT64:
		return &Buffer{t: thrift.Type_INT64, valuesInt64: make([]int64, 0, size)}
	case thrift.Type_BYTE_ARRAY:
		return &Buffer{t: thrift.Type_BYTE_ARRAY, valuesByteArray: make([][]byte, 0, size)}
	case thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return &Buffer{t: thrift.Type_BYTE_ARRAY, valuesByteArray: make([][]byte, 0, size), typeLength: uint(e.GetTypeLength())}
	case thrift.Type_FLOAT:
		return &Buffer{t: thrift.Type_FLOAT, valuesFloat32: make([]float32, 0, size)}
	case thrift.Type_DOUBLE:
		return &Buffer{t: thrift.Type_DOUBLE, valuesFloat64: make([]float64, 0, size)}
	case thrift.Type_INT96:
		return &Buffer{t: thrift.Type_INT96, valuesInt96: make([]Int96, 0, size)}
	default:
		panic(fmt.Sprintf("Warning: not supported type %#v in plain encoding dictionaryPage", t))
	}
}

func (b *Buffer) Append(v interface{}) error {
	switch b.t {
	case thrift.Type_BOOLEAN:
		switch vv := v.(type) {
		case bool:
			b.valuesBool = append(b.valuesBool, vv)
		case int:
			b.valuesBool = append(b.valuesBool, vv == 1)
		case int32:
			b.valuesBool = append(b.valuesBool, vv == 1)
		case int64:
			b.valuesBool = append(b.valuesBool, vv == 1)
		case uint:
			b.valuesBool = append(b.valuesBool, vv == 1)
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	case thrift.Type_INT32:
		switch vv := v.(type) {
		case int32:
			b.valuesInt32 = append(b.valuesInt32, vv)
		case int:
			b.valuesInt32 = append(b.valuesInt32, int32(vv))
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}

	case thrift.Type_INT64:
		switch vv := v.(type) {
		case int64:
			b.valuesInt64 = append(b.valuesInt64, vv)
		case int:
			b.valuesInt64 = append(b.valuesInt64, int64(vv))
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	case thrift.Type_FIXED_LEN_BYTE_ARRAY:
		fallthrough
	case thrift.Type_BYTE_ARRAY:
		switch vv := v.(type) {
		case string:
			b.valuesByteArray = append(b.valuesByteArray, []byte(vv))
		case []byte:
			b.valuesByteArray = append(b.valuesByteArray, vv)
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	case thrift.Type_FLOAT:
		switch vv := v.(type) {
		case int:
			b.valuesFloat32 = append(b.valuesFloat32, float32(vv))
		case float32:
			b.valuesFloat32 = append(b.valuesFloat32, vv)
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	case thrift.Type_DOUBLE:
		switch vv := v.(type) {
		case float32:
			b.valuesFloat64 = append(b.valuesFloat64, float64(vv))
		case float64:
			b.valuesFloat64 = append(b.valuesFloat64, vv)
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	case thrift.Type_INT96:
		switch vv := v.(type) {
		case Int96:
			b.valuesInt96 = append(b.valuesInt96, vv)
		default:
			return fmt.Errorf("could not encode value %v as %s", vv, b.t)
		}
	default:
		panic(fmt.Sprintf("Warning: not supported type %#v in plain encoding dictionaryPage", v))
	}

	return nil
}

func (b *Buffer) Reset() {
	switch b.t {
	case thrift.Type_BOOLEAN:
		b.valuesBool = b.valuesBool[:0]
	case thrift.Type_INT32:
		b.valuesInt32 = b.valuesInt32[:0]
	case thrift.Type_INT64:
		b.valuesInt64 = b.valuesInt64[:0]
	case thrift.Type_BYTE_ARRAY:
		b.valuesByteArray = b.valuesByteArray[:0]
	case thrift.Type_FIXED_LEN_BYTE_ARRAY:
		b.valuesByteArray = b.valuesByteArray[:0]
	case thrift.Type_FLOAT:
		b.valuesFloat32 = b.valuesFloat32[:0]
	case thrift.Type_DOUBLE:
		b.valuesFloat64 = b.valuesFloat64[:0]
	case thrift.Type_INT96:
		b.valuesInt96 = b.valuesInt96[:0]
	}
}
