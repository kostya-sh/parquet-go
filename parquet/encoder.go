package parquet

import (
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

var (
	typeBoolean           = thrift.TypePtr(thrift.Type_BOOLEAN)
	typeInt32             = thrift.TypePtr(thrift.Type_INT32)
	typeInt64             = thrift.TypePtr(thrift.Type_INT64)
	typeInt96             = thrift.TypePtr(thrift.Type_INT96)
	typeFloat             = thrift.TypePtr(thrift.Type_FLOAT)
	typeDouble            = thrift.TypePtr(thrift.Type_DOUBLE)
	typeByteArray         = thrift.TypePtr(thrift.Type_BYTE_ARRAY)
	typeFixedLenByteArray = thrift.TypePtr(thrift.Type_FIXED_LEN_BYTE_ARRAY)
	frtOptional           = thrift.FieldRepetitionTypePtr(thrift.FieldRepetitionType_OPTIONAL)
	frtRequired           = thrift.FieldRepetitionTypePtr(thrift.FieldRepetitionType_REQUIRED)
	frtRepeated           = thrift.FieldRepetitionTypePtr(thrift.FieldRepetitionType_REPEATED)
	ctUTF8                = thrift.ConvertedTypePtr(thrift.ConvertedType_UTF8)
	ctMap                 = thrift.ConvertedTypePtr(thrift.ConvertedType_MAP)
	ctMapKeyValue         = thrift.ConvertedTypePtr(thrift.ConvertedType_MAP_KEY_VALUE)
	ctList                = thrift.ConvertedTypePtr(thrift.ConvertedType_LIST)
)

// Encoder
type Encoder interface {
	WriteRecords(records []map[string]interface{}) error

	WriteInt32(name string, values []int32) error
	WriteInt64(name string, values []int64) error
	WriteFloat32(name string, values []float32) error
	WriteFloat64(name string, values []float64) error
	WriteByteArray(name string, values [][]byte) error

	WriteBool(name string, values []bool) error
}

// RowGroup
type RowGroup struct {
	thrift.RowGroup
}

type defaultEncoder struct {
	schema         *Schema
	version        string
	w              *thrift.CountingWriter
	filemetadata   *thrift.FileMetaData
	columnEncoders map[string]DataEncoder
	rowgroups      []*RowGroup
}

// NewEncoder
func NewEncoder(schema *Schema, w io.Writer) Encoder {
	return &defaultEncoder{
		schema:         schema,
		version:        "parquet-go", // FIXME
		w:              thrift.NewCountingWriter(w),
		filemetadata:   thrift.NewFileMetaData(),
		columnEncoders: make(map[string]DataEncoder),
		rowgroups:      make([]*RowGroup, 0, 5),
	}
}

func (e *defaultEncoder) getColumnEncoder(name string) (DataEncoder, bool) {
	enc, ok := e.columnEncoders[name]
	if !ok {
		// TODO have a better configuration strategy to choose the encoding algorithm
		preferences := EncodingPreferences{CompressionCodec: "", Strategy: "default"}

		e.columnEncoders[name] = NewPageEncoder(preferences)
	}

	return enc, true
}

// WriteRecords will write all the values defined in the Schema found in the given records.
// WriteRecords does not copy the values.
func (e *defaultEncoder) WriteRecords(records []map[string]interface{}) error {

	for colname, coldesc := range e.schema.columns {
		var a accumulator

		var accumulate func(interface{}) error

		switch coldesc.SchemaElement.GetType() {
		case thrift.Type_INT32:
			accumulate = a.WriteInt32
		case thrift.Type_INT64:
			accumulate = a.WriteInt64
		case thrift.Type_FLOAT:
			accumulate = a.WriteFloat32
		case thrift.Type_DOUBLE:
			accumulate = a.WriteFloat64
		case thrift.Type_BOOLEAN:
			accumulate = a.WriteBoolean
		case thrift.Type_INT96:
			panic("not supported")
		case thrift.Type_BYTE_ARRAY:
			accumulate = a.WriteString
		case thrift.Type_FIXED_LEN_BYTE_ARRAY:
			accumulate = a.WriteString
		default:
			panic("type not supported")
		}

		for i := 0; i < len(records); i++ {
			record := records[i]

			value, ok := record[colname]
			if !ok /*&& coldesc.IsRequired() */ {
				// column not found in the schema. ignore
				return fmt.Errorf("row %d does not have required field %s", i, colname)
			}

			if err := accumulate(value); err != nil {
				return fmt.Errorf("invalid value %s: %s", colname, err)
			}

		}

		var err error

		switch coldesc.SchemaElement.GetType() {
		case thrift.Type_INT32:
			err = e.WriteInt32(colname, a.int32Buff)
		case thrift.Type_INT64:
			err = e.WriteInt64(colname, a.int64Buff)
		case thrift.Type_FLOAT:
			err = e.WriteFloat32(colname, a.float32Buff)
		case thrift.Type_DOUBLE:
			err = e.WriteFloat64(colname, a.float64Buff)
		case thrift.Type_BOOLEAN:
			err = e.WriteBool(colname, a.boolBuff)
		case thrift.Type_INT96:
			panic("not supported")
		case thrift.Type_BYTE_ARRAY:
			err = e.WriteByteArray(colname, a.byteArrayBuff)
		case thrift.Type_FIXED_LEN_BYTE_ARRAY:
			err = e.WriteByteArray(colname, a.byteArrayBuff)
		default:
			panic("type not supported")
		}
		if err != nil {
			return fmt.Errorf("could not write column %s: %s", colname, err)
		}
	}

	return nil
}

// WriteInt32
func (e *defaultEncoder) WriteInt32(name string, values []int32) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteInt32(values)
}

// WriteInt64
func (e *defaultEncoder) WriteInt64(name string, values []int64) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteInt64(values)
}

// WriteFloat32
func (e *defaultEncoder) WriteFloat32(name string, values []float32) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteFloat32(values)
}

// WriteFloat64
func (e *defaultEncoder) WriteFloat64(name string, values []float64) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteFloat64(values)
}

// WriteByteArray
func (e *defaultEncoder) WriteByteArray(name string, values [][]byte) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteByteArray(values)
}

// WriteBool
func (e *defaultEncoder) WriteBool(name string, values []bool) error {
	enc, ok := e.getColumnEncoder(name)
	if !ok {
		return fmt.Errorf("invalid column %s", name)
	}
	return enc.WriteBool(values)
}

// Close writes all the pending data to the underlying stream.
func (e *defaultEncoder) Close() error {
	if err := writeHeader(e.w); err != nil {
		return fmt.Errorf("could not write parquet header to stream: %s", err)
	}

	// for _, rowGroup := range e.rowgroups {
	//
	// }

	// has to be in the same order of the schema
	for _, colname := range e.schema.Columns() {
		_, ok := e.getColumnEncoder(colname)
		if !ok {
			panic("should not have a column not encoded")
		}

	}

	return nil
}
