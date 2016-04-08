package parquet

import (
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/datatypes"
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
	ctInt32               = thrift.ConvertedTypePtr(thrift.ConvertedType_INT_32)
	ctInt64               = thrift.ConvertedTypePtr(thrift.ConvertedType_INT_64)
	ctUTF8                = thrift.ConvertedTypePtr(thrift.ConvertedType_UTF8)
	ctMap                 = thrift.ConvertedTypePtr(thrift.ConvertedType_MAP)
	ctMapKeyValue         = thrift.ConvertedTypePtr(thrift.ConvertedType_MAP_KEY_VALUE)
	ctList                = thrift.ConvertedTypePtr(thrift.ConvertedType_LIST)
)

type Type int64

const (
	Boolean           Type = 0
	Int32             Type = 1
	Int64             Type = 2
	Int96             Type = 3
	Float             Type = 4
	Double            Type = 5
	ByteArray         Type = 6
	FixedLenByteArray Type = 7
)

func (p Type) String() string {
	switch p {
	case Boolean:
		return "BOOLEAN"
	case Int32:
		return "INT32"
	case Int64:
		return "INT64"
	case Int96:
		return "INT96"
	case Float:
		return "FLOAT"
	case Double:
		return "DOUBLE"
	case ByteArray:
		return "BYTE_ARRAY"
	case FixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	}
	return "<UNSET>"
}

func parquetType(t thrift.Type) Type {
	switch t {
	case thrift.Type_BOOLEAN:
		return Boolean
	case thrift.Type_INT32:
		return Int32
	case thrift.Type_INT64:
		return Int64
	case thrift.Type_INT96:
		return Int96
	case thrift.Type_FLOAT:
		return Float
	case thrift.Type_DOUBLE:
		return Double
	case thrift.Type_BYTE_ARRAY:
		return ByteArray
	case thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return FixedLenByteArray
	default:
		return Boolean
	}
}

// Encoder
type Encoder interface {
	WriteRecords(records []map[string]interface{}) error
	Close() error

	// WriteInt32(name string, values []int32) error
	// WriteInt64(name string, values []int64) error
	// WriteFloat32(name string, values []float32) error
	// WriteFloat64(name string, values []float64) error
	// WriteByteArray(name string, values [][]byte) error

	// WriteBool(name string, values []bool) error
}

// RowGroup
type RowGroup struct {
	thrift.RowGroup
}

type defaultEncoder struct {
	io.WriteCloser
	schema          *Schema
	version         string
	filemetadata    *thrift.FileMetaData
	rowGroupEncoder *rowGroupEncoder
	headerWritten   bool
	recordBuffer    *datatypes.RecordBuffer
}

// NewEncoder
// TODO add ability to configure encoder behavior
func NewEncoder(schema *Schema, w io.WriteCloser) Encoder {
	enc := &defaultEncoder{
		WriteCloser:     w,
		schema:          schema,
		version:         "parquet-go", // FIXME
		filemetadata:    thrift.NewFileMetaData(),
		rowGroupEncoder: newRowGroupEncoder(schema),
		recordBuffer:    datatypes.NewRecordbuffer(schema.Elements()),
	}

	return enc
}

// WriteRecords will write all the values defined in the Schema found in the given records.
// WriteRecords does not copy the values.
func (e *defaultEncoder) WriteRecords(records []map[string]interface{}) error {
	if !e.headerWritten {
		if err := e.writeHeader(); err != nil {
			return fmt.Errorf("could not write header: %s", err)
		}
		e.headerWritten = true
	}

	for _, r := range records {
		e.recordBuffer.Append(r)
	}

	// TODO make this configurable
	if e.recordBuffer.Len() < 1024 {
		return nil
	}

	// write the whole record buffer to the rowGroupEncoder
	if err := e.recordBuffer.Write(e.rowGroupEncoder); err != nil {
		return err
	}

	e.recordBuffer.Reset()

	return e.rowGroupEncoder.Write(e)
}

// Close writes all the pending data to the underlying stream.
func (e *defaultEncoder) Close() error {
	if err := e.writeHeader(); err != nil {
		return err
	}

	// Write Metadata
	err := writeFileMetadata(e, e.filemetadata)
	if err != nil {
		return err
	}
	return e.WriteCloser.Close()
}

func (e *defaultEncoder) writeHeader() error {
	if !e.headerWritten {
		if err := writeHeader(e); err != nil {
			return fmt.Errorf("could not write header: %s", err)
		}
		e.headerWritten = true
	}
	return nil
}
