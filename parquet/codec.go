package parquet

import (
	"fmt"
	"io"

	pf "github.com/kostya-sh/parquet-go/parquetformat"
)

var (
	typeBoolean           = pf.TypePtr(pf.Type_BOOLEAN)
	typeInt32             = pf.TypePtr(pf.Type_INT32)
	typeInt64             = pf.TypePtr(pf.Type_INT64)
	typeInt96             = pf.TypePtr(pf.Type_INT96)
	typeFloat             = pf.TypePtr(pf.Type_FLOAT)
	typeDouble            = pf.TypePtr(pf.Type_DOUBLE)
	typeByteArray         = pf.TypePtr(pf.Type_BYTE_ARRAY)
	typeFixedLenByteArray = pf.TypePtr(pf.Type_FIXED_LEN_BYTE_ARRAY)
	frtOptional           = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_OPTIONAL)
	frtRequired           = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REQUIRED)
	frtRepeated           = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REPEATED)
	ctUTF8                = pf.ConvertedTypePtr(pf.ConvertedType_UTF8)
	ctMap                 = pf.ConvertedTypePtr(pf.ConvertedType_MAP)
	ctMapKeyValue         = pf.ConvertedTypePtr(pf.ConvertedType_MAP_KEY_VALUE)
	ctList                = pf.ConvertedTypePtr(pf.ConvertedType_LIST)
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

type defaultEncoder struct {
	schema         *Schema
	version        string
	w              *CountingWriter
	filemetadata   *pf.FileMetaData
	columnEncoders map[string]DataEncoder
	rowgroups      []*RowGroup
}

func NewEncoder(schema *Schema, w io.Writer) Encoder {
	return &defaultEncoder{
		schema:         schema,
		version:        "parquet-go", // FIXME
		w:              NewCountingWriter(w),
		filemetadata:   pf.NewFileMetaData(),
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
		case pf.Type_INT32:
			accumulate = a.WriteInt32
		case pf.Type_INT64:
			accumulate = a.WriteInt64
		case pf.Type_FLOAT:
			accumulate = a.WriteFloat32
		case pf.Type_DOUBLE:
			accumulate = a.WriteFloat64
		case pf.Type_BOOLEAN:
			accumulate = a.WriteBoolean
		case pf.Type_INT96:
			panic("not supported")
		case pf.Type_BYTE_ARRAY:
			accumulate = a.WriteString
		case pf.Type_FIXED_LEN_BYTE_ARRAY:
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
		case pf.Type_INT32:
			err = e.WriteInt32(colname, a.int32Buff)
		case pf.Type_INT64:
			err = e.WriteInt64(colname, a.int64Buff)
		case pf.Type_FLOAT:
			err = e.WriteFloat32(colname, a.float32Buff)
		case pf.Type_DOUBLE:
			err = e.WriteFloat64(colname, a.float64Buff)
		case pf.Type_BOOLEAN:
			err = e.WriteBool(colname, a.boolBuff)
		case pf.Type_INT96:
			panic("not supported")
		case pf.Type_BYTE_ARRAY:
			err = e.WriteByteArray(colname, a.byteArrayBuff)
		case pf.Type_FIXED_LEN_BYTE_ARRAY:
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

	for _, rowGroup := range e.rowgroups {
		err := rowGroup.MarshalThrift(e.w)

		for _, page := range rowGroup.pages {

		}
	}

	// has to be in the same order of the schema
	for _, colname := range e.schema.Columns() {
		enc, ok := e.getColumnEncoder(colname)
		if !ok {
			panic("should not have a column not encoded")
		}

	}

	return nil
}

// Decoder
type Decoder struct {
	r      io.ReadSeeker
	meta   *pf.FileMetaData
	schema *Schema
}

// NewDecoder
func NewDecoder(r io.ReadSeeker) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) readSchema() (err error) {
	if d.meta != nil {
		return nil
	}
	d.meta, err = readFileMetaData(d.r)
	if err != nil {
		return err
	}

	d.schema, err = schemaFromFileMetaData(d.meta)

	return err
}

func (d *Decoder) Columns() []ColumnDescriptor {
	var columns []ColumnDescriptor
	if err := d.readSchema(); err != nil {
		panic(err) // FIXME
	}
	for _, v := range d.schema.columns {
		columns = append(columns, v)
	}

	return columns
}

func (d *Decoder) NewRowGroupScanner( /*filter ?*/ ) []*RowGroupScanner {
	var groups []*RowGroupScanner
	if err := d.readSchema(); err != nil {
		panic(err) // FIXME
	}

	rowGroups := d.meta.GetRowGroups()

	for _, rowGroup := range rowGroups {
		groups = append(groups, &RowGroupScanner{
			r:        d.r,
			idx:      0,
			rowGroup: rowGroup,
			columns:  d.meta.GetSchema()[1:],
		})
	}

	return groups
}
