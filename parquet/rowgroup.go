package parquet

import (
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/column"
	"github.com/kostya-sh/parquet-go/parquet/datatypes"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// RowGroupScanner
type RowGroupScanner struct {
	r        io.ReadSeeker
	idx      int
	rowGroup *thrift.RowGroup
	columns  []*thrift.SchemaElement
}

func (rg *RowGroupScanner) NewColumnScanners() []*column.Scanner {
	var columnScanners []*column.Scanner

	for idx, columnSchema := range rg.columns {
		chunk := rg.rowGroup.GetColumns()[idx]
		columnScanners = append(columnScanners, column.NewScanner(rg.r, columnSchema, []*thrift.ColumnChunk{chunk}))
	}

	return columnScanners
}

type rowGroupEncoder struct {
	encoders        map[string]*column.Encoder
	columns         []string
	rowGroups       []*thrift.RowGroup
	currentRowGroup *thrift.RowGroup
}

func newRowGroupEncoder(s *Schema) *rowGroupEncoder {
	enc := &rowGroupEncoder{
		encoders:  make(map[string]*column.Encoder),
		rowGroups: []*thrift.RowGroup{},
		columns:   make([]string, 0, len(s.Elements())),
	}

	for _, element := range s.Elements() {
		enc.columns = append(enc.columns, element.Name)
		enc.encoders[element.Name] = column.NewEncoder(element, column.DefaultPreferences())
	}

	enc.addRowGroup(enc.newRowGroup())

	return enc
}

func (enc *rowGroupEncoder) addRowGroup(rowGroup *thrift.RowGroup) {
	enc.rowGroups = append(enc.rowGroups, rowGroup)
	enc.currentRowGroup = rowGroup
}

func (enc *rowGroupEncoder) newRowGroup() *thrift.RowGroup {
	rowGroup := thrift.NewRowGroup()
	rowGroup.SortingColumns = []*thrift.SortingColumn{} // Not supported yet
	return rowGroup
}

func (enc *rowGroupEncoder) WriteBuffer(column string, b *datatypes.Buffer) error {

	columnEncoder, ok := enc.encoders[column]
	if !ok {
		return fmt.Errorf("invalid column name %s", column)
	}

	return columnEncoder.WriteBuffer(b)

	//return nil
}

// Flush writes to disk all the pending memory buffers maintained in the encoders
func (enc *rowGroupEncoder) Write(w io.Writer) error {

	// we have to respect the order
	chunks := make([]*thrift.ColumnChunk, 0, len(enc.columns))

	for _, name := range enc.columns {
		encoder := enc.encoders[name]
		chunk, err := encoder.WriteChunk(w)
		if err != nil {
			return fmt.Errorf("error writing column chunk %s: %s", name, err)
		}

		columnChunk := thrift.NewColumnChunk()
		columnChunk.MetaData = encoder.Metadata
		// columnChunk.FilePath = &w.FilePath()
		columnChunk.MetaData.NumValues = chunk.NumValues()

		chunks = append(chunks, columnChunk)
		// chunk.Compress()
		//rowGroup := thrift.NewRowGroup()
		// total += int64(columnEncoder.CompressedSize())
		// rowGroup.NumRows = math.MaxInt(rowGroup.NumRows, columnEncoder.NumValues())
		enc.currentRowGroup.TotalByteSize += chunk.ByteSize()
	}

	enc.currentRowGroup.Columns = chunks

	return nil
}

// // WriteInt32
// func (e *defaultEncoder) WriteInt32(name string, values []int32) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteInt32(values)
// }

// // WriteInt64
// func (e *defaultEncoder) WriteInt64(name string, values []int64) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteInt64(values)
// }

// // WriteFloat32
// func (e *defaultEncoder) WriteFloat32(name string, values []float32) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteFloat32(values)
// }

// // WriteFloat64
// func (e *defaultEncoder) WriteFloat64(name string, values []float64) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteFloat64(values)
// }

// // WriteByteArray
// func (e *defaultEncoder) WriteByteArray(name string, values [][]byte) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteByteArray(values)
// }

// // WriteBool
// func (e *defaultEncoder) WriteBool(name string, values []bool) error {
// 	enc, ok := e.getColumnEncoder(name)
// 	if !ok {
// 		return fmt.Errorf("invalid column %s", name)
// 	}
// 	return enc.WriteBool(values)
// }
