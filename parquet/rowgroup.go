package parquet

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquet/column"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

// RowGroupScanner
type RowGroupScanner struct {
	r        io.ReadSeeker
	idx      int
	rowGroup *parquetformat.RowGroup
	columns  []*parquetformat.SchemaElement
}

func (rg *RowGroupScanner) NewColumnScanners() []*column.Scanner {
	var columnScanners []*column.Scanner

	for idx, columnSchema := range rg.columns {
		chunk := rg.rowGroup.GetColumns()[idx]
		columnScanners = append(columnScanners, column.NewScanner(rg.r, chunk, columnSchema))
	}

	return columnScanners
}

type WriteOffsetter interface {
	io.Writer
	Offset() int64
}

func createRowGroup(columns []*parquetformat.ColumnChunk) *parquetformat.RowGroup {
	rowGroup := parquetformat.NewRowGroup()
	var total int64 = 0
	var numRows int64 = 0
	for _, columnChunk := range columns {
		total += columnChunk.MetaData.GetTotalUncompressedSize()
		numRows = columnChunk.MetaData.GetNumValues()
	}

	rowGroup.TotalByteSize = total
	rowGroup.NumRows = numRows
	rowGroup.Columns = columns
	rowGroup.SortingColumns = []*parquetformat.SortingColumn{} // Not supported yet

	return rowGroup
}
