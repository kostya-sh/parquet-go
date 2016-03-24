package parquet

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquet/column"
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
		columnScanners = append(columnScanners, column.NewScanner(rg.r, chunk, columnSchema))
	}

	return columnScanners
}

// func newrow(pages []Page) *thrift.RowGroup {
// 	rowGroup := thrift.NewRowGroup()
// 	var total int64 = 0
// 	var numRows int64 = 0
// 	var columns []*thrift.SchemaElement

// 	for idx, page := range pages {
// 		total += int64(page.CompressedSize())
// 		numRows = math.MaxInt(numRows, page.NumValues())
// 		columns = append(columns, columns[i].SchemaElement)
// 	}

// 	rowGroup.TotalByteSize = total
// 	rowGroup.NumRows = numRows
// 	rowGroup.Columns = columns
// 	rowGroup.SortingColumns = []*thrift.SortingColumn{} // Not supported yet

// 	return rowGroup
// }
