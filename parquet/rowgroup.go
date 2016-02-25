package parquet

import (
	"bufio"
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

type RowGroup struct {
	pages []Page
}

func NewRowGroup(w io.Writer) *RowGroup {
	return &RowGroup{
		buffer: bufio.NewWriter(w),
	}
}

// func (rg *RowGroup) newDataPage(col *ColumnDescriptor) dataPage {

// }
func (*) MarshalThrift(w WriteOffsetter) error {

}

func newrow(columns []*ColumnDescriptor, pages []Page) *parquetformat.RowGroup {
	rowGroup := parquetformat.NewRowGroup()
	var total int64 = 0
	var numRows int64 = 0
	var columns []*parquetformat.SchemaElement

	for idx, page := range pages {
		total += int64(page.CompressedSize())
		numRows = math.MaxInt(numRows, page.NumValues())
		columns = append(columns, columns[i].SchemaElement)
	}

	rowGroup.TotalByteSize = total
	rowGroup.NumRows = numRows
	rowGroup.Columns = columns
	rowGroup.SortingColumns = []*parquetformat.SortingColumn{} // Not supported yet

	return rowGroup
}
