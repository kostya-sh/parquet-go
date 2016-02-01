package parquet

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

type RowGroupScanner struct {
	r        io.ReadSeeker
	idx      int
	rowGroup *parquetformat.RowGroup
	columns  []*parquetformat.SchemaElement
}

func (rg *RowGroupScanner) NewColumnScanners() []*ColumnScanner {
	var columnScanners []*ColumnScanner

	for idx, columnSchema := range rg.columns {
		chunk := rg.rowGroup.GetColumns()[idx]
		columnScanners = append(columnScanners, NewColumnScanner(rg.r, chunk, columnSchema))
	}

	return columnScanners
}
