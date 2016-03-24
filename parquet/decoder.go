package parquet

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// Decoder
type Decoder struct {
	r      io.ReadSeeker
	meta   *thrift.FileMetaData
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
