package parquet

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

type ColumnEncoder struct {
	Schema *parquetformat.SchemaElement
}

func NewColumnEncoder(schema *parquetformat.SchemaElement) *ColumnEncoder {
	return &ColumnEncoder{Schema: schema}
}

func (e *ColumnEncoder) WriteChunk(w io.Writer, offset int64, name string) (int64, error) {
	chunk := parquetformat.NewColumnChunk()
	chunk.FileOffset = offset
	chunk.FilePath = &name

	return chunk.Write(w)
}
