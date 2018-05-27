package parquet

import (
	"fmt"
	"io"
	"os"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

type File struct {
	MetaData *parquetformat.FileMetaData
	Schema   Schema
	reader   io.ReadSeeker
}

func OpenFile(path string) (*File, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: failed to open file: %s", err)
	}

	meta, err := ReadFileMetaData(r)
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("parquet: failed to read metadata: %s", err)
	}
	schema, err := MakeSchema(meta)
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("parquet: failed to parse schema: %s", err)
	}

	return &File{
		MetaData: meta,
		Schema:   schema,
		reader:   r,
	}, nil
}

// NewReader creates a ColumnChunkReader for readng a single column chunk for
// column col from a row group rg.
func (f File) NewReader(col Column, rg int) (*ColumnChunkReader, error) {
	if rg >= len(f.MetaData.RowGroups) {
		return nil, fmt.Errorf("no such rowgroup: %d", rg)
	}
	chunks := f.MetaData.RowGroups[rg].Columns
	if col.Index() >= len(chunks) {
		return nil, fmt.Errorf("rowgroup %d has %d column chunks, column %d requested",
			rg, len(chunks), col.Index())
	}
	return newColumnChunkReader(f.reader, f.MetaData, col, chunks[col.Index()])
}

func (f *File) Close() error {
	if c, ok := f.reader.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
