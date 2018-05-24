package parquet

import (
	"fmt"
	"io"
	"strings"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// ColumnChunkReader provides methods to read values stored from a single
// parquet column chunk.
type ColumnChunkReader interface {
	Next() bool
	Levels() Levels
	Value() interface{}
	Err() error
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

// NewColumnChunkReader creates a ColumnChunkReader to read cc from r.
func NewColumnChunkReader(r io.ReadSeeker, col Column, cc parquetformat.ColumnChunk) (ColumnChunkReader, error) {
	if ccName := strings.Join(cc.MetaData.PathInSchema, "."); ccName != col.name {
		return nil, fmt.Errorf("column schema for %s and column chunk for %s do not match", col.name, ccName)
	}
	switch col.schemaElement.GetType() {
	case parquetformat.Type_BOOLEAN:
		return newBooleanColumnChunkReader(r, col, cc)
	case parquetformat.Type_BYTE_ARRAY:
		return newByteArrayColumnChunkReader(r, col, cc)
	default:
		return nil, fmt.Errorf("Type %s not yet supported", col.schemaElement.GetType())
	}
}
