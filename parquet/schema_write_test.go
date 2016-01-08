package parquet

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

func TestCanWriteSchemaWithNoColumns(t *testing.T) {
	c := NewEncoder("")
	var b bytes.Buffer

	if err := c.Write(&b); err != nil {
		t.Fatal(err)
	}

	d := NewDecoder(bytes.NewReader(b.Bytes()))
	if err := d.ReadSchema(); err != nil {
		t.Fatalf("error reading schema: %s", err)
	}

	if len(d.schema.columns) != 0 {
		t.Fatalf("expected 0 columns")
	}
}

func write() error {
	var offset int64 = 0
	name := "tmp"

	columnSchema := parquetformat.NewSchemaElement()
	columnWriters := []*ColumnEncoder{NewColumnEncoder(columnSchema)}

	var b bytes.Buffer

	for _, chunkWriter := range columnWriters {
		n, err := chunkWriter.WriteChunk(&b, offset, name)
		if err != nil {
			return fmt.Errorf("chunk writer: could not write chunk for column %s: %s", chunkWriter.Schema.Name, err)
		}
		offset += n
	}

	return nil
}

func TestCanWriteSchemaWithOneColumn(t *testing.T) {
	c := NewEncoder("")
	var b bytes.Buffer

	if err := c.Write(&b); err != nil {
		t.Fatal(err)
	}

	d := NewDecoder(bytes.NewReader(b.Bytes()))
	if err := d.ReadSchema(); err != nil {
		t.Fatalf("error reading schema: %s", err)
	}

	if len(d.schema.columns) != 0 {
		t.Fatalf("expected 0 columns")
	}
}
