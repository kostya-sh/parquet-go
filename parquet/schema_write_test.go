package parquet

import (
	"bytes"
	"testing"
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
