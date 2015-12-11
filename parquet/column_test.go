package parquet

import (
	"fmt"
	"os"
	"testing"
)

func TestColumnReader(t *testing.T) {
	r, err := os.Open("../../parquet-test/harness/input/Booleans.parquet")
	//f, err := os.Open("/home/ksh/downloads/nation.impala.parquet")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	// err = getColumnReader(f, 0)
	// fmt.Printf("Error: %s\n", err)

	m, err := ReadFileMetaData(r)
	if err != nil {
		t.Fatalf("%s", err)
	}
	schema, err := SchemaFromFileMetaData(m)
	if err != nil {
		t.Fatalf("%s", err)
	}

	c := 0
	for _, rg := range m.RowGroups {
		cc := rg.Columns[c]
		cr, err := NewBooleanColumnChunkReader(r, schema, cc)
		if err != nil {
			t.Fatalf("%s", err)
		}
		for cr.Next() {
			fmt.Println("->", cr.Value())
		}
		if cr.Err() != nil {
			t.Fatalf("%s", cr.Err())
		}
	}
}
