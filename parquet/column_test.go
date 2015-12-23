package parquet

import (
	"fmt"
	"os"
	"testing"
)

func TestBooleanColumnReader(t *testing.T) {
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

	c := 2
	for _, rg := range m.RowGroups {
		cc := rg.Columns[c]
		cs := schema.ColumnByPath(cc.MetaData.PathInSchema)
		cr, err := NewBooleanColumnChunkReader(r, cs, cc)
		if err != nil {
			t.Fatalf("%s", err)
		}
		for cr.Next() {
			fmt.Printf("V:%v\tD:%d\tR:%d\n", cr.Value(), cr.Levels().D, cr.Levels().R)
		}
		if cr.Err() != nil {
			t.Fatalf("%s", cr.Err())
		}
	}
}

func TestByteArrayColumnReader(t *testing.T) {
	r, err := os.Open("../../parquet-test/harness/input/ByteArrays.parquet")
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

	c := 2
	for _, rg := range m.RowGroups {
		cc := rg.Columns[c]
		cs := schema.ColumnByPath(cc.MetaData.PathInSchema)
		cr, err := NewByteArrayColumnChunkReader(r, cs, cc)
		if err != nil {
			t.Fatalf("%s", err)
		}
		for cr.Next() {
			fmt.Printf("V:%v\tD:%d\tR:%d\n", string(cr.ByteArray()), cr.Levels().D, cr.Levels().R)
		}
		if cr.Err() != nil {
			t.Fatalf("%s", cr.Err())
		}
	}
}
