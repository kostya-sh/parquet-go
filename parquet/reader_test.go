package parquet

import (
	"reflect"
	"testing"
)

type cell struct {
	d int
	r int
	v interface{}
}

func checkColumnReaderValues(t *testing.T, path string, c int, expected []cell) {
	t.Helper()

	f, err := OpenFile(path)
	if err != nil {
		t.Errorf("failed to open %s: %s", path, err)
		return
	}
	defer f.Close()

	col := f.Schema.Columns()[c]
	cr, err := f.NewReader(col, 0) // TODO: iterate over all row grouops
	if err != nil {
		t.Errorf("failed to created column reader for column %d: %s", c, err)
		return
	}

	k := 0
	for {
		vals := make([]interface{}, 3, 3)

		d := make([]int, 3, 3)
		r := make([]int, 3, 3)
		n, err := cr.Read(vals, d, r)
		if err == EndOfChunk {
			break
		}
		if err != nil {
			t.Errorf("column %d: read failed: %s", c, err)
			break
		}

		for i, vi := 0, 0; i < n; i++ {
			if k < len(expected) {
				got := cell{d[i], r[i], nil}
				if d[i] == col.MaxD() {
					got.v = vals[vi]
					vi++
				}

				if want := expected[k]; !reflect.DeepEqual(got, want) {
					t.Errorf("column %d: value at pos %d = %#v, want %#v", c, k, got, want)
				}
				//fmt.Printf("V:%v\tD:%d\tR:%d\n", cr.Value(), cr.Levels().D, cr.Levels().R)
			}
			k++
		}

	}
	if k != len(expected) {
		t.Errorf("column %d: read %d values, want %d values", c, k, len(expected))
	}
}

func TestColumnReaderBoolean(t *testing.T) {
	checkColumnReaderValues(t, "testdata/Booleans.parquet", 0, []cell{
		{0, 0, true},
		{0, 0, true},
		{0, 0, false},
		{0, 0, true},
		{0, 0, false},
		{0, 0, true},
	})

	checkColumnReaderValues(t, "testdata/Booleans.parquet", 1, []cell{
		{0, 0, nil},
		{1, 0, false},
		{1, 0, true},
		{1, 0, true},
		{0, 0, nil},
		{1, 0, true},
	})

	checkColumnReaderValues(t, "testdata/Booleans.parquet", 2, []cell{
		{0, 0, nil},

		{0, 0, nil},

		{1, 0, true},

		{1, 0, true},
		{1, 1, false},
		{1, 1, true},

		{0, 0, nil},
		{1, 0, true},
	})
}

func TestColumnReaderByteArray(t *testing.T) {
	checkColumnReaderValues(t, "testdata/ByteArrays.parquet", 0, []cell{
		{0, 0, []byte{'r', '1'}},
		{0, 0, []byte{'r', '2'}},
		{0, 0, []byte{'r', '3'}},
		{0, 0, []byte{'r', '4'}},
		{0, 0, []byte{'r', '5'}},
		{0, 0, []byte{'r', '6'}},
	})

	checkColumnReaderValues(t, "testdata/ByteArrays.parquet", 1, []cell{
		{0, 0, nil},
		{1, 0, []byte{'o', '2'}},
		{1, 0, []byte{'o', '3'}},
		{1, 0, []byte{'o', '4'}},
		{0, 0, nil},
		{1, 0, []byte{'o', '6'}},
	})

	checkColumnReaderValues(t, "testdata/ByteArrays.parquet", 2, []cell{
		{0, 0, nil},

		{0, 0, nil},

		{1, 0, []byte{'p', '3', '_', '1'}},

		{1, 0, []byte{'p', '4', '_', '1'}},
		{1, 1, []byte{'p', '4', '_', '2'}},
		{1, 1, []byte{'p', '4', '_', '3'}},

		{0, 0, nil},

		{1, 0, []byte{'p', '6', '_', '1'}},
	})
}

func TestColumnReaderDicByteArray(t *testing.T) {
	checkColumnReaderValues(t, "testdata/ByteArrays.parquet", 3, []cell{
		{0, 0, []byte{'p', 'a', 'r', 'q', 'u', 'e', 't'}},
		{0, 0, []byte{'g', 'o'}},
		{0, 0, []byte{'p', 'a', 'r', 'q', 'u', 'e', 't'}},
		{0, 0, []byte{'g', 'o'}},
		{0, 0, []byte{'p', 'a', 'r', 'q', 'u', 'e', 't'}},
		{0, 0, []byte{'g', 'o'}},
	})
}

func TestSkipPage(t *testing.T) {
	f, err := OpenFile("testdata/Booleans.parquet")
	if err != nil {
		t.Errorf("failed to open file: %s", err)
		return
	}
	defer f.Close()

	cr, err := f.NewReader(f.Schema.Columns()[0], 0)
	if err != nil {
		t.Errorf("failed to create column reader: %s", err)
		return
	}

	if cr.PageHeader() == nil {
		t.Errorf("PageHeader is null")
	}

	err = cr.SkipPage()
	if err != EndOfChunk {
		t.Errorf("unexpected error: want %s, got %s", EndOfChunk, err)
	}

	if ph := cr.PageHeader(); ph != nil {
		t.Errorf("PageHeader is not null at the end of the chunk: %v", ph)
	}
}
