package parquet

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

func TestReadFileMetaDataFromInvalidFiles(t *testing.T) {
	invalidFiles := []string{
		"NoMagicInHeader.parquet",
		"NoMagicInFooter.parquet",
		"InvalidFooterLength.parquet",
		"TooSmall.parquet",
		"CorruptedMeta.parquet",
	}

	for _, f := range invalidFiles {
		r, err := os.Open(fmt.Sprintf("testdata/invalid/%s", f))
		if err != nil {
			t.Errorf("Unable to read file %s: %s", f, err)
			continue
		}

		_, err = ReadFileMetaData(r)
		if err == nil {
			t.Errorf("Error expected reading %s", f)
		}
		t.Logf("%s: %s", f, err)
		r.Close()
	}
}

func TestReadFileMetaData(t *testing.T) {
	r, err := os.Open("testdata/OneRecord.parquet")
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	defer r.Close()

	m, err := ReadFileMetaData(r)
	if err != nil {
		t.Errorf("Unexptected error: %s", err)
	}
	b, _ := json.MarshalIndent(m, "", " ")
	t.Logf("Read: %s", b)

	// No need to write too many checks here. If a record has been read then
	// there is a very high chance that it has been deserialized by thrift
	// properly
	if m.NumRows != 1 {
		t.Errorf("NumRows: was %d, expected 1", m.NumRows)
	}
	if len(m.Schema) != 2 {
		t.Errorf("Shema size: was %d, expected 2", len(m.Schema))
	}
	fieldType := *m.Schema[1].Type
	if fieldType != parquetformat.Type_BOOLEAN {
		t.Errorf("Field type: was %s, expected BOOLEAN", fieldType)
	}
}
