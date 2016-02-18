package parquet

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	pf "github.com/kostya-sh/parquet-go/parquetformat"
)

func int32Ptr(v int32) *int32 {
	return &v
}

func createFileMetaData(schema ...*pf.SchemaElement) *pf.FileMetaData {
	return &pf.FileMetaData{Schema: schema}
}

func TestCreateInvalidSchemas(t *testing.T) {
	invalidFileMetaDatas := []*pf.FileMetaData{
		// empty schema array
		createFileMetaData(),

		// nil NumChildren
		createFileMetaData(
			&pf.SchemaElement{Name: "test"},
		),

		// negative NumChildren
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(-1)},
		),

		// invalid NumChildren (more then SchemaElement elements)
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(3)},
		),

		// no repetition_type for a leaf
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, Name: "f1"},
		),

		// NumChildren is too small
		createFileMetaData(
			&pf.SchemaElement{Name: "test1", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1"},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f2"},
		),

		// no TypeLength for fixed_len_byte_array
		createFileMetaData(
			&pf.SchemaElement{Name: "test1", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeFixedLenByteArray, RepetitionType: frtRequired, Name: "f1"},
		),

		// int32 with converted_type = UTF8
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeInt32, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctUTF8},
		),
		// boolean with converted_type = MAP
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctMap},
		),
		// boolean with converted_type = LIST
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctList},
		),
		// boolean with converted_type = MAP_KEY_VALUE
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctMapKeyValue},
		),
	}

	for _, meta := range invalidFileMetaDatas {
		_, err := schemaFromFileMetaData(meta)
		if err == nil {
			t.Errorf("Error expected for %+v", meta)
		} else {
			t.Logf("Error for %+v: %s", meta, err)
		}
	}
}

func mustCreateSchema(meta *pf.FileMetaData) *Schema {
	s, err := schemaFromFileMetaData(meta)
	if err != nil {
		panic(err)
	}
	return s
}

func TestCreateSchemaFromFileMetaDataAndMarshal(t *testing.T) {
	s := mustCreateSchema(createFileMetaData(
		&pf.SchemaElement{
			Name:        "test.Message",
			NumChildren: int32Ptr(10),
		},
		&pf.SchemaElement{
			Type:           typeBoolean,
			RepetitionType: frtRequired,
			Name:           "RequiredBoolean",
		},
		&pf.SchemaElement{
			Type:           typeInt32,
			RepetitionType: frtOptional,
			Name:           "OptionalInt32",
		},
		&pf.SchemaElement{
			Type:           typeInt64,
			RepetitionType: frtRepeated,
			Name:           "RepeatedInt64",
		},
		&pf.SchemaElement{
			Type:           typeInt96,
			RepetitionType: frtOptional,
			Name:           "OptionalInt96",
		},
		&pf.SchemaElement{
			Type:           typeFloat,
			RepetitionType: frtOptional,
			Name:           "OptionalFloat",
		},
		&pf.SchemaElement{
			Type:           typeDouble,
			RepetitionType: frtOptional,
			Name:           "OptionalDouble",
		},
		&pf.SchemaElement{
			Type:           typeByteArray,
			RepetitionType: frtOptional,
			Name:           "OptionalByteArray",
		},
		&pf.SchemaElement{
			Type:           typeFixedLenByteArray,
			TypeLength:     int32Ptr(10),
			RepetitionType: frtOptional,
			Name:           "OptionalFixedLenByteArray",
		},
		&pf.SchemaElement{
			Type:           typeByteArray,
			RepetitionType: frtRequired,
			Name:           "RequiredString",
			ConvertedType:  ctUTF8,
		},
		&pf.SchemaElement{
			RepetitionType: frtRequired,
			Name:           "RequiredGroup",
			NumChildren:    int32Ptr(1),
		},
		&pf.SchemaElement{
			Type:           typeInt32,
			RepetitionType: frtOptional,
			Name:           "OptionalInt32",
		},
	))

	want := `message test.Message {
  required boolean RequiredBoolean;
  optional int32 OptionalInt32;
  repeated int64 RepeatedInt64;
  optional int96 OptionalInt96;
  optional float OptionalFloat;
  optional double OptionalDouble;
  optional byte_array OptionalByteArray;
  optional fixed_len_byte_array(10) OptionalFixedLenByteArray;
  required byte_array RequiredString (UTF8);
  required group RequiredGroup {
    optional int32 OptionalInt32;
  }
}`

	if got := s.DisplayString(); got != want {
		t.Errorf("DisplayString: got \n%s\nwant\n%s", got, want)
	}
}

var dremelPaperExampleMeta = createFileMetaData(
	&pf.SchemaElement{
		Name:        "Document",
		NumChildren: int32Ptr(3),
	},
	&pf.SchemaElement{
		Name:           "DocId",
		Type:           typeInt64,
		RepetitionType: frtRequired,
	},
	&pf.SchemaElement{
		Name:           "Links",
		RepetitionType: frtOptional,
		NumChildren:    int32Ptr(2),
	},
	&pf.SchemaElement{
		Name:           "Backward",
		Type:           typeInt64,
		RepetitionType: frtRepeated,
	},
	&pf.SchemaElement{
		Name:           "Forward",
		Type:           typeInt64,
		RepetitionType: frtRepeated,
	},
	&pf.SchemaElement{
		Name:           "Name",
		RepetitionType: frtRepeated,
		NumChildren:    int32Ptr(2),
	},
	&pf.SchemaElement{
		Name:           "Language",
		RepetitionType: frtRepeated,
		NumChildren:    int32Ptr(2),
	},
	&pf.SchemaElement{
		Name:           "Code",
		Type:           typeByteArray,
		RepetitionType: frtRequired,
	},
	&pf.SchemaElement{
		Name:           "Country",
		Type:           typeByteArray,
		RepetitionType: frtOptional,
	},
	&pf.SchemaElement{
		Name:           "Url",
		Type:           typeByteArray,
		RepetitionType: frtOptional,
	},
)

func TestSchemaColumns(t *testing.T) {
	s := mustCreateSchema(dremelPaperExampleMeta)

	eq := func(a *ColumnSchema, b *ColumnSchema) bool {
		if a == nil && b == nil {
			return true
		}
		if a == nil || b == nil {
			return false
		}
		return *a == *b
	}

	check := func(path []string, expected *ColumnSchema) {
		name := strings.Join(path, ".")
		cs := s.ColumnByPath(path)
		cs2 := s.ColumnByName(name)
		if !eq(cs, cs2) {
			t.Errorf("ColumnByPath(%v) = %+v is not the same as ColumnByName(%s) = %+v", path, cs, name, cs2)
		}
		if !eq(cs, expected) {
			t.Errorf("wrong ColumnSchema for %v: got %+v, want %+v", path, *cs, *expected)
		}
	}

	// required non-nested field
	check([]string{"DocId"}, &ColumnSchema{
		MaxLevels:     Levels{0, 0},
		SchemaElement: dremelPaperExampleMeta.Schema[1],
	})

	// optional/repeated
	check([]string{"Links", "Backward"}, &ColumnSchema{
		MaxLevels:     Levels{D: 2, R: 1},
		SchemaElement: dremelPaperExampleMeta.Schema[3],
	})
	check([]string{"Links", "Forward"}, &ColumnSchema{
		MaxLevels:     Levels{D: 2, R: 1},
		SchemaElement: dremelPaperExampleMeta.Schema[4],
	})

	// repeated/repeated/required
	check([]string{"Name", "Language", "Code"}, &ColumnSchema{
		MaxLevels:     Levels{D: 2, R: 2},
		SchemaElement: dremelPaperExampleMeta.Schema[7],
	})

	// repeated/repeated/optional
	check([]string{"Name", "Language", "Country"}, &ColumnSchema{
		MaxLevels:     Levels{D: 3, R: 2},
		SchemaElement: dremelPaperExampleMeta.Schema[8],
	})

	// repeated/optional
	check([]string{"Name", "Url"}, &ColumnSchema{
		MaxLevels:     Levels{D: 2, R: 1},
		SchemaElement: dremelPaperExampleMeta.Schema[9],
	})

	// not a field
	check([]string{"Links"}, nil)
	check([]string{"Name", "UnknownField"}, nil)
}

func TestDremelPaperExampleDisplayString(t *testing.T) {
	s := mustCreateSchema(dremelPaperExampleMeta)

	want := `message Document {
  required int64 DocId;
  optional group Links {
    repeated int64 Backward;
    repeated int64 Forward;
  }
  repeated group Name {
    repeated group Language {
      required byte_array Code;
      optional byte_array Country;
    }
    optional byte_array Url;
  }
}`

	if got := s.DisplayString(); got != want {
		t.Errorf("DisplayString: got \n%s\nwant\n%s", got, want)
	}
}

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

		_, err = readFileMetaData(r)
		if err == nil {
			t.Errorf("Error expected reading %s", f)
		}
		t.Logf("%s: %s", f, err)
		r.Close()
	}
}

func TestreadFileMetaData(t *testing.T) {
	r, err := os.Open("testdata/OneRecord.parquet")
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	defer r.Close()

	m, err := readFileMetaData(r)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
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
		t.Errorf("Schema size: was %d, expected 2", len(m.Schema))
	}
	fieldType := *m.Schema[1].Type
	if fieldType != pf.Type_BOOLEAN {
		t.Errorf("Field type: was %s, expected BOOLEAN", fieldType)
	}
}

// func TestCanWriteSchemaWithNoColumns(t *testing.T) {
// 	c := NewEncoder([]*pf.ColumnChunk{})
// 	var b bytes.Buffer

// 	if err := c.Write(&b); err != nil {
// 		t.Fatal(err)
// 	}

// 	d := NewDecoder(bytes.NewReader(b.Bytes()))
// 	if err := d.ReadSchema(); err != nil {
// 		t.Fatalf("error reading schema: %s", err)
// 	}

// 	if len(d.schema.columns) != 0 {
// 		t.Fatalf("expected 0 columns")
// 	}
// }

// func TestCanWriteSchemaWithOneColumnAndNoRows(t *testing.T) {
// 	c := NewEncoder([]*pf.ColumnChunk{})

// 	var b bytes.Buffer

// 	if err := c.Write(&b); err != nil {
// 		t.Fatal(err)
// 	}

// 	d := NewDecoder(bytes.NewReader(b.Bytes()))
// 	if err := d.ReadSchema(); err != nil {
// 		t.Fatalf("error reading schema: %s", err)
// 	}

// 	if len(d.schema.columns) != 0 {
// 		t.Fatalf("expected 0 columns")
// 	}
// }
