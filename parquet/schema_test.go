package parquet

import (
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

var typeBoolean = pf.TypePtr(pf.Type_BOOLEAN)
var typeInt32 = pf.TypePtr(pf.Type_INT32)
var typeInt64 = pf.TypePtr(pf.Type_INT64)
var typeInt96 = pf.TypePtr(pf.Type_INT96)
var typeFloat = pf.TypePtr(pf.Type_FLOAT)
var typeDouble = pf.TypePtr(pf.Type_DOUBLE)
var typeByteArray = pf.TypePtr(pf.Type_BYTE_ARRAY)
var typeFixedLenByteArray = pf.TypePtr(pf.Type_FIXED_LEN_BYTE_ARRAY)

var frtOptional = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_OPTIONAL)
var frtRequired = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REQUIRED)
var frtRepeated = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REPEATED)

var ctUTF8 = pf.ConvertedTypePtr(pf.ConvertedType_UTF8)
var ctMap = pf.ConvertedTypePtr(pf.ConvertedType_MAP)
var ctMapKeyValue = pf.ConvertedTypePtr(pf.ConvertedType_MAP_KEY_VALUE)
var ctList = pf.ConvertedTypePtr(pf.ConvertedType_LIST)

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
		_, err := MakeSchema(meta)
		if err == nil {
			t.Errorf("Error expected for %+v", meta)
		} else {
			t.Logf("Error for %+v: %s", meta, err)
		}
	}
}

func mustCreateSchema(meta *pf.FileMetaData) Schema {
	s, err := MakeSchema(meta)
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

	check := func(path []string, expected *Column) {
		name := strings.Join(path, ".")
		col, found := s.ColumnByPath(path)
		col2, found2 := s.ColumnByName(name)
		if found != found2 || col != col2 {
			t.Errorf("ColumnByPath(%v) = %+v is not the same as ColumnByName(%s) = %+v", path, col, name, col2)
		}
		if (expected == nil && found) || (expected != nil && *expected != col) {
			t.Errorf("wrong ColumnSchema for %v: got %+v, want %+v", path, col, expected)
		}
	}

	// required non-nested field
	check([]string{"DocId"}, &Column{
		index:         0,
		name:          "DocId",
		maxLevels:     levels{0, 0},
		schemaElement: dremelPaperExampleMeta.Schema[1],
	})

	// optional/repeated
	check([]string{"Links", "Backward"}, &Column{
		index:         1,
		name:          "Links.Backward",
		maxLevels:     levels{d: 2, r: 1},
		schemaElement: dremelPaperExampleMeta.Schema[3],
	})
	check([]string{"Links", "Forward"}, &Column{
		index:         2,
		name:          "Links.Forward",
		maxLevels:     levels{d: 2, r: 1},
		schemaElement: dremelPaperExampleMeta.Schema[4],
	})

	// repeated/repeated/required
	check([]string{"Name", "Language", "Code"}, &Column{
		index:         3,
		name:          "Name.Language.Code",
		maxLevels:     levels{d: 2, r: 2},
		schemaElement: dremelPaperExampleMeta.Schema[7],
	})

	// repeated/repeated/optional
	check([]string{"Name", "Language", "Country"}, &Column{
		index:         4,
		name:          "Name.Language.Country",
		maxLevels:     levels{d: 3, r: 2},
		schemaElement: dremelPaperExampleMeta.Schema[8],
	})

	// repeated/optional
	check([]string{"Name", "Url"}, &Column{
		index:         5,
		name:          "Name.Url",
		maxLevels:     levels{d: 2, r: 1},
		schemaElement: dremelPaperExampleMeta.Schema[9],
	})

	// not a field
	check([]string{"Links"}, nil)
	check([]string{"Name", "UnknownField"}, nil)

	cols := s.Columns()
	if len(cols) != 6 {
		t.Errorf("len(Columns()) = %d, want 6", len(cols))
	}
	for i, col := range cols {
		if col.Index() != i {
			t.Errorf("Index(%v) = %d, want %d", col, col.Index(), i)
		}
	}
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
