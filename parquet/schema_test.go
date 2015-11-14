package parquet

import (
	"bytes"
	"testing"

	"git.apache.org/thrift.git/lib/go/thrift"
	pf "github.com/kostya-sh/parquet-go/parquetformat"
)

func createFileMetaData(schema ...*pf.SchemaElement) pf.FileMetaData {
	meta := pf.FileMetaData{}
	meta.Schema = schema
	return meta
}

var typeBoolean *pf.Type = pf.TypePtr(pf.Type_BOOLEAN)
var typeInt32 *pf.Type = pf.TypePtr(pf.Type_INT32)
var typeInt64 *pf.Type = pf.TypePtr(pf.Type_INT64)
var typeInt96 *pf.Type = pf.TypePtr(pf.Type_INT96)
var typeFloat *pf.Type = pf.TypePtr(pf.Type_FLOAT)
var typeDouble *pf.Type = pf.TypePtr(pf.Type_DOUBLE)
var typeByteArray *pf.Type = pf.TypePtr(pf.Type_BYTE_ARRAY)
var typeFixedLenByteArray *pf.Type = pf.TypePtr(pf.Type_FIXED_LEN_BYTE_ARRAY)

var frtOptional *pf.FieldRepetitionType = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_OPTIONAL)
var frtRequired *pf.FieldRepetitionType = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REQUIRED)
var frtRepeated *pf.FieldRepetitionType = pf.FieldRepetitionTypePtr(pf.FieldRepetitionType_REPEATED)

var ctUTF8 *pf.ConvertedType = pf.ConvertedTypePtr(pf.ConvertedType_UTF8)
var ctMap *pf.ConvertedType = pf.ConvertedTypePtr(pf.ConvertedType_MAP)
var ctMapKeyValue *pf.ConvertedType = pf.ConvertedTypePtr(pf.ConvertedType_MAP_KEY_VALUE)
var ctList *pf.ConvertedType = pf.ConvertedTypePtr(pf.ConvertedType_LIST)

func TestCreateInvalidSchemas(t *testing.T) {
	invalidFileMetaDatas := []pf.FileMetaData{
		// empty schema array
		createFileMetaData(),

		// nil NumChildren
		createFileMetaData(
			&pf.SchemaElement{Name: "test"},
		),

		// negative NumChildren
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(-1)},
		),

		// invalid NumChildren (more then SchemaElement elements)
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(3)},
		),

		// no repetition_type for a leaf
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, Name: "f1"},
		),

		// NumChildren is too small
		createFileMetaData(
			&pf.SchemaElement{Name: "test1", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1"},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f2"},
		),

		// no TypeLength for fixed_len_byte_array
		createFileMetaData(
			&pf.SchemaElement{Name: "test1", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeFixedLenByteArray, RepetitionType: frtRequired, Name: "f1"},
		),

		// int32 with converted_type = UTF8
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeInt32, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctUTF8},
		),
		// boolean with converted_type = MAP
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctMap},
		),
		// boolean with converted_type = LIST
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctList},
		),
		// boolean with converted_type = MAP_KEY_VALUE
		createFileMetaData(
			&pf.SchemaElement{Name: "test", NumChildren: thrift.Int32Ptr(1)},
			&pf.SchemaElement{Type: typeBoolean, RepetitionType: frtRequired, Name: "f1", ConvertedType: ctMapKeyValue},
		),
	}

	for _, meta := range invalidFileMetaDatas {
		_, err := SchemaFromFileMetaData(meta)
		if err == nil {
			t.Errorf("Error expected for %+v", meta)
		} else {
			t.Logf("Error for %+v: %s", meta, err)
		}
	}
}

func TestCreateSchemaFromFileMetaDataAndMarshal(t *testing.T) {
	meta := createFileMetaData(
		&pf.SchemaElement{
			Name:        "test.Message",
			NumChildren: thrift.Int32Ptr(10),
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
			TypeLength:     thrift.Int32Ptr(10),
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
			NumChildren:    thrift.Int32Ptr(1),
		},
		&pf.SchemaElement{
			Type:           typeInt32,
			RepetitionType: frtOptional,
			Name:           "OptionalInt32",
		},
	)

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
}
`

	s, err := SchemaFromFileMetaData(meta)
	if err != nil {
		t.Fatalf("Unexpcted error: %s", err)
	}

	buf := new(bytes.Buffer)
	err = s.MarshalDL(buf)
	if err != nil {
		t.Fatalf("Unexpcted error: %s", err)
	}
	got := buf.String()
	if got != want {
		t.Errorf("MarshalDL. got: \n%s\nwant:\n%s", got, want)
	}
}

func TestMaxLevelsOfDremelPaperSchema(t *testing.T) {
	meta := createFileMetaData(
		&pf.SchemaElement{
			Name:        "Document",
			NumChildren: thrift.Int32Ptr(3),
		},
		&pf.SchemaElement{
			Name:           "DocId",
			Type:           typeInt64,
			RepetitionType: frtRequired,
		},
		&pf.SchemaElement{
			Name:           "Links",
			RepetitionType: frtOptional,
			NumChildren:    thrift.Int32Ptr(2),
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
			NumChildren:    thrift.Int32Ptr(2),
		},
		&pf.SchemaElement{
			Name:           "Language",
			RepetitionType: frtRepeated,
			NumChildren:    thrift.Int32Ptr(2),
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

	s, err := SchemaFromFileMetaData(meta)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	checkMaxLevels := func(path []string, wantD int, wantR int) {
		d, r := s.maxLevels(path)
		if d != wantD || r != wantR {
			t.Errorf("Wrong max levels for %v: got (D:%d, R:%d), want (D:%d, R:%d)", path, d, r, wantD, wantR)
		}
	}

	// required non-nested field
	checkMaxLevels([]string{"DocId"}, 0, 0)

	// optional/repeated
	checkMaxLevels([]string{"Links", "Forward"}, 2, 1)
	checkMaxLevels([]string{"Links", "Backward"}, 2, 1)

	// repeated/repeated/required
	checkMaxLevels([]string{"Name", "Language", "Code"}, 2, 2)

	// repeated/repeated/optional
	checkMaxLevels([]string{"Name", "Language", "Country"}, 3, 2)

	// repeated/optional
	checkMaxLevels([]string{"Name", "Url"}, 2, 1)

	// not a field
	checkMaxLevels([]string{"Links"}, -1, -1)
	checkMaxLevels([]string{"Name", "UnknownField"}, -1, -1)
}
