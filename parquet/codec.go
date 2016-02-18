package parquet

import (
	"bytes"
	"io"

	pf "github.com/kostya-sh/parquet-go/parquetformat"
)

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

// Encoder
type Encoder struct {
	version string
}

func NewEncoder(schema *Schema) *Encoder {
	// parse schema
	schema_elements := []string{}
	for range schema_elements {

	}

	return &Encoder{
		version: "parquet-go", // FIXME
	}
}

func NewColumnChunk(name string) (*pf.ColumnChunk, bytes.Buffer) {
	// values := make([]int32, 100)
	// for i := 0; i < 100; i++ {
	// 	values[i] = int32(i)
	// }

	var pageBuffer bytes.Buffer
	// w := bufio.NewWriter(&pageBuffer)
	// preferences := EncodingPreferences{
	// 	CompressionCodec: "gzip",
	// 	Strategy:         "default",
	// }

	// enc := NewPageEncoder(preferences)
	// for i := 0; i < 3; i++ {
	// 	err := enc.WriteInt32(values)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	// pages := enc.Pages()

	// // DataPage
	// var b bytes.Buffer
	// w := bufio.NewWriter(&b)
	// enc := encoding.NewPlainEncoder(w)
	// for i := 0; i < 100; i++ {
	// 	err := enc.WriteInt32(values)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	// enc.Flush()

	// var compressed bytes.Buffer
	// wc := snappy.NewWriter(&compressed)
	// if _, err := io.Copy(wc, &b); err != nil {
	// 	log.Fatal(err)
	// }

	// // Page Header
	// header := pf.NewPageHeader()
	// header.CompressedPageSize = int32(compressed.Len())
	// header.UncompressedPageSize = int32(b.Len())
	// header.Type = pf.PageType_DATA_PAGE
	// header.DataPageHeader = pf.NewDataPageHeader()
	// header.DataPageHeader.NumValues = int32(100)
	// header.DataPageHeader.Encoding = pf.Encoding_PLAIN
	// header.DataPageHeader.DefinitionLevelEncoding = pf.Encoding_BIT_PACKED
	// header.DataPageHeader.RepetitionLevelEncoding = pf.Encoding_BIT_PACKED

	// if _, err := header.Write(&final); err != nil {
	// 	log.Fatal(err)
	// }

	// _, err := io.Copy(&final, &compressed)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // ColumnChunk
	// offset := 0
	// filename := "thisfile.parquet"
	chunk := pf.NewColumnChunk()
	// chunk.FileOffset = int64(offset)
	// chunk.FilePath = &filename
	// chunk.MetaData = pf.NewColumnMetaData()
	// chunk.MetaData.TotalCompressedSize = int64(compressed.Len())
	// chunk.MetaData.TotalUncompressedSize = int64(b.Len())
	// chunk.MetaData.Codec = pf.CompressionCodec_SNAPPY

	// chunk.MetaData.DataPageOffset = 0
	// chunk.MetaData.DictionaryPageOffset = nil

	// chunk.MetaData.Type = pf.Type_INT32
	// chunk.MetaData.PathInSchema = []string{name}

	return chunk, pageBuffer
}

type Decoder struct {
	r      io.ReadSeeker
	meta   *pf.FileMetaData
	schema *Schema
}

func NewDecoder(r io.ReadSeeker) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) readSchema() (err error) {
	if d.meta != nil {
		return nil
	}
	d.meta, err = readFileMetaData(d.r)
	if err != nil {
		return err
	}

	d.schema, err = schemaFromFileMetaData(d.meta)

	return err
}

func (d *Decoder) Columns() []ColumnDescriptor {
	var columns []ColumnDescriptor
	if err := d.readSchema(); err != nil {
		panic(err) // FIXME
	}
	for _, v := range d.schema.columns {
		columns = append(columns, v)
	}

	return columns
}

func (d *Decoder) NewRowGroupScanner( /*filter ?*/ ) []*RowGroupScanner {
	var groups []*RowGroupScanner
	if err := d.readSchema(); err != nil {
		panic(err) // FIXME
	}

	rowGroups := d.meta.GetRowGroups()

	for _, rowGroup := range rowGroups {
		groups = append(groups, &RowGroupScanner{
			r:        d.r,
			idx:      0,
			rowGroup: rowGroup,
			columns:  d.meta.GetSchema()[1:],
		})
	}

	return groups
}
