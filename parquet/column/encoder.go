package column

import (
	"bytes"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// Note: you can only have one dictionary page per each column chunk

//Encoder
type Encoder struct {
	Schema   *thrift.SchemaElement
	Metadata *thrift.ColumnMetaData
}

// NewEncoder
func NewEncoder(schema *thrift.SchemaElement) *Encoder {
	return &Encoder{Schema: schema, Metadata: thrift.NewColumnMetaData()}
}

func NewColumnChunk(name string) (*thrift.ColumnChunk, bytes.Buffer) {
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
	// header := thrift.NewPageHeader()
	// header.CompressedPageSize = int32(compressed.Len())
	// header.UncompressedPageSize = int32(b.Len())
	// header.Type = thrift.PageType_DATA_PAGE
	// header.DataPageHeader = thrift.NewDataPageHeader()
	// header.DataPageHeader.NumValues = int32(100)
	// header.DataPageHeader.Encoding = thrift.Encoding_PLAIN
	// header.DataPageHeader.DefinitionLevelEncoding = thrift.Encoding_BIT_PACKED
	// header.DataPageHeader.RepetitionLevelEncoding = thrift.Encoding_BIT_PACKED

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
	chunk := thrift.NewColumnChunk()
	// chunk.FileOffset = int64(offset)
	// chunk.FilePath = &filename
	// chunk.MetaData = thrift.NewColumnMetaData()
	// chunk.MetaData.TotalCompressedSize = int64(compressed.Len())
	// chunk.MetaData.TotalUncompressedSize = int64(b.Len())
	// chunk.MetaData.Codec = thrift.CompressionCodec_SNAPPY

	// chunk.MetaData.DataPageOffset = 0
	// chunk.MetaData.DictionaryPageOffset = nil

	// chunk.MetaData.Type = thrift.Type_INT32
	// chunk.MetaData.PathInSchema = []string{name}

	return chunk, pageBuffer
}
