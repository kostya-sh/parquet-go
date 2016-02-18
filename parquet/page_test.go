package parquet

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
)

func TestEncodeDataPageHeader(t *testing.T) {
	values := make([]int32, 100)
	for i := 0; i < 100; i++ {
		values[i] = int32(i)
	}

	var final bytes.Buffer

	// DataPage
	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	enc := encoding.NewPlainEncoder(w)
	err := enc.WriteInt32(values)
	if err != nil {
		t.Fatal(err)
	}
	enc.Flush()

	enc := NewPageEncoder(w)
	err := enc.WriteInt32(value)
	if err != nil {
		t.Fatal(err)
	}

	// var compressed bytes.Buffer
	// wc := snappy.NewWriter(&compressed)
	// if _, err := io.Copy(wc, &b); err != nil {
	// 	t.Fatal(err)
	// }

	// // Page Header
	// header := parquetformat.NewPageHeader()
	// header.CompressedPageSize = int32(compressed.Len())
	// header.UncompressedPageSize = int32(b.Len())
	// header.Type = parquetformat.PageType_DATA_PAGE
	// header.DataPageHeader = parquetformat.NewDataPageHeader()
	// header.DataPageHeader.NumValues = int32(enc.NumValues())
	// header.DataPageHeader.Encoding = parquetformat.Encoding_PLAIN
	// header.DataPageHeader.DefinitionLevelEncoding = parquetformat.Encoding_BIT_PACKED
	// header.DataPageHeader.RepetitionLevelEncoding = parquetformat.Encoding_BIT_PACKED
	// header.Write(&final)
	// _, err := io.Copy(&final, &compressed)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // ColumnChunk
	// offset := 0
	// name := "thisfile.parquet"
	// chunk := parquetformat.NewColumnChunk()
	// chunk.FileOffset = int64(offset)
	// chunk.FilePath = &name
	// chunk.MetaData = parquetformat.NewColumnMetaData()
	// chunk.MetaData.TotalCompressedSize = 0
	// chunk.MetaData.TotalUncompressedSize = 0
	// chunk.MetaData.Codec = parquetformat.CompressionCodec_SNAPPY

	// chunk.MetaData.DataPageOffset = 0
	// chunk.MetaData.DictionaryPageOffset = nil

	// chunk.MetaData.Type = parquetformat.Type_INT32
	// chunk.MetaData.PathInSchema = []string{"some"}

	// if _, err := chunk.Write(&final); err != nil {
	// 	t.Fatal(err)
	// }

	// // Schema Element
	// columnSchema := parquetformat.NewSchemaElement()
	// columnSchema.Name = name
	// columnSchema.NumChildren = nil
	// columnSchema.Type = typeInt32
	// columnSchema.RepetitionType = nil

	// // Encoder
	// eenc := NewEncoder([]*parquetformat.ColumnChunk{})
	// //eenc.AddRowGroup()
	// fd, err := os.Create("some.file.parquet")
	// if err != nil {
	// 	log.Println(err)
	// }

	// if err := eenc.Write(fd); err != nil {
	// 	log.Println(err)
	// }
	// fd.Close()
}
