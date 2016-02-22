package column

// you can only have one dictionary page per each column chunk
import (
	"bytes"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	pf "github.com/kostya-sh/parquet-go/parquetformat"
)

type Encoder interface {
}

type defaultEncoder struct {
	Schema   *pf.SchemaElement
	Metadata *pf.ColumnMetaData

	dDecoder *encoding.RLE32Decoder
	rDecoder *encoding.RLE32Decoder
}

func NewEncoder(schema *pf.SchemaElement) Encoder {
	return &defaultEncoder{Schema: schema, Metadata: pf.NewColumnMetaData()}
}

func (e *defaultEncoder) WriteChunk(w io.Writer, offset int, name string) (int, error) {

	return 0, nil
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
