package parquet

import (
	"bufio"
	"bytes"
	"io"
	"log"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

// Encoder
type Encoder struct {
	version string
	columns []*parquetformat.ColumnChunk
}

func NewEncoder(columns []*parquetformat.ColumnChunk) *Encoder {
	// parse schema
	schema_elements := []string{}
	for range schema_elements {

	}

	return &Encoder{
		columns: columns,
		version: "parquet-go", // FIXME
	}
}

func (c *Encoder) AddRowGroup() {

}

func NewPage() {

}

func NewColumnChunk(name string) (*parquetformat.ColumnChunk, bytes.Buffer) {
	values := make([]int32, 100)
	for i := 0; i < 100; i++ {
		values[i] = int32(i)
	}

	var final bytes.Buffer

	// DataPage
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	enc := encoding.NewPlainEncoder(w)
	for i := 0; i < 100; i++ {
		err := enc.WriteInt32(values[i])
		if err != nil {
			log.Fatal(err)
		}
	}
	enc.Flush()

	var compressed bytes.Buffer
	wc := snappy.NewWriter(&compressed)
	if _, err := io.Copy(wc, &b); err != nil {
		log.Fatal(err)
	}

	// Page Header
	header := parquetformat.NewPageHeader()
	header.CompressedPageSize = int32(compressed.Len())
	header.UncompressedPageSize = int32(b.Len())
	header.Type = parquetformat.PageType_DATA_PAGE
	header.DataPageHeader = parquetformat.NewDataPageHeader()
	header.DataPageHeader.NumValues = int32(100)
	header.DataPageHeader.Encoding = parquetformat.Encoding_PLAIN
	header.DataPageHeader.DefinitionLevelEncoding = parquetformat.Encoding_BIT_PACKED
	header.DataPageHeader.RepetitionLevelEncoding = parquetformat.Encoding_BIT_PACKED

	if _, err := header.Write(&final); err != nil {
		log.Fatal(err)
	}

	_, err := io.Copy(&final, &compressed)
	if err != nil {
		log.Fatal(err)
	}

	// ColumnChunk
	offset := 0
	filename := "thisfile.parquet"
	chunk := parquetformat.NewColumnChunk()
	chunk.FileOffset = int64(offset)
	chunk.FilePath = &filename
	chunk.MetaData = parquetformat.NewColumnMetaData()
	chunk.MetaData.TotalCompressedSize = int64(compressed.Len())
	chunk.MetaData.TotalUncompressedSize = int64(b.Len())
	chunk.MetaData.Codec = parquetformat.CompressionCodec_SNAPPY

	chunk.MetaData.DataPageOffset = 0
	chunk.MetaData.DictionaryPageOffset = nil

	chunk.MetaData.Type = parquetformat.Type_INT32
	chunk.MetaData.PathInSchema = []string{name}

	return chunk, final
}

type Decoder struct {
	r      io.ReadSeeker
	meta   *parquetformat.FileMetaData
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

func (d *Decoder) Columns() []ColumnSchema {
	var columns []ColumnSchema
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
