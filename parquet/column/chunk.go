package column

import (
	"bytes"
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/memory"
	"github.com/kostya-sh/parquet-go/parquet/page"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

type Chunk struct {
	numValues  int64
	data       []*page.DataPage
	dictionary *page.DictionaryPage
	index      *page.IndexPage
	metadata   *thrift.ColumnMetaData
	buffer     *bytes.Buffer
}

func NewChunk(metadata *thrift.ColumnMetaData, buffer []byte) *Chunk {
	return &Chunk{metadata: metadata, buffer: bytes.NewBuffer(buffer)}
}

func (c *Chunk) NumValues() int64 {
	return c.numValues
}

func (c *Chunk) ByteSize() int64 {
	return int64(c.buffer.Len())
}

func (c *Chunk) Decode(acc memory.Accumulator) error {

	for _, dataPage := range c.data {
		if err := dataPage.Decode(c.dictionary, acc); err != nil {
			return fmt.Errorf("dataPage: %s", err)
		}
	}

	return nil
}

// func (c *Chunk) ColumnChunk() *thrift.ColumnChunk {
// 	cc := &thrift.ColumnChunk{}
// 	cc.FileOffset = fileoffset
// 	cc.FilePath = ""
// 	cc.MetaData = thrift.NewColumnMetaData()
// 	// cc.MetaData.
// }
