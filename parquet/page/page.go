package page

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// DataEncoder
type DataEncoder interface {
	WriteBool([]bool) error
	WriteInt32([]int32) error
	WriteInt64([]int64) error
	WriteFloat32([]float32) error
	WriteFloat64([]float64) error
	WriteByteArray([][]byte) error
}

type dataPage struct {
	header *thrift.PageHeader
}

func (page *dataPage) Type() thrift.PageType {
	return thrift.PageType_DATA_PAGE
}

func (page *dataPage) CompressedSize() int32 {
	return page.header.CompressedPageSize
}

func (page *dataPage) UncompressedSize() int32 {
	return page.header.UncompressedPageSize
}

func (page *dataPage) NumValues() int32 {
	return page.header.DataPageHeader.NumValues
}

// encoder should provide this
func newDataPage() *dataPage {

	datapage := thrift.NewDataPageHeader()
	datapage.NumValues = 0
	datapage.Encoding = thrift.Encoding_PLAIN
	datapage.DefinitionLevelEncoding = thrift.Encoding_RLE
	datapage.RepetitionLevelEncoding = thrift.Encoding_RLE
	datapage.Statistics = nil /** Optional statistics for the data in this page**/

	header := thrift.NewPageHeader()
	header.Type = thrift.PageType_DATA_PAGE
	header.DataPageHeader = datapage
	header.CompressedPageSize = 0
	header.UncompressedPageSize = 0

	return &dataPage{header}
}

type Page interface {
	// Type() thrift.PageType
	// CompressedSize() int32
	// UncompressedSize() int32
	NumValues() int32
}

// PageEncoder encodes a stream of values into a set of pages
type PageEncoder interface {
	DataEncoder
	Pages() []Page
}

// EncodingPreferences specify how to encode
type EncodingPreferences struct {
	CompressionCodec string // specify compression codec
	Strategy         string // Strategy is the name of the strategy to use to compress the data.
}

// NewPageEncoder creates a default encoder.
func NewPageEncoder(preferences EncodingPreferences) PageEncoder {
	switch preferences.CompressionCodec {
	case "lzo":
		panic("not yet supported")
	case "gzip":

	case "snappy":
		panic("not yet supported")
	case "":

	default:
		panic("compression codec not supported")
	}

	var encoder PageEncoder

	switch preferences.Strategy {
	case "default":
		fallthrough
	default:
		encoder = newDefaultPageEncoder(preferences.CompressionCodec)
	}

	return encoder
}

type defaultPageEncoder struct {
	buffer        bytes.Buffer
	pages         []Page
	currentWriter *bufio.Writer
	encoder       encoding.Encoder
	encoderType   thrift.Encoding
	compression   string
}

func newDefaultPageEncoder(compressionCodec string) *defaultPageEncoder {
	encoder := &defaultPageEncoder{
		compression: compressionCodec,
		encoderType: thrift.Encoding_PLAIN,
		encoder:     encoding.NewPlainEncoder(),
	}
	encoder.addPage()
	return encoder
}

func (e *defaultPageEncoder) mempool() io.Writer {
	return new(bytes.Buffer)
}

func (e *defaultPageEncoder) addPage() error {
	if e.currentWriter != nil {

		// create DataPage
		b := e.mempool()

		page := newDataPage()
		err := e.currentWriter.Flush()
		if err != nil {
			return err
		}

		compressed, err := e.compress(e.buffer.Bytes())
		if err != nil {
			return err
		}

		uncompressedSize := e.buffer.Len()
		compressedSize := len(compressed)

		page.header.DataPageHeader.Encoding = e.encoderType
		page.header.UncompressedPageSize = int32(uncompressedSize)
		page.header.CompressedPageSize = int32(compressedSize)

		// Write header
		_, err = page.header.Write(b)

		if err != nil {
			return fmt.Errorf("could not write data page header to buffer:%s", err)
		}
		// Write repetition levels

		// Write definition levels

		// Write values
		_, err = io.Copy(b, &e.buffer)
		if err != nil {
			return fmt.Errorf("could not write data page value to buffer:%s", err)
		}

		//TODO: ?page.buffer = b

		e.pages = append(e.pages, page)
	}

	e.currentWriter = bufio.NewWriter(&e.buffer)

	return nil
}

func (e *defaultPageEncoder) compress(p []byte) ([]byte, error) {
	var compressed bytes.Buffer // TODO get from a buffer pool
	switch e.compression {
	case "gzip":
		w := gzip.NewWriter(&compressed)
		if _, err := w.Write(p); err != nil {
			return nil, err
		}
	case "snappy":
		wc := snappy.NewWriter(&compressed)
		if _, err := wc.Write(p); err != nil {
			return nil, err
		}
	case "":
		return p, nil
	default:
		log.Println("defaultPageEncoder: warning unknown compression codec.")
	}

	return compressed.Bytes(), nil
}

// Pages return all the pages written by this encoder
func (e *defaultPageEncoder) Pages() []Page {
	return e.pages
}

func (e *defaultPageEncoder) WriteBool(values []bool) error {
	err := e.encoder.WriteBool(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write bool: %s", err)
	}

	return nil
}

func (e *defaultPageEncoder) WriteInt32(values []int32) error {
	err := e.encoder.WriteInt32(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write int32: %s", err)
	}

	return nil
}

func (e *defaultPageEncoder) WriteInt64(values []int64) error {
	err := e.encoder.WriteInt64(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write int64: %s", err)
	}

	return nil
}

func (e *defaultPageEncoder) WriteFloat32(values []float32) error {
	err := e.encoder.WriteFloat32(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write float32: %s", err)
	}

	return nil
}

func (e *defaultPageEncoder) WriteFloat64(values []float64) error {
	err := e.encoder.WriteFloat64(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write float64: %s", err)
	}

	return nil
}

func (e *defaultPageEncoder) WriteByteArray(values [][]byte) error {
	err := e.encoder.WriteByteArray(e.currentWriter, values)
	if err != nil {
		return fmt.Errorf("defaultPageEncoder: could not write byteArray: %s", err)
	}

	return nil
}
