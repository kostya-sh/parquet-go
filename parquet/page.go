package parquet

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

type DataEncoder interface {
	NumValues() int
	Encoding() parquetformat.Encoding
	Bytes() []byte
}

type dataPage struct {
	header *parquetformat.PageHeader
}

func (dp *dataPage) Type() parquetformat.PageType {
	return parquetformat.PageType_DATA_PAGE
}

// encoder should provide this
func newDataPage() *dataPage {

	datapage := parquetformat.NewDataPageHeader()
	datapage.NumValues = 0
	datapage.Encoding = parquetformat.Encoding_PLAIN
	datapage.DefinitionLevelEncoding = parquetformat.Encoding_RLE
	datapage.RepetitionLevelEncoding = parquetformat.Encoding_BIT_PACKED
	datapage.Statistics = nil /** Optional statistics for the data in this page**/

	header := parquetformat.NewPageHeader()
	header.Type = parquetformat.PageType_DATA_PAGE
	header.DataPageHeader = datapage
	header.CompressedPageSize = 0
	header.UncompressedPageSize = 0

	return &dataPage{header}
}

type Page interface {
	Type() parquetformat.PageType
}

type PageScanner interface {
	Scan() bool
	Page() Page
}

// PageEncoder encodes a stream of values into a set of pages
type PageEncoder interface {
	Pages() []Page
}

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
	compression   string
}

func newDefaultPageEncoder(compressionCodec string) *defaultPageEncoder {
	encoder := &defaultPageEncoder{
		compression: compressionCodec,
	}
	encoder.addPage()
	return encoder
}

func (e *defaultPageEncoder) addPage() {
	if e.currentWriter != nil {
		page := newDataPage()
		e.pages = append(e.pages, page)
	}
	e.currentWriter = bufio.NewWriter(&e.buffer)
}

func (e *defaultPageEncoder) Pages() []Page {

	return []Page{}
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

// // Page Encoders by type
// type boolPageEncoder struct {
// }

// type int32PageEncoder struct {
// }

// type int64PageEncoder struct {
// }

// type int96PageEncoder struct {
// }

// type floatPageEncoder struct {
// }

// type doublePageEncoder struct {
// }

// type byteArrayPageEncoder struct {
// }

// type fixedLenByteArrayPageEncoder struct {
// }
