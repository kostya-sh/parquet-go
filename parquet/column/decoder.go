package column

import (
	"fmt"
	"io"

	"os"

	"github.com/kostya-sh/parquet-go/parquet/datatypes"
	"github.com/kostya-sh/parquet-go/parquet/page"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

/*
- BOOLEAN: 1 bit boolean
- INT32: 32 bit signed int
- INT64: 64 bit signed int
- INT96: 96 bit signed int
- FLOAT: IEEE 32-bit floating point values
- DOUBLE: IEEE 64-bit floating point values
- BYTE_ARRAY: arbitrarily long byte arrays
*/

// Scanner implements the logic to de-serialize columns in the parquet format
type Scanner struct {
	rs             io.ReadSeeker // The reader provided by the client.
	schema         *thrift.SchemaElement
	chunks         []*thrift.ColumnChunk
	cursor         int
	totalPagesRead int
	err            error
	currentChunk   *chunk
	//dictionaryLUT  []string // Look Up Table for dictionary encoded column chunks
}

// NewScanner returns a Scanner that reads from r
// and interprets the stream as described in the ColumnChunk parquet format
func NewScanner(rs io.ReadSeeker, schema *thrift.SchemaElement, chunks []*thrift.ColumnChunk) *Scanner {
	return &Scanner{rs: rs, schema: schema, chunks: chunks}
}

// setErr records the first error encountered.
// it will not overwrite the existing error unless is nil or is io.EOF
func (s *Scanner) setErr(err error) {
	if s.err == nil || s.err == io.EOF {
		s.err = err
	}
}

// Err returns the first non io.EOF error encountered while scanning the data inside a rowGroup
func (s *Scanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

// Scan reads an entire column chunk
func (s *Scanner) Scan() bool {

	if s.cursor >= len(s.chunks) {
		return false
	}

	if s.err != nil {
		return false
	}

	meta := s.chunks[s.cursor].MetaData

	offset := meta.GetDataPageOffset()

	if meta.IsSetDictionaryPageOffset() && offset > meta.GetDictionaryPageOffset() {
		offset = meta.GetDictionaryPageOffset()
	}

	if meta.IsSetIndexPageOffset() && offset > meta.GetIndexPageOffset() {
		offset = meta.GetIndexPageOffset()
	}

	_, err := s.rs.Seek(offset, os.SEEK_SET)
	if err != nil {
		s.setErr(err)
		return false
	}

	// substitute the original reader with a limited one to get io.EOF
	r := io.LimitReader(s.rs, meta.TotalCompressedSize)

	pageScanner := page.NewScanner(s.schema, meta.GetCodec(), r)

	currentChunk := new(chunk)

	currentChunk.numValues = meta.GetNumValues()

	for pageScanner.Scan() {
		if page, ok := pageScanner.DataPage(); ok {
			currentChunk.data = append(currentChunk.data, page)
		}
		if index, ok := pageScanner.IndexPage(); ok {
			currentChunk.index = index
		}
		if dictionary, ok := pageScanner.DictionaryPage(); ok {
			currentChunk.dictionary = dictionary
		}
	}

	if err := pageScanner.Err(); err != nil {
		s.setErr(err)
		return false
	}

	// chunk is ready to be decoded
	s.currentChunk = currentChunk

	s.cursor++

	return true
}

type chunk struct {
	numValues  int64
	data       []*page.DataPage
	dictionary *page.DictionaryPage
	index      *page.IndexPage
}

func (c *chunk) Decode(acc datatypes.Accumulator) error {

	for _, dataPage := range c.data {
		if err := dataPage.Decode(c.dictionary, acc); err != nil {
			return fmt.Errorf("dataPage: %s", err)
		}
	}

	return nil
}

// NumValues returns the number of values in the current chunk
func (s *Scanner) NumValues() int64 {
	if s.currentChunk == nil {
		return 0
	}

	return s.currentChunk.numValues
}

// column.Scanner.ReadInt32 returns all the values in the current chunk
func (s *Scanner) Decode(acc datatypes.Accumulator) error {
	if s.currentChunk == nil {
		return fmt.Errorf("no chunk")
	}

	return s.currentChunk.Decode(acc)
}

func (s *Scanner) NewAccumulator() datatypes.Accumulator {
	return datatypes.NewSimpleAccumulator(s.schema.GetType())
}

// func (s *Scanner) Int32() ([]int32, bool) {
// 	alloc := make([]int32, 0, s.NumValues())
// 	ok := s.ReadInt32(alloc)
// 	return alloc, ok
// }

// // ReadInt64
// func (s *Scanner) ReadInt64([]int64) bool {
// 	return true
// }

// // ReadString
// func (s *Scanner) ReadString([]int64) bool {
// 	return true
// }

// func (s *Scanner) Bool() ([]bool, bool) {
// 	return nil, true
// }

// func (s *Scanner) Int64() ([]int64, bool) {
// 	return nil, true
// }

// func (s *Scanner) String() ([]string, bool) {
// 	return nil, true
// }

// // read another page in the column chunk
// func (s *Scanner) nextPage() (err error) {
// 	meta := s.chunks[0].MetaData

// 	r := s.rs

// 	var header thrift.PageHeader
// 	err = header.Read(r)
// 	if err != nil {
// 		if strings.HasSuffix(err.Error(), "EOF") { // FIXME: find a better way to detect io.EOF
// 			return io.EOF
// 		}
// 		return fmt.Errorf("column scanner: could not read chunk header: %s", err)
// 	}

// 	r = io.LimitReader(r, int64(header.CompressedPageSize))

// 	log.Printf("%s %s %#v\n", meta.GetPathInSchema(), "header: ", header)

// 	// handle compressed data
// 	switch meta.Codec {
// 	case thrift.CompressionCodec_GZIP:
// 		r, err = gzip.NewReader(r)
// 		if err != nil {
// 			return err
// 		}
// 	case thrift.CompressionCodec_LZO:
// 		// https://github.com/rasky/go-lzo/blob/master/decompress.go#L149			s.r = r
// 		panic("NYI")

// 	case thrift.CompressionCodec_SNAPPY:
// 		r = snappy.NewReader(r)
// 	case thrift.CompressionCodec_UNCOMPRESSED:
// 		// use the limit reader
// 	}

// 	// this is important so that the decoder use the same ByteReader
// 	rb := bufio.NewReader(r)

// 	switch header.Type {
// 	case thrift.PageType_INDEX_PAGE:
// 		panic("nyi")

// 	case thrift.PageType_DICTIONARY_PAGE:
// 		if !header.IsSetDictionaryPageHeader() {
// 			panic("unexpected DictionaryPageHeader was not set")
// 		}

// 		s.readDictionaryPage(header.DictionaryPageHeader, rb)
// 	case thrift.PageType_DATA_PAGE_V2:
// 		panic("nyi")

// 	case thrift.PageType_DATA_PAGE:
// 		if !header.IsSetDataPageHeader() {
// 			panic("unexpected DataPageHeader was not set")
// 		}

// 		/* header, rb =*/ s.readDataPage(header.DataPageHeader, rb)
// 	default:
// 		panic("parquet.Scanner: unknown PageHeader.PageType")
// 	}

// 	if n, err := io.Copy(ioutil.Discard, rb); err != nil {
// 		return err
// 	} else if n > 0 {
// 		panic("not all the data was consumed.")
// 	}

// 	return nil
// }

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"io/ioutil"

// 	"github.com/kostya-sh/parquet-go/thrift"
// )

// // TODO: add other commons methods
// type ColumnChunkReader interface {
// 	Next() bool
// 	NextPage() bool
// 	Levels() Levels
// 	Err() error
// 	Value() interface{}
// }

// // TODO: shorter name?
// type BooleanColumnChunkReader struct {
// 	// TODO: consider using a separate reader for each column chunk (no seeking)
// 	r *countingReader

// 	// initialized once
// 	maxLevels Levels
// 	totalSize int64

// 	// changing state
// 	err            error
// 	curLevels      Levels
// 	curValue       bool
// 	pageValuesRead int
// 	atStartOfPage  bool
// 	atLastPage     bool
// 	valuesRead     int64
// 	dataPageOffset int64
// 	header         *thrift.PageHeader

// 	// decoders
// 	decoder  *booleanPlainDecoder
// 	dDecoder *rle32Decoder
// 	rDecoder *rle32Decoder
// }

// func NewBooleanColumnChunkReader(r io.ReadSeeker, cs *ColumnSchema, chunk *thrift.ColumnChunk) (*BooleanColumnChunkReader, error) {
// 	if chunk.FilePath != nil {
// 		return nil, fmt.Errorf("data in another file: '%s'", *chunk.FilePath)
// 	}

// 	// chunk.FileOffset is useless
// 	// see https://issues.apache.org/jira/browse/PARQUET-291
// 	meta := chunk.MetaData
// 	if meta == nil {
// 		return nil, fmt.Errorf("missing ColumnMetaData")
// 	}

// 	if meta.Type != thrift.Type_BOOLEAN {
// 		return nil, fmt.Errorf("wrong type, expected BOOLEAN was %s", meta.Type)
// 	}

// 	schemaElement := cs.SchemaElement
// 	if schemaElement.RepetitionType == nil {
// 		return nil, fmt.Errorf("nil RepetitionType (root SchemaElement?)")
// 	}

// 	// uncompress
// 	if meta.Codec != thrift.CompressionCodec_UNCOMPRESSED {
// 		return nil, fmt.Errorf("unsupported compression codec: %s", meta.Codec)
// 	}

// 	// only REQUIRED non-nested columns are supported for now
// 	// so definitionLevel = 1 and repetitionLevel = 1
// 	cr := BooleanColumnChunkReader{
// 		r:              &countingReader{rs: r},
// 		totalSize:      meta.TotalCompressedSize,
// 		dataPageOffset: meta.DataPageOffset,
// 		maxLevels:      cs.MaxLevels,
// 		decoder:        newBooleanPlainDecoder(),
// 	}

// 	if *schemaElement.RepetitionType == thrift.FieldRepetitionType_REQUIRED {
// 		// TODO: also check that len(Path) = maxD
// 		// For data that is required, the definition levels are not encoded and
// 		// always have the value of the max definition level.
// 		cr.curLevels.D = cr.maxLevels.D
// 		// TODO: document level ranges
// 	} else {
// 		cr.dDecoder = newRLE32Decoder(bitWidth(cr.maxLevels.D))
// 	}
// 	if cr.curLevels.D == 0 && *schemaElement.RepetitionType != thrift.FieldRepetitionType_REPEATED {
// 		// TODO: I think we need to check all schemaElements in the path
// 		cr.curLevels.R = 0
// 		// TODO: clarify the following comment from parquet-format/README:
// 		// If the column is not nested the repetition levels are not encoded and
// 		// always have the value of 1
// 	} else {
// 		cr.rDecoder = newRLE32Decoder(bitWidth(cr.maxLevels.R))
// 	}

// 	return &cr, nil
// }

// // AtStartOfPage returns true if the reader is positioned at the first value of a page.
// func (cr *BooleanColumnChunkReader) AtStartOfPage() bool {
// 	return cr.atStartOfPage
// }

// // PageHeader returns page header of the current page.
// func (cr *BooleanColumnChunkReader) PageHeader() *thrift.PageHeader {
// 	return cr.header
// }

// func (cr *BooleanColumnChunkReader) readDataPage() ([]byte, error) {
// 	var err error
// 	n := cr.r.n

// 	if _, err := cr.r.rs.Seek(cr.dataPageOffset, 0); err != nil {
// 		return nil, err
// 	}
// 	ph := thrift.PageHeader{}
// 	if err := ph.Read(cr.r); err != nil {
// 		return nil, err
// 	}
// 	if ph.Type != thrift.PageType_DATA_PAGE {
// 		return nil, fmt.Errorf("DATA_PAGE type expected, but was %s", ph.Type)
// 	}
// 	dph := ph.DataPageHeader
// 	if dph == nil {
// 		return nil, fmt.Errorf("null DataPageHeader in %+v", ph)
// 	}
// 	if dph.Encoding != thrift.Encoding_PLAIN {
// 		return nil, fmt.Errorf("unsupported encoding %s for BOOLEAN type", dph.Encoding)
// 	}

// 	size := int64(ph.CompressedPageSize)
// 	data, err := ioutil.ReadAll(io.LimitReader(cr.r, size))
// 	if err != nil {
// 		return nil, err
// 	}
// 	if int64(len(data)) != size {
// 		return nil, fmt.Errorf("unable to read page fully: got %d bytes, expected %d", len(data), size)
// 	}
// 	if cr.r.n > cr.totalSize {
// 		return nil, fmt.Errorf("over-read")
// 	}

// 	cr.header = &ph
// 	if cr.r.n == cr.totalSize {
// 		cr.atLastPage = true
// 	}
// 	cr.dataPageOffset += (cr.r.n - n)

// 	return data, nil
// }

// // Next advances the reader to the next value, which then will be available
// // through Boolean() method. It returns false when the reading stops, either by
// // reaching the end of the input or an error.
// func (cr *BooleanColumnChunkReader) Next() bool {
// 	if cr.err != nil {
// 		return false
// 	}

// 	if cr.decoder.data == nil {
// 		if cr.atLastPage {
// 			// TODO: may be set error if trying to read past the end of the column chunk
// 			return false
// 		}

// 		// read next page
// 		data, err := cr.readDataPage()
// 		if err != nil {
// 			// TODO: handle EOF
// 			cr.err = err
// 			return false
// 		}
// 		//fmt.Printf("%v\n", data)
// 		start := 0
// 		// TODO: it looks like thrift README is incorrect
// 		// first R then D
// 		if cr.rDecoder != nil {
// 			// decode definition levels data
// 			// TODO: uint32 -> int overflow
// 			// TODO: error handing
// 			n := int(binary.LittleEndian.Uint32(data[:4]))
// 			cr.rDecoder.init(data[4 : n+4])
// 			start = n + 4
// 		}
// 		if cr.dDecoder != nil {
// 			// decode repetition levels data
// 			// TODO: uint32 -> int overflow
// 			// TODO: error handing
// 			n := int(binary.LittleEndian.Uint32(data[start : start+4]))
// 			cr.dDecoder.init(data[start+4 : start+n+4])
// 			start += n + 4
// 		}
// 		cr.decoder.init(data[start:])
// 		cr.atStartOfPage = true
// 	} else {
// 		cr.atStartOfPage = false
// 	}

// 	pageNumValues := int(cr.header.DataPageHeader.NumValues)

// 	// TODO: hasNext and error checking
// 	if cr.dDecoder != nil {
// 		d, _ := cr.dDecoder.next()
// 		cr.curLevels.D = int(d)
// 	}
// 	if cr.rDecoder != nil {
// 		r, _ := cr.rDecoder.next()
// 		cr.curLevels.R = int(r)
// 	}
// 	if cr.curLevels.D == cr.maxLevels.D {
// 		cr.curValue, cr.err = cr.decoder.next()
// 		if cr.err != nil {
// 			return false
// 		}
// 	} else {
// 		cr.curValue = false // just to be deterministic
// 	}

// 	cr.valuesRead++
// 	cr.pageValuesRead++
// 	if cr.pageValuesRead >= pageNumValues {
// 		cr.pageValuesRead = 0
// 		cr.decoder.data = nil
// 	}

// 	return true
// }

// // Skip behaves like Next but it may skip decoding the current value.
// // TODO: think about use case and implement if necessary
// func (cr *BooleanColumnChunkReader) Skip() bool {
// 	panic("nyi")
// }

// // NextPage advances the reader to the first value of the next page. It behaves as Next.
// // TODO: think about how this method can be used to skip unreadable/corrupted pages
// func (cr *BooleanColumnChunkReader) NextPage() bool {
// 	if cr.err != nil {
// 		return false
// 	}
// 	cr.atStartOfPage = true
// 	// TODO: implement
// 	return false
// }

// // Levels returns repetition and definition levels of the most recent value
// // generated by a call to Next or NextPage.
// func (cr *BooleanColumnChunkReader) Levels() Levels {
// 	return cr.curLevels
// }

// // Boolean returns the most recent value generated by a call to Next or NextPage.
// func (cr *BooleanColumnChunkReader) Boolean() bool {
// 	return cr.curValue
// }

// // Value returns Boolean()
// func (cr *BooleanColumnChunkReader) Value() interface{} {
// 	return cr.Boolean()
// }

// // Err returns the first non-EOF error that was encountered by the reader.
// func (cr *BooleanColumnChunkReader) Err() error {
// 	return cr.err
// }
