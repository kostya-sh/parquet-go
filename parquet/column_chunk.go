package parquet

import (
	"io"

	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"

	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/encoding/bitpacking"
	"github.com/kostya-sh/parquet-go/encoding/rle"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

// Create a DataPage of Type
// Compress it
// Fill stats
// Write it to the file and record the set.
// Plain Encoder needs only data pages
//  WriteInt()

// you can only have one dictionary page per each column chunk
// you can have multiple data pages

type ColumnEncoder struct {
	Schema *parquetformat.SchemaElement
}

func NewColumnEncoder(schema *parquetformat.SchemaElement) *ColumnEncoder {
	return &ColumnEncoder{Schema: schema}
}

func (e *ColumnEncoder) WriteChunk(w io.Writer, offset int, name string) (int, error) {
	return 0, nil
}

/*
- BOOLEAN: 1 bit boolean
- INT32: 32 bit signed int
- INT64: 64 bit signed int
- INT96: 96 bit signed int
- FLOAT: IEEE 32-bit floating point values
- DOUBLE: IEEE 64-bit floating point values
- BYTE_ARRAY: arbitrarily long byte arrays
*/

var Config = struct {
	Debug bool
}{
	Debug: true,
}

// ColumnScanner implements the logic to deserialize columns in the parquet format
type ColumnScanner struct {
	rs             io.ReadSeeker // The reader provided by the client.
	r              io.Reader
	chunk          *parquetformat.ColumnChunk
	meta           *parquetformat.ColumnMetaData
	schema         *parquetformat.SchemaElement
	dictionaryLUT  []string // Look Up Table for dictionary encoded column chunks
	totalPagesRead int
	err            error
}

// NewColumnScanner returns a ColumnScanner that reads from r
// and interprets the stream as described in the ColumnChunk parquet format
func NewColumnScanner(rs io.ReadSeeker, chunk *parquetformat.ColumnChunk, schema *parquetformat.SchemaElement) *ColumnScanner {
	return &ColumnScanner{rs: rs, r: nil, chunk: chunk, meta: chunk.MetaData, schema: schema}
}

// setErr records the first error encountered.
// it will not overwrite the existing error unless is nil or is io.EOF
func (s *ColumnScanner) setErr(err error) {
	if s.err == nil || s.err == io.EOF {
		s.err = err
	}
}

func (s *ColumnScanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

func (s *ColumnScanner) Scan() bool {
	log.Println(s.meta.GetPathInSchema(), s.meta)

	if s.totalPagesRead == 0 {
		columnStart := s.meta.GetDataPageOffset()

		if s.meta.IsSetDictionaryPageOffset() {
			if columnStart > s.meta.GetDictionaryPageOffset() {
				columnStart = s.meta.GetDictionaryPageOffset()
			}
		}

		_, err := s.rs.Seek(columnStart, os.SEEK_SET)
		if err != nil {
			s.setErr(err)
			return false
		}

		// substitute the original reader with a limited one to get io.EOF
		// when the chunk is read
		s.r = io.LimitReader(s.rs, s.meta.TotalCompressedSize)
	}

	for {
		if err := s.nextPage(); err != nil {
			s.setErr(err)
			if err == io.EOF {
				log.Printf("columnScanner: %s (%s): total pages read: %d", s.meta.GetPathInSchema(), s.meta.Type, s.totalPagesRead)
			}
			return false
		}

		s.totalPagesRead++
	}

	log.Printf("columnScanner: %s (%s): total pages read: %d", s.meta.GetPathInSchema(), s.meta.Type, s.totalPagesRead)
	return true
}

func (s *ColumnScanner) nextPage() (err error) {

	r := s.r

	var header parquetformat.PageHeader
	err = header.Read(r)
	if err != nil {
		if strings.HasSuffix(err.Error(), "EOF") { // FIXME: find a better way to detect io.EOF
			return io.EOF
		}
		return fmt.Errorf("column scanner: could not read chunk header: %s", err)
	}

	r = io.LimitReader(r, int64(header.CompressedPageSize))

	log.Printf("%s %s %#v\n", s.meta.GetPathInSchema(), "header: ", header)

	// handle compressed data
	switch s.meta.Codec {
	case parquetformat.CompressionCodec_GZIP:
		r, err = gzip.NewReader(r)
		if err != nil {
			return err
		}
	case parquetformat.CompressionCodec_LZO:
		// https://github.com/rasky/go-lzo/blob/master/decompress.go#L149			s.r = r
		panic("NYI")

	case parquetformat.CompressionCodec_SNAPPY:
		r = snappy.NewReader(r)
	case parquetformat.CompressionCodec_UNCOMPRESSED:
		// use the limit reader
	}

	// this is important so that the decoder use the same ByteReader
	rb := bufio.NewReader(r)

	switch header.Type {
	case parquetformat.PageType_INDEX_PAGE:
		panic("nyi")

	case parquetformat.PageType_DICTIONARY_PAGE:
		if !header.IsSetDictionaryPageHeader() {
			panic("unexpected DictionaryPageHeader was not set")
		}

		s.readDictionaryPage(header.DictionaryPageHeader, rb)
	case parquetformat.PageType_DATA_PAGE_V2:
		panic("nyi")

	case parquetformat.PageType_DATA_PAGE:
		if !header.IsSetDataPageHeader() {
			panic("unexpected DataPageHeader was not set")
		}

		s.readDataPage(header.DataPageHeader, rb)
	default:
		panic("parquet.ColumnScanner: unknown PageHeader.PageType")
	}

	if n, err := io.Copy(ioutil.Discard, rb); err != nil {
		return err
	} else if n > 0 {
		panic("not all the data was consumed.")
	}

	return nil
}

func (s *ColumnScanner) readDictionaryPage(header *parquetformat.DictionaryPageHeader, rb *bufio.Reader) error {
	count := int(header.GetNumValues())
	dictEnc := header.GetEncoding()

	log.Println(s.meta.PathInSchema, ": dictionary.page ", count, dictEnc)

	switch dictEnc {
	case parquetformat.Encoding_PLAIN_DICTIONARY:
		// read the values encoded as Plain
		d := encoding.NewPlainDecoder(rb, s.meta.GetType(), int(header.GetNumValues()))

		switch s.meta.GetType() {
		case parquetformat.Type_INT32:
			out := make([]int32, 0, count)
			read, err := d.DecodeInt32(out)
			if err != nil || read != count {
				panic("unexpected")
			}

			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}

		case parquetformat.Type_INT64:
			out := make([]int64, 0, count)
			read, err := d.DecodeInt64(out)
			if err != nil || read != count {
				panic("unexpected")
			}

			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}
		case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
			out := make([]string, 0, count)
			read, err := d.DecodeStr(out)
			if err != nil || read != count {
				panic("unexpected")
			}

			for idx, value := range out {
				log.Printf("%d %s", idx, value)
			}

		case parquetformat.Type_INT96:
			log.Println("Warning: skipping not supported type int96 in plain encoding dictionary")
			return nil
		default:

		}
	default:
		panic("dictionary encoding " + dictEnc.String() + "not yet supported") // FIXME
	}

	return nil
}

func (s *ColumnScanner) readDataPage(header *parquetformat.DataPageHeader, rb *bufio.Reader) {
	log.Printf("%s %v\n", s.meta.PathInSchema, header)

	count := int(header.GetNumValues())

	log.Println(s.meta.PathInSchema, "data.page.header.num_values:", count)

	// only levels that are repeated need a Repetition level:
	// optional or required fields are never repeated
	// and can be skipped while attributing repetition levels.
	if s.schema.GetRepetitionType() == parquetformat.FieldRepetitionType_REPEATED {
		repEnc := header.GetRepetitionLevelEncoding()
		switch repEnc {

		case parquetformat.Encoding_BIT_PACKED:
			dec := bitpacking.NewDecoder(rb, 1) // FIXME 1 ?
			for dec.Scan() {
				log.Println("repetition level decoding:", dec.Value())
			}

			if err := dec.Err(); err != nil {
				log.Println(err)
			}
		default:
			log.Println("WARNING could not handle %s", repEnc)
		}
	}

	// a required field is always defined and does not need a definition level.
	if s.schema.GetRepetitionType() != parquetformat.FieldRepetitionType_REQUIRED {
		defEnc := header.GetDefinitionLevelEncoding()
		switch defEnc {
		case parquetformat.Encoding_RLE:
			dec := rle.NewDecoder(rb)

			for dec.Scan() {
				log.Println("definition level decoding:", dec.Value())
			}

			if err := dec.Err(); err != nil {
				log.Println(err)
			}

		default:
			log.Println("WARNING could not handle %s", defEnc)
		}
	}

	// FIXME there is something at the beginning of the data page. 4 bytes.. ?
	var dummy int32
	err := binary.Read(rb, binary.LittleEndian, &dummy)
	if err != nil {
		panic(err)
	}

	switch header.Encoding {
	case parquetformat.Encoding_BIT_PACKED:
	case parquetformat.Encoding_DELTA_BINARY_PACKED:
	case parquetformat.Encoding_DELTA_BYTE_ARRAY:
	case parquetformat.Encoding_DELTA_LENGTH_BYTE_ARRAY:
	case parquetformat.Encoding_PLAIN:
		d := encoding.NewPlainDecoder(rb, s.meta.GetType(), int(header.NumValues))
		switch s.meta.GetType() {

		case parquetformat.Type_INT32:
			out := make([]int32, 0, count)
			read, err := d.DecodeInt32(out)
			if err != nil || read != count {
				panic("unexpected")
			}
			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}

		case parquetformat.Type_INT64:
			out := make([]int64, 0, count)

			read, err := d.DecodeInt64(out)
			if err != nil || read != count {
				panic("unexpected")
			}
			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}

		case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
			s.dictionaryLUT = make([]string, 0, count)
			read, err := d.DecodeStr(s.dictionaryLUT)
			if err != nil || read != count {
				panic("unexpected")
			}

		case parquetformat.Type_INT96:
			panic("not supported type int96")
		default:
		}
	case parquetformat.Encoding_RLE:

	case parquetformat.Encoding_RLE_DICTIONARY:
		fallthrough
	case parquetformat.Encoding_PLAIN_DICTIONARY:
		b, err := rb.ReadByte()
		if err != nil {
			panic(err)
		}

		dec := rle.NewHybridBitPackingRLEDecoder(rb, int(b))

		for dec.Scan() {
			log.Println(s.meta.GetPathInSchema(), dec.Value())
		}

		if err := dec.Err(); err != nil {
			panic(fmt.Errorf("%s: plain_dictionary: %s", s.meta.GetPathInSchema(), err))
		}

	default:
		panic("Not supported type for " + header.GetEncoding().String())
	}
}

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"io/ioutil"

// 	"github.com/kostya-sh/parquet-go/parquetformat"
// )

// // TODO: add other commons methods
// type ColumnChunkReader interface {
// 	Next() bool
// 	NextPage() bool
// 	Levels() Levels
// 	Err() error
// 	Value() interface{}
// }

// type countingReader struct {
// 	rs io.ReadSeeker
// 	n  int64
// }

// func (r *countingReader) Read(p []byte) (n int, err error) {
// 	n, err = r.rs.Read(p)
// 	r.n += int64(n)
// 	return
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
// 	header         *parquetformat.PageHeader

// 	// decoders
// 	decoder  *booleanPlainDecoder
// 	dDecoder *rle32Decoder
// 	rDecoder *rle32Decoder
// }

// func NewBooleanColumnChunkReader(r io.ReadSeeker, cs *ColumnSchema, chunk *parquetformat.ColumnChunk) (*BooleanColumnChunkReader, error) {
// 	if chunk.FilePath != nil {
// 		return nil, fmt.Errorf("data in another file: '%s'", *chunk.FilePath)
// 	}

// 	// chunk.FileOffset is useless
// 	// see https://issues.apache.org/jira/browse/PARQUET-291
// 	meta := chunk.MetaData
// 	if meta == nil {
// 		return nil, fmt.Errorf("missing ColumnMetaData")
// 	}

// 	if meta.Type != parquetformat.Type_BOOLEAN {
// 		return nil, fmt.Errorf("wrong type, expected BOOLEAN was %s", meta.Type)
// 	}

// 	schemaElement := cs.SchemaElement
// 	if schemaElement.RepetitionType == nil {
// 		return nil, fmt.Errorf("nil RepetitionType (root SchemaElement?)")
// 	}

// 	// uncompress
// 	if meta.Codec != parquetformat.CompressionCodec_UNCOMPRESSED {
// 		return nil, fmt.Errorf("unsupported compression codec: %s", meta.Codec)
// 	}

// 	// only REQUIRED non-neseted columns are supported for now
// 	// so definitionLevel = 1 and repetitionLevel = 1
// 	cr := BooleanColumnChunkReader{
// 		r:              &countingReader{rs: r},
// 		totalSize:      meta.TotalCompressedSize,
// 		dataPageOffset: meta.DataPageOffset,
// 		maxLevels:      cs.MaxLevels,
// 		decoder:        newBooleanPlainDecoder(),
// 	}

// 	if *schemaElement.RepetitionType == parquetformat.FieldRepetitionType_REQUIRED {
// 		// TODO: also check that len(Path) = maxD
// 		// For data that is required, the definition levels are not encoded and
// 		// always have the value of the max definition level.
// 		cr.curLevels.D = cr.maxLevels.D
// 		// TODO: document level ranges
// 	} else {
// 		cr.dDecoder = newRLE32Decoder(bitWidth(cr.maxLevels.D))
// 	}
// 	if cr.curLevels.D == 0 && *schemaElement.RepetitionType != parquetformat.FieldRepetitionType_REPEATED {
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
// func (cr *BooleanColumnChunkReader) PageHeader() *parquetformat.PageHeader {
// 	return cr.header
// }

// func (cr *BooleanColumnChunkReader) readDataPage() ([]byte, error) {
// 	var err error
// 	n := cr.r.n

// 	if _, err := cr.r.rs.Seek(cr.dataPageOffset, 0); err != nil {
// 		return nil, err
// 	}
// 	ph := parquetformat.PageHeader{}
// 	if err := ph.Read(cr.r); err != nil {
// 		return nil, err
// 	}
// 	if ph.Type != parquetformat.PageType_DATA_PAGE {
// 		return nil, fmt.Errorf("DATA_PAGE type expected, but was %s", ph.Type)
// 	}
// 	dph := ph.DataPageHeader
// 	if dph == nil {
// 		return nil, fmt.Errorf("null DataPageHeader in %+v", ph)
// 	}
// 	if dph.Encoding != parquetformat.Encoding_PLAIN {
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
// 		// TODO: it looks like parquetformat README is incorrect
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
