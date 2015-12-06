package parquet

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
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

	log.Println(s.meta.GetPathInSchema(), "header: ", header)

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
	case parquetformat.PageType_DICTIONARY_PAGE:
		if !header.IsSetDictionaryPageHeader() {
			panic("unexpected DictionaryPageHeader was not set")
		}

		s.readDictionaryPage(header.DictionaryPageHeader, rb)
	case parquetformat.PageType_DATA_PAGE_V2:

	case parquetformat.PageType_DATA_PAGE:
		if !header.IsSetDataPageHeader() {
			panic("unexpected DataPageHeader was not set")
		}

		s.readDataPage(header.DataPageHeader, rb)
	default:
		panic("parquet.ColumnScanner: unknown PageHeader.PageType")
	}

	if _, err = io.Copy(ioutil.Discard, rb); err != nil {
		return err
	}

	return nil
}

func (s *ColumnScanner) readDictionaryPage(header *parquetformat.DictionaryPageHeader, rb *bufio.Reader) error {
	count := int(header.GetNumValues())
	dictEnc := header.GetEncoding()

	log.Println("\t: dictionary.page ", count, dictEnc)

	switch dictEnc {
	case parquetformat.Encoding_PLAIN_DICTIONARY:
		// read the values encoded as Plain
		d := encoding.NewPlainDecoder(rb, s.meta.GetType(), int(header.GetNumValues()))

		switch s.meta.GetType() {
		case parquetformat.Type_INT32, parquetformat.Type_INT64:
			out := make([]int, 0, count)
			read, err := d.DecodeInt(out)
			if err != nil || read != count {
				panic("unexpected")
			}
		case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
			out := make([]string, 0, count)
			read, err := d.DecodeStr(out)
			if err != nil || read != count {
				panic("unexpected")
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
	count := int(header.GetNumValues())

	log.Println("\t", s.meta.PathInSchema, "data.page.header.num_values:", count)

	// Only levels that are repeated need a Repetition level:
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

	// A required field is always defined and does not need a definition level.
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

	switch header.Encoding {
	case parquetformat.Encoding_BIT_PACKED:
	case parquetformat.Encoding_DELTA_BINARY_PACKED:
	case parquetformat.Encoding_DELTA_BYTE_ARRAY:
	case parquetformat.Encoding_DELTA_LENGTH_BYTE_ARRAY:
	case parquetformat.Encoding_PLAIN:
		d := encoding.NewPlainDecoder(rb, s.meta.GetType(), int(header.NumValues))
		switch s.meta.GetType() {
		case parquetformat.Type_INT32, parquetformat.Type_INT64:
			out := make([]int, 0, count)
			// FIXME there is something at the beginning of the data page. 4 bytes.. ?
			var dummy int32
			err := binary.Read(rb, binary.LittleEndian, &dummy)

			read, err := d.DecodeInt(out)
			if err != nil || read != count {
				panic("unexpected")
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
		log.Println("RLE/PLAIN DICTIONARY ")
		var dummy int32
		err := binary.Read(rb, binary.LittleEndian, &dummy)
		if err != nil {
			panic(err)
		}
		log.Println("plain dictionary:", dummy)

		b, err := rb.ReadByte()
		if err != nil {
			panic(err)
		}

		log.Println("bit decoding: ", int(b))

		if err := rle.NewHybridDecoder(rb, int(b)); err != nil {
			log.Println("err ", err)
		}

	default:
		panic("Not supported type for " + header.GetEncoding().String())
	}
}
