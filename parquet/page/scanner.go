package page

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// Scanner
type Scanner interface {
	Scan() bool
	DataPage() (DataPage, bool)
	DictionaryPage() (DictionaryPage, bool)
	IndexPage() (IndexPage, bool)
	Err() error
}

type scanner struct {
	r          io.Reader
	dictionary DictionaryPage
	dataPage   DataPage
	indexPage  IndexPage
}

func NewScanner(r io.Reader, codec thrift.CompressionCodec) Scanner {
	return &scanner{r: r, codec: codec}
}

// read another page in the column chunk
func (s *scanner) Scan() (err error) {
	var (
		header thrift.PageHeader
		codec  thrift.CompressionCodec
	)

	s.dictionary = nil
	s.dataPage = nil
	s.indexPage = nil

	err = header.Read(s.r)
	if err != nil {
		if strings.HasSuffix(err.Error(), "EOF") { // FIXME: find a better way to detect io.EOF
			return io.EOF
		}
		return fmt.Errorf("column scanner: could not read chunk header: %s", err)
	}

	// setup reader
	r := io.LimitReader(s.r, int64(header.CompressedPageSize))
	r, err := compressionReader(r, s.codec)
	if err != nil {
		s.setErr(err)
		return false
	}
	// this is important so that the decoder use the same ByteReader
	rb := bufio.NewReader(r)

	// read the page
	page, err := s.readPage(rb)
	if err != nil {
		s.setErr(err)
		return false
	}

	s.page = page

	// check if we consumed all the data from the limit reader as a safe guard
	if n, err := io.Copy(ioutil.Discard, rb); err != nil {
		s.setErr(err)
		return false
	} else if n > 0 {
		err := fmt.Errorf("not all the data was consumed.")
		s.setErr(err)
		return false
	}

	return true
}

//
func compressionReader(r io.Reader, codec thrift.CompressionCodec) (io.Reader, error) {
	switch codec {
	case thrift.CompressionCodec_GZIP:
		r, err = gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("could not create gzip reader:%s", err)
		}
	case thrift.CompressionCodec_LZO:
		// https://github.com/rasky/go-lzo/blob/master/decompress.go#L149			s.r = r
		return nil, fmt.Errorf("NYI")

	case thrift.CompressionCodec_SNAPPY:
		r = snappy.NewReader(r)
		return r, nil
	case thrift.CompressionCodec_UNCOMPRESSED:
		// use the same reader
		return r, nil
	default:
		return nil, fmt.Errorf("unknown compression format %s", codec)
	}
}

// readPage
func (s *scanner) readPage() (Page, error) {
	t := s.header.Type

	switch t {

	case thrift.PageType_INDEX_PAGE:
		return nil, fmt.Errorf("WARNING IndexPage not yet implemented")

	case thrift.PageType_DICTIONARY_PAGE:
		if !s.header.IsSetDictionaryPageHeader() {
			return nil, fmt.Errorf("bad file format:DictionaryPageHeader flag was not set")
		}
		return s.readDictionaryPage()

	case thrift.PageType_DATA_PAGE_V2:
		panic("nyi")

	case thrift.PageType_DATA_PAGE:
		if !header.IsSetDataPageHeader() {
			return nil, fmt.Errorf("bad file format: DataPageHeader flag was not set")
		}
		return s.readDataPage()

	default:
		return nil, fmt.Errorf("unknown PageHeader.PageType: %s", t)
	}

	return nil
}

func (s *scanner) DataPage() (DataPage, bool) {
	return s.page, true
}

func (s *scanner) DictionaryPage() (DictionaryPage, bool) {
	return s.page, true
}

func (s *scanner) IndexPage() (IndexPage, bool) {
	return s.page, true
}

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

// Read a dictionary page. There is only one dictionary page for each column chunk
func (s *scanner) readDictionaryPage() (*DictionaryPage, error) {

	count := int(s.header.GetNumValues())
	dictEnc := s.header.GetEncoding()

	switch dictEnc {
	case thrift.Encoding_PLAIN_DICTIONARY:
		t := meta.GetType()
		count := int(header.GetNumValues())
		decoder := encoding.NewPlainDecoder(s.r, t, count)
		page := NewDictionaryPage()

		switch meta.GetType() {

		case thrift.Type_INT32:
			read, err := d.DecodeInt32(page.valuesInt32)
			if err != nil || read != count {
				panic("unexpected")
			}
		case thrift.Type_INT64:
			read, err := d.DecodeInt64(page.valuesInt64)
			if err != nil || read != count {
				panic("unexpected")
			}
		case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
			read, err := d.DecodeStr(page.valuesString)
			if err != nil || read != count {
				panic("unexpected")
			}
		case thrift.Type_DOUBLE:
		case thrift.Type_FLOAT:
		case thrift.Type_INT96:
		default:
			panic("dictionary encoding " + dictEnc.String() + "not yet supported") // FIXME
		}

		return nil, nil
	}
}
