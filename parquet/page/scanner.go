package page

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

// Scanner scans through pages inside a single column chunk
type Scanner interface {
	Scan() bool
	DataPage() (*DataPage, bool)
	DictionaryPage() (*DictionaryPage, bool)
	IndexPage() (*IndexPage, bool)
	Err() error
}

type scanner struct {
	schema     *thrift.SchemaElement
	r          io.Reader
	dictionary *DictionaryPage
	dataPage   *DataPage
	indexPage  *IndexPage
	codec      thrift.CompressionCodec
	err        error
	totalRead  int
}

func NewScanner(schema *thrift.SchemaElement, codec thrift.CompressionCodec, r io.Reader) Scanner {
	return &scanner{schema: schema, r: r, codec: codec}
}

// Scan reads the next page inside the column chunk. returns false if no more data pages
// are present or if an error occurred.
func (s *scanner) Scan() bool {
	var (
		header thrift.PageHeader
	)

	if s.err != nil {
		return false
	}

	s.dictionary = nil
	s.dataPage = nil
	s.indexPage = nil

	err := header.Read(s.r)
	if err != nil {
		if strings.HasSuffix(err.Error(), "EOF") { // FIXME: find a better way to detect io.EOF
			s.setErr(io.EOF)
			return false
		}
		s.setErr(fmt.Errorf("column scanner: could not read chunk header: %s", err))
		return false
	}

	// setup reader
	r := io.LimitReader(s.r, int64(header.CompressedPageSize))
	r, err = s.compressionReader(r, &header)
	if err != nil {
		s.setErr(err)
		return false
	}

	// read the page
	if err := s.readPage(r, &header); err != nil {
		s.setErr(err)
		return false
	}

	// check if we consumed all the data from the limit reader as a safe guard
	if n, err := io.Copy(ioutil.Discard, r); err != nil {
		if err == io.EOF {
			return true
		}
		s.setErr(err)
		return false
	} else if n > 0 {
		err := fmt.Errorf("not all the data was consumed for page %s", header.GetType())
		s.setErr(err)
		return false
	}

	return true
}

// returns a reader for the right compression
func (s *scanner) compressionReader(r io.Reader, header *thrift.PageHeader) (io.Reader, error) {
	switch s.codec {
	case thrift.CompressionCodec_GZIP:
		r, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("could not create gzip reader:%s", err)
		}
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("could not read gzip reader:%s", err)
		}
		if err := r.Close(); err != nil {
			log.Println("WARNING error closing gzip reader:%s", err)
		}
		return bytes.NewReader(b), nil

	case thrift.CompressionCodec_LZO:
		// https://github.com/rasky/go-lzo/blob/master/decompress.go#L149			s.r = r
		return nil, fmt.Errorf("NYI")

	case thrift.CompressionCodec_SNAPPY:
		src, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("could not create gzip reader:%s", err)
		}

		out := make([]byte, int(header.GetUncompressedPageSize()))
		out, err = snappy.Decode(out, src)
		if err != nil {
			return nil, fmt.Errorf("could not create gzip reader:%s", err)
		}

		return bytes.NewReader(out), nil

	case thrift.CompressionCodec_UNCOMPRESSED:
		// use the same reader
		return r, nil

	default:
		return nil, fmt.Errorf("unknown compression format %s", s.codec)
	}
}

func (s *scanner) readPage(r io.Reader, header *thrift.PageHeader) error {

	switch header.GetType() {

	case thrift.PageType_INDEX_PAGE:
		if !header.IsSetIndexPageHeader() {
			return nil
		}

		s.indexPage = NewIndexPage(header.GetIndexPageHeader())
		// TODO read indexPage
		return nil

	case thrift.PageType_DICTIONARY_PAGE:
		if !header.IsSetDictionaryPageHeader() {
			return fmt.Errorf("bad file format:DictionaryPageHeader flag was not set")
		}
		dictHeader := header.GetDictionaryPageHeader()
		s.dictionary = NewDictionaryPage(s.schema, dictHeader)
		return s.dictionary.Decode(r)

	case thrift.PageType_DATA_PAGE_V2:
		panic("nyi")

	case thrift.PageType_DATA_PAGE:
		s.totalRead += int(header.GetDataPageHeader().GetNumValues())

		log.Println("num values in page:", header.GetDataPageHeader().GetNumValues(), "totalRead:", s.totalRead)

		if !header.IsSetDataPageHeader() {
			return fmt.Errorf("bad file format: DataPageHeader flag was not set")
		}
		s.dataPage = NewDataPage(s.schema, header.GetDataPageHeader())
		return s.dataPage.ReadAll(r)

	default:
		return fmt.Errorf("unknown PageHeader.PageType: %s", header.GetType())
	}
}

func (s *scanner) DataPage() (*DataPage, bool) {
	return s.dataPage, s.dataPage != nil
}

func (s *scanner) DictionaryPage() (*DictionaryPage, bool) {
	return s.dictionary, s.dictionary != nil
}

func (s *scanner) IndexPage() (*IndexPage, bool) {
	return s.indexPage, s.indexPage != nil
}

func (s *scanner) setErr(err error) {
	if s.err == nil || s.err == io.EOF {
		s.err = err
	}
}

// Err returns the first non io.EOF error encountered while scanning the data inside a rowGroup
func (s *scanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}
