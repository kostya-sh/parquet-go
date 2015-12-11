package parquet

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// TODO: add other commons methods
type ColumnChunkReader interface {
	Next() bool
	NextPage() bool
	Err() error
	// TODO: use smaller type, maybe byte?
	R() int
	// TODO: use smaller type, maybe byte?
	D() int
	Value() interface{}
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

// TODO: shorter name?
type BooleanColumnChunkReader struct {
	// TODO: consider using a separate reader for each column chunk (no seeking)
	r *countingReader

	// initialized once
	fixedR    int
	fixedD    int
	totalSize int64

	// changing state
	err            error
	atStartOfPage  bool
	atLastPage     bool
	valuesRead     int64
	dataPageOffset int64
	header         *parquetformat.PageHeader

	// decoder
	decoder booleanPlainDecoder
}

func NewBooleanColumnChunkReader(r io.ReadSeeker, schema Schema, chunk *parquetformat.ColumnChunk) (*BooleanColumnChunkReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("data in another file: '%s'", *chunk.FilePath)
	}

	// chunk.FileOffset is useless
	// see https://issues.apache.org/jira/browse/PARQUET-291
	meta := chunk.MetaData
	if meta == nil {
		return nil, fmt.Errorf("missing ColumnMetaData")
	}

	if meta.Type != parquetformat.Type_BOOLEAN {
		return nil, fmt.Errorf("wrong type, expected BOOLEAN was %s", meta.Type)
	}

	schemaElement := schema.element(meta.PathInSchema)
	if schemaElement.RepetitionType == nil {
		return nil, fmt.Errorf("nil RepetitionType (root SchemaElement?)")
	}

	// uncompress
	if meta.Codec != parquetformat.CompressionCodec_UNCOMPRESSED {
		return nil, fmt.Errorf("unsupported compression codec: %s", meta.Codec)
	}

	// only REQUIRED non-neseted columns are supported for now
	// so definitionLevel = 1 and repetitionLevel = 1
	cr := BooleanColumnChunkReader{
		r:              &countingReader{rs: r},
		totalSize:      meta.TotalCompressedSize,
		dataPageOffset: meta.DataPageOffset,
	}

	if *schemaElement.RepetitionType == parquetformat.FieldRepetitionType_REQUIRED {
		// For data that is required, the definition levels are not encoded and
		// always have the value of the max definition level.
		cr.fixedD, _ = schema.maxLevels(meta.PathInSchema)
	} else {
		panic("nyi")
	}
	if len(meta.PathInSchema) == 1 {
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
		cr.fixedR = 1
	} else {
		panic("nyi")
	}

	return &cr, nil
}

// AtStartOfPage returns true if the reader is positioned at the first value of a page.
func (cr *BooleanColumnChunkReader) AtStartOfPage() bool {
	return cr.atStartOfPage
}

// PageHeader returns page header of the current page.
func (cr *BooleanColumnChunkReader) PageHeader() *parquetformat.PageHeader {
	return cr.header
}

func (cr *BooleanColumnChunkReader) readDataPage() ([]byte, error) {
	var err error

	if _, err := cr.r.rs.Seek(cr.dataPageOffset, 0); err != nil {
		return nil, err
	}
	ph := parquetformat.PageHeader{}
	if err := ph.Read(cr.r); err != nil {
		return nil, err
	}
	if ph.Type != parquetformat.PageType_DATA_PAGE {
		return nil, fmt.Errorf("DATA_PAGE type expected, but was %s", ph.Type)
	}
	dph := ph.DataPageHeader
	if dph == nil {
		return nil, fmt.Errorf("null DataPageHeader in %+v", ph)
	}
	if dph.Encoding != parquetformat.Encoding_PLAIN {
		return nil, fmt.Errorf("unsupported encoding %s for BOOLEAN type", dph.Encoding)
	}

	size := int64(ph.CompressedPageSize)
	data, err := ioutil.ReadAll(io.LimitReader(cr.r, size))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, fmt.Errorf("unable to read page fully: got %d bytes, expected %d", len(data), size)
	}
	if cr.r.n > cr.totalSize {
		return nil, fmt.Errorf("over-read")
	}

	cr.header = &ph
	if cr.r.n == cr.totalSize {
		cr.atLastPage = true
	}
	cr.dataPageOffset += size

	return data, nil
}

// Next advances the reader to the next value, which then will be available
// through Boolean() method. It returns false when the reading stops, either by
// reaching the end of the input or an error.
func (cr *BooleanColumnChunkReader) Next() bool {
	if cr.err != nil {
		return false
	}

	done := cr.decoder.data != nil && cr.decoder.next()
	if cr.decoder.err != nil {
		cr.err = cr.decoder.err
		return false
	}

	if !done {
		if cr.atLastPage {
			// TODO: may be set error if trying to read past the end of the column chunk
			return false
		}

		data, err := cr.readDataPage()
		if err != nil {
			cr.err = err
			return false
		}
		cr.decoder.init(data, cr.header.DataPageHeader.NumValues)
		cr.atStartOfPage = true

		done = cr.decoder.next()
		if cr.decoder.err != nil {
			cr.err = cr.decoder.err
			return false
		}
	} else {
		cr.atStartOfPage = false
	}

	cr.valuesRead++
	return done
}

// Skip behaves like Next but it may skip decoding the current value.
// TODO: think about use case and implement if necessary
func (cr *BooleanColumnChunkReader) Skip() bool {
	panic("nyi")
}

// NextPage advances the reader to the first value of the next page. It behaves as Next.
// TODO: think about how this method can be used to skip unreadable/corrupted pages
func (cr *BooleanColumnChunkReader) NextPage() bool {
	if cr.err != nil {
		return false
	}
	cr.atStartOfPage = true
	// TODO: implement
	return false
}

// R returns repetition level of the most recent value generated by a call to Next or NextPage.
func (cr *BooleanColumnChunkReader) R() int {
	if cr.fixedR >= 0 {
		return cr.fixedR
	}
	panic("nyi")
}

// D returns definition level of the most recent value generated by a call to Next or NextPage.
func (cr *BooleanColumnChunkReader) D() int {
	if cr.fixedD >= 0 {
		return cr.fixedD
	}
	panic("nyi")
}

// Boolean returns the most recent value generated by a call to Next or NextPage.
func (cr *BooleanColumnChunkReader) Boolean() bool {
	return cr.decoder.value
}

// Value returns Boolean()
func (cr *BooleanColumnChunkReader) Value() interface{} {
	return cr.Boolean()
}

// Err returns the first non-EOF error that was encountered by the reader.
func (cr *BooleanColumnChunkReader) Err() error {
	return cr.err
}
