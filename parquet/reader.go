package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

var (
	EndOfChunk = errors.New("EndOfChunk")
)

// ColumnChunkReader allows to read data from a single column chunk of a parquet
// file.
type ColumnChunkReader struct {
	col Column

	reader *countingReader
	meta   *parquetformat.FileMetaData

	err            error
	chunkMeta      *parquetformat.ColumnMetaData
	page           *parquetformat.PageHeader
	readPageValues int
	pageNumValues  int

	valuesDecoder     valuesDecoder
	dictValuesDecoder dictValuesDecoder
	dDecoder          levelsDecoder
	rDecoder          levelsDecoder
}

func newColumnChunkReader(r io.ReadSeeker, meta *parquetformat.FileMetaData, col Column, chunk *parquetformat.ColumnChunk) (*ColumnChunkReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("nyi: data is in another file: '%s'", *chunk.FilePath)
	}

	c := col.Index()
	// chunk.FileOffset is useless so ChunkMetaData is required here
	// as we cannot read it from r
	// see https://issues.apache.org/jira/browse/PARQUET-291
	if chunk.MetaData == nil {
		return nil, fmt.Errorf("missing meta data for column %c", c)
	}

	if typ := *col.schemaElement.Type; chunk.MetaData.Type != typ {
		return nil, fmt.Errorf("wrong type in column chunk metadata, expected %s was %s",
			typ, chunk.MetaData.Type)
	}

	// TODO: support more codecs
	if codec := chunk.MetaData.Codec; codec != parquetformat.CompressionCodec_UNCOMPRESSED {
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}

	cr := &ColumnChunkReader{
		col:       col,
		reader:    &countingReader{rs: r, offset: chunk.MetaData.DataPageOffset},
		meta:      meta,
		chunkMeta: chunk.MetaData,
	}

	nested := strings.IndexByte(col.name, '.') >= 0
	repType := *col.schemaElement.RepetitionType
	if !nested && repType == parquetformat.FieldRepetitionType_REQUIRED {
		// TODO: also check that len(Path) = maxD
		// For data that is required, the definition levels are not encoded and
		// always have the value of the max definition level.
		cr.dDecoder = &constDecoder{value: col.maxD}
		// TODO: document level ranges
	} else {
		cr.dDecoder = newRLE32Decoder(bitWidth(col.maxD))
	}
	if !nested && repType != parquetformat.FieldRepetitionType_REPEATED {
		// TODO: I think we need to check all schemaElements in the path (confirm if above)
		cr.rDecoder = &constDecoder{value: 0}
		// TODO: clarify the following comment from parquet-format/README:
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
	} else {
		cr.rDecoder = newRLE32Decoder(bitWidth(col.maxR))
	}

	cr.err = cr.readPage(true)

	return cr, nil
}

func (cr *ColumnChunkReader) newValuesDecoder(pageEncoding parquetformat.Encoding) (valuesDecoder, error) {
	if pageEncoding == parquetformat.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquetformat.Encoding_RLE_DICTIONARY
	}

	typ := *cr.col.schemaElement.Type
	switch typ {
	case parquetformat.Type_BOOLEAN:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &booleanPlainDecoder{}, nil
		}

	case parquetformat.Type_BYTE_ARRAY:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayPlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayPlainDecoder{length: int(*cr.col.schemaElement.TypeLength)}, nil
		}

	case parquetformat.Type_FLOAT:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &floatPlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_DOUBLE:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &doublePlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_INT32:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int32PlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_INT64:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int64PlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_INT96:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int96PlainDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	default:
		return nil, fmt.Errorf("unsupported type: %s", typ)
	}

	return nil, fmt.Errorf("unsupported encoding %s for %s type", pageEncoding, typ)
}

func (cr *ColumnChunkReader) newDictValuesDecoder(dictEncoding parquetformat.Encoding) (dictValuesDecoder, error) {
	if dictEncoding == parquetformat.Encoding_PLAIN_DICTIONARY {
		dictEncoding = parquetformat.Encoding_PLAIN
	}

	typ := *cr.col.schemaElement.Type
	switch typ {
	case parquetformat.Type_BYTE_ARRAY:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayDictDecoder{
				dictDecoder: dictDecoder{vd: &byteArrayPlainDecoder{}},
			}, nil
		}

	case parquetformat.Type_FLOAT:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &floatDictDecoder{
				dictDecoder: dictDecoder{vd: &floatPlainDecoder{}},
			}, nil
		}

	case parquetformat.Type_DOUBLE:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &doubleDictDecoder{
				dictDecoder: dictDecoder{vd: &doublePlainDecoder{}},
			}, nil
		}

	case parquetformat.Type_INT32:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int32DictDecoder{
				dictDecoder: dictDecoder{vd: &int32PlainDecoder{}},
			}, nil
		}

	case parquetformat.Type_INT64:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int64DictDecoder{
				dictDecoder: dictDecoder{vd: &int64PlainDecoder{}},
			}, nil
		}

	case parquetformat.Type_INT96:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int96DictDecoder{
				dictDecoder: dictDecoder{vd: &int96PlainDecoder{}},
			}, nil
		}

	default:
		return nil, fmt.Errorf("type %s doesn't support dictionary encoding", typ)
	}

	return nil, fmt.Errorf("unsupported encoding for %s dictionary page: %s", typ, dictEncoding)
}

func (cr *ColumnChunkReader) readPageData(ph parquetformat.PageHeader) ([]byte, error) {
	size := int64(ph.CompressedPageSize)
	data, err := ioutil.ReadAll(io.LimitReader(cr.reader, size))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, fmt.Errorf("unable to read page fully: got %d bytes, expected %d", len(data), size)
	}
	if cr.reader.n > cr.chunkMeta.TotalUncompressedSize {
		return nil, fmt.Errorf("over-read")
	}
	return data, nil
}

func (cr *ColumnChunkReader) readPage(first bool) error {
	if _, err := cr.reader.SeekToOffset(); err != nil {
		return err
	}

	ph := parquetformat.PageHeader{}
	if err := ph.Read(cr.reader); err != nil {
		return err
	}

	if first && ph.Type == parquetformat.PageType_DICTIONARY_PAGE {
		dph := ph.DictionaryPageHeader
		if dph == nil {
			return fmt.Errorf("null DictionaryPageHeader in %+v", ph)
		}
		if count := dph.NumValues; count <= 0 {
			return fmt.Errorf("non-positive NumValues in DICTIONARY_PAGE: %d", count)
		}

		dictData, err := cr.readPageData(ph)
		if err != nil {
			return err
		}

		d, err := cr.newDictValuesDecoder(dph.Encoding)
		if err != nil {
			return err
		}
		if err := d.initValues(dictData, int(dph.NumValues)); err != nil {
			return err
		}
		cr.dictValuesDecoder = d

		if err = ph.Read(cr.reader); err != nil {
			return err
		}
	}

	if ph.Type != parquetformat.PageType_DATA_PAGE {
		return fmt.Errorf("DATA_PAGE type expected, but was %s", ph.Type)
	}
	dph := ph.DataPageHeader
	if dph == nil {
		return fmt.Errorf("null DataPageHeader in %+v", ph)
	}
	count := int(dph.NumValues)

	switch dph.Encoding {
	case parquetformat.Encoding_PLAIN_DICTIONARY, parquetformat.Encoding_RLE_DICTIONARY:
		if cr.dictValuesDecoder == nil {
			return fmt.Errorf("No DICTIONARY_PAGE for %s encoding", dph.Encoding)
		}
	}

	var err error
	cr.valuesDecoder, err = cr.newValuesDecoder(dph.Encoding)
	if err != nil {
		return err
	}

	data, err := cr.readPageData(ph)
	if err != nil {
		return err
	}

	pos := 0
	// TODO: it looks like parquetformat README is incorrect: first R then D
	if _, isConst := cr.rDecoder.(*constDecoder); !isConst {
		// decode definition levels data
		// TODO: uint32 -> int overflow
		// TODO: error handing
		n := int(binary.LittleEndian.Uint32(data[:4]))
		pos += 4
		cr.rDecoder.init(data[pos:pos+n], count)
		pos += n
	} else {
		cr.rDecoder.init(nil, count)
	}
	if _, isConst := cr.dDecoder.(*constDecoder); !isConst {
		// decode repetition levels data
		// TODO: uint32 -> int overflow
		// TODO: error handing
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		cr.dDecoder.init(data[pos:pos+n], count)
		pos += n
	} else {
		cr.dDecoder.init(nil, count)
	}
	if err := cr.valuesDecoder.init(data[pos:], count); err != nil {
		return err
	}

	cr.page = &ph
	cr.readPageValues = 0
	cr.pageNumValues = int(dph.NumValues)

	return nil
}

// Read reads up to len(dLevels) values into values and corresponding definition
// and repetition levels into d and r respectfully. Panics if len(dLevels) !=
// len(rLevels). It returns the number of values read and any errors
// encountered.
//
// Note that after Read values contain only non-null values that could be less
// than n.
//
// When there is not enough values in the current page to fill values Read
// doesn't advance to the next page and returns the number of values read.  If
// this page was last page in its column chunk it returns EndOfColumnChunk
// error.
func (cr *ColumnChunkReader) Read(values interface{}, dLevels []int, rLevels []int) (n int, err error) {
	if lv := reflect.ValueOf(values).Len(); lv != len(dLevels) || lv != len(rLevels) {
		panic("incorrect arguments (len)")
	}

	if cr.err != nil {
		return 0, cr.err
	}

	// read levels
	nd, err := cr.dDecoder.decode(dLevels)
	if err != nil {
		return n, fmt.Errorf("failed to read definition levels: %s", err)
	}
	nr, err := cr.rDecoder.decode(rLevels)
	if err != nil {
		return n, fmt.Errorf("failed to read repetition levels: %s", err)
	}
	if nd != nr {
		return n, fmt.Errorf("counts mismatch, #d = %d, #r = %d",
			nd, nr)
	}
	n = nd

	// read values
	nn := 0
	for _, ld := range dLevels {
		if ld == cr.col.MaxD() {
			nn++
		}
	}
	_, err = cr.valuesDecoder.decode(reflect.ValueOf(values).Slice(0, nn).Interface())
	if err != nil {
		return n, fmt.Errorf("failed to read values: %s", err)
	}

	// advance to the next page if necessary
	cr.readPageValues += n
	if cr.readPageValues > cr.pageNumValues {
		panic("something wrong (read to many values)")
	}
	if cr.readPageValues == cr.pageNumValues {
		// skipping a page at the end is the same as reading the next one
		cr.SkipPage()
	}

	return n, nil
}

// SkipPage positions cr at the beginning of the next page skipping all values
// in the current page.
//
// Returns EndOfChunk if no more data available
func (cr *ColumnChunkReader) SkipPage() error {
	if cr.reader.n == cr.chunkMeta.TotalUncompressedSize { // TODO: maybe use chunkMeta.NumValues
		cr.err = EndOfChunk
		cr.page = nil
	} else {
		// TODO: read data lazily only if Read is called
		cr.err = cr.readPage(false)
	}
	return cr.err
}

// PageHeader returns PageHeader of a page that is about to be read or
// currently being read.
func (cr *ColumnChunkReader) PageHeader() *parquetformat.PageHeader {
	return cr.page
}

type constDecoder struct {
	value int
	count int
	i     int
}

func (d *constDecoder) init(_ []byte, count int) {
	d.count = count
	d.i = 0
}

func (d *constDecoder) decode(levels []int) (n int, err error) {
	n = len(levels)
	if d.count-d.i < n {
		n = d.count - d.i
	}
	for i := 0; i < n; i++ {
		levels[i] = d.value
	}
	d.i += n
	return n, nil
}

type countingReader struct {
	rs     io.ReadSeeker
	n      int64
	offset int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	r.offset += int64(n)
	return
}

func (r *countingReader) SeekToOffset() (n int64, err error) {
	return r.rs.Seek(r.offset, io.SeekStart)
}
