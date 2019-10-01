package parquet

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"reflect"

	"github.com/golang/snappy"
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
	dictPage       *parquetformat.PageHeader
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

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}
	cr := &ColumnChunkReader{
		col:       col,
		reader:    &countingReader{rs: r, offset: offset},
		meta:      meta,
		chunkMeta: chunk.MetaData,
	}

	if col.maxD == 0 {
		// TODO: also check that len(Path) = maxD
		// For data that is required, the definition levels are not encoded and
		// always have the value of the max definition level.
		cr.dDecoder = constDecoder(0)
		// TODO: document level ranges
	} else {
		cr.dDecoder = newRLEDecoder(bits.Len16(col.maxD))
	}
	if col.maxR == 0 {
		// TODO: I think we need to check all schemaElements in the path (confirm if above)
		cr.rDecoder = constDecoder(0)
		// TODO: clarify the following comment from parquet-format/README:
		// If the column is not nested the repetition levels are not encoded and
		// always have the value of 1
	} else {
		cr.rDecoder = newRLEDecoder(bits.Len16(col.maxR))
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
		case parquetformat.Encoding_RLE:
			return &booleanRLEDecoder{}, nil
		}

	case parquetformat.Type_BYTE_ARRAY:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayPlainDecoder{}, nil
		case parquetformat.Encoding_DELTA_LENGTH_BYTE_ARRAY:
			return &byteArrayDeltaLengthDecoder{}, nil
		case parquetformat.Encoding_DELTA_BYTE_ARRAY:
			return &byteArrayDeltaDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayPlainDecoder{length: int(*cr.col.schemaElement.TypeLength)}, nil
		case parquetformat.Encoding_DELTA_BYTE_ARRAY:
			return &byteArrayDeltaDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
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
		case parquetformat.Encoding_DELTA_BINARY_PACKED:
			return &int32DeltaBinaryPackedDecoder{}, nil
		case parquetformat.Encoding_RLE_DICTIONARY:
			return cr.dictValuesDecoder, nil
		}

	case parquetformat.Type_INT64:
		switch pageEncoding {
		case parquetformat.Encoding_PLAIN:
			return &int64PlainDecoder{}, nil
		case parquetformat.Encoding_DELTA_BINARY_PACKED:
			return &int64DeltaBinaryPackedDecoder{}, nil
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
	case parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
		switch dictEncoding {
		case parquetformat.Encoding_PLAIN:
			return &byteArrayDictDecoder{
				dictDecoder: dictDecoder{vd: &byteArrayPlainDecoder{length: int(*cr.col.schemaElement.TypeLength)}},
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

func (cr *ColumnChunkReader) readPageData(compressedSize int32, uncompressedSize int32) (data []byte, err error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}
	r := io.LimitReader(cr.reader, int64(compressedSize))

	codec := cr.chunkMeta.Codec
	switch codec {
	case parquetformat.CompressionCodec_SNAPPY:
		// decompress after reading:
		// parquet uses snappy block encoding (snappy.Reader is for streaming encoing)
	case parquetformat.CompressionCodec_UNCOMPRESSED:
		// do nothing
	case parquetformat.CompressionCodec_GZIP:
		r, err = gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}

	data, err = ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if cr.reader.n > cr.chunkMeta.TotalCompressedSize {
		return nil, errors.New("over-read")
	}
	if codec == parquetformat.CompressionCodec_SNAPPY {
		data, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, err
		}
	}
	if len(data) != int(uncompressedSize) {
		return nil, errors.New("page data after uncompression is incomplete")
	}
	return data, nil
}

func (cr *ColumnChunkReader) readPageDataV1(ph *parquetformat.PageHeader, dph *parquetformat.DataPageHeader) (valuesData, dData, rData []byte, err error) {
	data, err := cr.readPageData(ph.CompressedPageSize, ph.UncompressedPageSize)
	if err != nil {
		return nil, nil, nil, err
	}

	// TODO: it looks like parquetformat README is incorrect: first R then D
	if _, isConst := cr.rDecoder.(constDecoder); !isConst {
		if enc := dph.RepetitionLevelEncoding; enc != parquetformat.Encoding_RLE {
			return nil, nil, nil, fmt.Errorf("%s RepetitionLevelEncoding is not supported", enc)
		}
		if len(data) < 4 {
			return nil, nil, nil, errors.New("not enough data to read repetition levels")
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 0 || uint(n+4) > uint(len(data)) {
			return nil, nil, nil, errors.New("invalid repetition levels data length")
		}
		rData = data[4 : 4+n]
		data = data[4+n:]
	}
	if _, isConst := cr.dDecoder.(constDecoder); !isConst {
		if enc := dph.DefinitionLevelEncoding; enc != parquetformat.Encoding_RLE {
			return nil, nil, nil, fmt.Errorf("%s DefinitionLevelEncoding is not supported", enc)
		}
		if len(data) < 4 {
			return nil, nil, nil, errors.New("not enough data to read definition levels")
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 0 || uint(n+4) > uint(len(data)) {
			return nil, nil, nil, errors.New("invalid definition levels data length")
		}
		dData = data[4 : 4+n]
		data = data[4+n:]
	}

	return data, dData, rData, nil
}

func (cr *ColumnChunkReader) readPageDataV2(ph *parquetformat.PageHeader, dph *parquetformat.DataPageHeaderV2) (valuesData, dData, rData []byte, err error) {
	if dph.RepetitionLevelsByteLength < 0 {
		return nil, nil, nil, fmt.Errorf("invalid RepetitionLevelsByteLength")
	}
	if dph.DefinitionLevelsByteLength < 0 {
		return nil, nil, nil, fmt.Errorf("invalid DefinitionLevelsByteLength")
	}

	levelsSize := dph.RepetitionLevelsByteLength + dph.DefinitionLevelsByteLength
	r := io.LimitReader(cr.reader, int64(levelsSize))
	levelsData, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(levelsData) != int(levelsSize) {
		return nil, nil, nil, errors.New("unable to read all levels data")
	}
	if _, isConst := cr.rDecoder.(constDecoder); !isConst {
		n := int(dph.RepetitionLevelsByteLength)
		rData = levelsData[:n]
		levelsData = levelsData[n:]
	} else {
		if dph.RepetitionLevelsByteLength != 0 {
			return nil, nil, nil, fmt.Errorf("RepetitionLevelsByteLength != 0 for column with no r levels")
		}
	}
	if _, isConst := cr.dDecoder.(constDecoder); !isConst {
		dData = levelsData
	} else {
		if dph.DefinitionLevelsByteLength != 0 {
			return nil, nil, nil, fmt.Errorf("DefinitionLevelsByteLength != 0 for column with no r levels")
		}
	}

	valuesData, err = cr.readPageData(ph.CompressedPageSize-levelsSize, ph.UncompressedPageSize-levelsSize)
	if err != nil {
		return nil, nil, nil, err
	}

	return valuesData, dData, rData, nil
}

func (cr *ColumnChunkReader) readPage(first bool) error {
	if _, err := cr.reader.SeekToOffset(); err != nil {
		return err
	}

	ph := &parquetformat.PageHeader{}
	if err := ph.Read(cr.reader); err != nil {
		return err
	}

	if first && ph.Type == parquetformat.PageType_DICTIONARY_PAGE {
		cr.dictPage = ph

		dph := ph.DictionaryPageHeader
		if dph == nil {
			return fmt.Errorf("null DictionaryPageHeader in %+v", ph)
		}
		if count := dph.NumValues; count < 0 {
			return fmt.Errorf("negative NumValues in DICTIONARY_PAGE: %d", count)
		}

		dictData, err := cr.readPageData(ph.CompressedPageSize, ph.UncompressedPageSize)
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

		if cr.chunkMeta.DictionaryPageOffset != nil {
			cr.reader.offset = cr.chunkMeta.DataPageOffset
			if _, err = cr.reader.SeekToOffset(); err != nil {
				return err
			}
		}
		ph = &parquetformat.PageHeader{}
		if err = ph.Read(cr.reader); err != nil {
			return err
		}
	}

	var (
		numValues      int
		valuesEncoding parquetformat.Encoding
		dph            *parquetformat.DataPageHeader
		dph2           *parquetformat.DataPageHeaderV2
	)
	switch ph.Type {
	case parquetformat.PageType_DATA_PAGE:
		dph = ph.DataPageHeader
		if dph == nil {
			return fmt.Errorf("missing both DataPageHeader and DataPageHeaderV2 in %+v", ph)
		}
		numValues = int(dph.NumValues)
		valuesEncoding = dph.Encoding
	case parquetformat.PageType_DATA_PAGE_V2:
		dph2 = ph.DataPageHeaderV2
		if dph2 == nil {
			return fmt.Errorf("missing both DataPageHeader and DataPageHeaderV2 in %+v", ph)
		}
		numValues = int(dph2.NumValues)
		valuesEncoding = dph2.Encoding

	default:
		return fmt.Errorf("DATA_PAGE or DATA_PAGE_V2 type expected, but was %s", ph.Type)
	}

	if numValues < 0 {
		return fmt.Errorf("negative page NumValues")
	}

	switch valuesEncoding {
	case parquetformat.Encoding_PLAIN_DICTIONARY, parquetformat.Encoding_RLE_DICTIONARY:
		if cr.dictValuesDecoder == nil {
			return fmt.Errorf("No DICTIONARY_PAGE for %s encoding", valuesEncoding)
		}
	}

	var err error
	cr.valuesDecoder, err = cr.newValuesDecoder(valuesEncoding)
	if err != nil {
		return err
	}

	var valuesData, dData, rData []byte
	if dph != nil {
		valuesData, dData, rData, err = cr.readPageDataV1(ph, dph)
	} else {
		valuesData, dData, rData, err = cr.readPageDataV2(ph, dph2)
	}
	if err != nil {
		return err
	}

	if dData != nil {
		cr.dDecoder.init(dData)
	}
	if rData != nil {
		cr.rDecoder.init(rData)
	}
	if err := cr.valuesDecoder.init(valuesData); err != nil {
		return err
	}

	cr.page = ph
	cr.readPageValues = 0
	cr.pageNumValues = numValues

	return nil
}

// Read reads up to len(dLevels) values into values and corresponding definition
// and repetition levels into dLevels and rLevels respectfully. Panics if
// len(dLevels) != len(rLevels) != len(values). It returns the number of values
// read (including nulls) and any errors encountered.
//
// Note that after Read values slice contains only non-null values. Number of
// these values could be less than n.
//
// values must be a slice of interface{} or type that corresponds to the column
// type (such as []int32 for INT32 column or [][]byte for BYTE_ARRAY column).
//
// When there is not enough values in the current page to fill dLevels Read
// doesn't advance to the next page and returns the number of values read.  If
// this page was the last page in its column chunk and there is no more data to
// read it returns EndOfChunk error.
func (cr *ColumnChunkReader) Read(values interface{}, dLevels []uint16, rLevels []uint16) (n int, err error) {
	if lv := reflect.ValueOf(values).Len(); lv != len(dLevels) || lv != len(rLevels) {
		panic("incorrect arguments (len)")
	}

	if cr.err != nil {
		return 0, cr.err
	}

	// read levels
	batchSize := len(dLevels)
	if rem := cr.pageNumValues - cr.readPageValues; rem < batchSize {
		batchSize = rem
	}
	if err := cr.dDecoder.decodeLevels(dLevels[:batchSize]); err != nil {
		return n, fmt.Errorf("failed to read definition levels: %s", err)
	}
	if err := cr.rDecoder.decodeLevels(rLevels[:batchSize]); err != nil {
		return n, fmt.Errorf("failed to read repetition levels: %s", err)
	}

	// read values
	nn := 0 // number of non-null values
	for _, ld := range dLevels[:batchSize] {
		if ld == cr.col.MaxD() {
			nn++
		}
	}
	if nn != 0 {
		err = cr.valuesDecoder.decode(reflect.ValueOf(values).Slice(0, nn).Interface())
		if err != nil {
			return n, fmt.Errorf("failed to read values: %s", err)
		}
	}

	// advance to the next page if necessary
	cr.readPageValues += batchSize
	if cr.readPageValues > cr.pageNumValues {
		panic("something wrong (read to many values)")
	}
	if cr.readPageValues == cr.pageNumValues {
		// skipping a page at the end is the same as reading the next one
		// ignore the returned error as it will be remembered in cr.err
		// and returned on the next call to Read()
		_ = cr.SkipPage()
	}

	return batchSize, nil
}

// SkipPage positions cr at the beginning of the next page skipping all values
// in the current page.
//
// Returns EndOfChunk if no more data available
func (cr *ColumnChunkReader) SkipPage() error {
	if cr.err != nil {
		return cr.err
	}
	if cr.reader.n == cr.chunkMeta.TotalCompressedSize { // TODO: maybe use chunkMeta.NumValues
		cr.err = EndOfChunk
	} else {
		// TODO: read data lazily only if Read is called
		cr.err = cr.readPage(false)
	}
	if cr.err != nil {
		cr.page = nil
	}
	return cr.err
}

// PageHeader returns PageHeader of a page that is about to be read or
// currently being read.
//
// If there was an error reading the last page (including EndOfChunk) PageHeder
// returns nil.
func (cr *ColumnChunkReader) PageHeader() *parquetformat.PageHeader {
	return cr.page
}

// DictionaryPageHeader returns a DICTIONARY_PAGE page header if the column
// chunk has one or nil otherwise.
func (cr *ColumnChunkReader) DictionaryPageHeader() *parquetformat.PageHeader {
	return cr.dictPage
}

type constDecoder uint16

func (d constDecoder) init(_ []byte) {
}

func (d constDecoder) decodeLevels(dst []uint16) error {
	for i := 0; i < len(dst); i++ {
		dst[i] = uint16(d)
	}
	return nil
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
