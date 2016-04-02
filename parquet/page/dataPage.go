package page

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
	"github.com/kostya-sh/parquet-go/parquet/encoding/rle"
	"github.com/kostya-sh/parquet-go/parquet/memory"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

//DataPage represents one data page inside a column chunk
type DataPage struct {
	schema              *thrift.SchemaElement
	header              *thrift.DataPageHeader
	meta                *thrift.ColumnMetaData
	maxDefinitionLevels uint32
	rb                  *bufio.Reader
}

// NewDataPage
func NewDataPage(schema *thrift.SchemaElement, header *thrift.DataPageHeader) *DataPage {
	return &DataPage{schema: schema, header: header}
}

func (p *DataPage) ReadAll(r io.Reader) error {
	// r = dump(r)
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	r = bytes.NewReader(b)
	p.rb = bufio.NewReader(r)
	return nil
}

func min(a, b uint) int {
	if a < b {
		return int(b)
	}
	return int(a)
}

func (p *DataPage) readDefinitionAndRepetitionLevels(rb *bufio.Reader) (repetition []uint64, defintion []uint64, err error) {

	// Repetition Levels
	// only levels that are repeated need a Repetition level:
	// optional or required fields are never repeated
	// and can be skipped while attributing repetition levels.
	if p.schema.GetRepetitionType() == thrift.FieldRepetitionType_REPEATED {
		repEnc := p.header.GetRepetitionLevelEncoding()
		switch repEnc {
		case thrift.Encoding_BIT_PACKED:
			dec := bitpacking.NewDecoder(1)
			runs, err := dec.ReadLength(rb)
			if err != nil {
				return nil, nil, fmt.Errorf("bitpacking.ReadLength:%s", err)
			}
			out := make([]int32, min(uint(p.header.GetNumValues()), runs*8))
			if err := dec.Read(rb, out); err != nil {
				return nil, nil, fmt.Errorf("bitpacking cannot read:%s", err)
			}
		// 	result := make([]int32, 0, int(runs*8))
		// finish:
		// 	for i := 0; i < int(runs); i++ {
		// 		if err := dec.Read(rb, out); err != nil {
		// 			return nil, nil, fmt.Errorf("bitpacking cannot read after %d blocks:%s", i, err)
		// 		}

		// 		for j := 0; j < 8; j++ {
		// 			if len(result)+1 > int(p.header.GetNumValues()) {
		// 				break finish
		// 			}
		// 			result = append(result, out[j])
		// 		}
		// 	}

		default:
			return nil, nil, fmt.Errorf("WARNING could not handle %s", repEnc)
		}
	}

	// Definition Levels
	// For data that is required, the definition levels are skipped.
	// If encoded, it will always have the value of the max definition level.
	if p.schema.GetRepetitionType() != thrift.FieldRepetitionType_REQUIRED {
		defEnc := p.header.GetDefinitionLevelEncoding()
		switch defEnc {
		case thrift.Encoding_RLE:
			p.maxDefinitionLevels = 0
			// length of the <encoded-data> in bytes stored as 4 bytes little endian
			var length uint32

			if err := binary.Read(rb, binary.LittleEndian, &length); err != nil {
				return nil, nil, err
			}

			lr := io.LimitReader(rb, int64(length))

			_, err := rle.ReadUint64(lr, 1, uint(p.header.GetNumValues()))
			if err != nil {
				return nil, nil, err
			}

			if n, _ := io.Copy(ioutil.Discard, lr); n > 0 {
				log.Println("WARNING not all data was consumed in RLE encoder")
			}

		default:
			return nil, nil, fmt.Errorf("WARNING could not handle %s", defEnc)
		}
	}

	return []uint64{}, []uint64{}, nil
}

func (p *DataPage) createDecoder(rb *bufio.Reader, page *DictionaryPage) (encoding.Decoder, error) {

	numValues := uint(p.header.NumValues)

	switch p.header.Encoding {
	case thrift.Encoding_BIT_PACKED:
	case thrift.Encoding_DELTA_BINARY_PACKED:
	case thrift.Encoding_DELTA_BYTE_ARRAY:
	case thrift.Encoding_DELTA_LENGTH_BYTE_ARRAY:
	case thrift.Encoding_PLAIN:
		return encoding.NewPlainDecoder(rb, numValues), nil
	case thrift.Encoding_RLE:

	case thrift.Encoding_RLE_DICTIONARY:
		fallthrough
	case thrift.Encoding_PLAIN_DICTIONARY:
		if page == nil {
			return nil, fmt.Errorf("data page in dictionary page format but no dictionary was defined")
		}
		return encoding.NewPlainDictionaryDecoder(rb, page, numValues), nil
	default:
		panic("Not supported type for " + p.header.GetEncoding().String())
	}

	panic("NYI")
}

func (p *DataPage) Decode(page *DictionaryPage, accumulator memory.Accumulator) error {

	p.readDefinitionAndRepetitionLevels(p.rb)
	d, err := p.createDecoder(p.rb, page)
	if err != nil {
		return fmt.Errorf("could not create decoder: %s", err)
	}
	return accumulator.Accumulate(d, uint(p.header.GetNumValues()))
}

// // Decode using the given reader
// func (p *DataPage) Decode(rb *bufio.Reader, page *DictionaryPage) error {
// 	header := p.header

// 	Type := p.schema.GetType()
// 	count := int(header.GetNumValues())

// 	p.readDefinitionAndRepetitionLevels(rb)

// 	// Handle DataPageEncoding
// 	switch header.Encoding {
// 	case thrift.Encoding_BIT_PACKED:

// 	case thrift.Encoding_DELTA_BINARY_PACKED:
// 	case thrift.Encoding_DELTA_BYTE_ARRAY:
// 	case thrift.Encoding_DELTA_LENGTH_BYTE_ARRAY:
// 	case thrift.Encoding_PLAIN:
// 		d := encoding.NewPlainDecoder(rb, Type, int(header.NumValues))
// 		switch Type {

// 		case thrift.Type_INT32:
// 			out := make([]int32, 0, count)
// 			read, err := d.DecodeInt32(out)
// 			if err != nil || read != count {
// 				panic("unexpected")
// 			}

// 			for idx, value := range out {
// 				log.Printf("%d %d", idx, value)
// 			}

// 		case thrift.Type_INT64:
// 			out := make([]int64, 0, count)

// 			read, err := d.DecodeInt64(out)
// 			if err != nil || read != count {
// 				panic("unexpected")
// 			}
// 			for idx, value := range out {
// 				log.Printf("%d %d", idx, value)
// 			}

// 		case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
// 			// s.dictionaryLUT = make([]string, 0, count)
// 			out := make([]string, 0, count)
// 			read, err := d.DecodeStr(out)
// 			if err != nil || read != count {
// 				panic("unexpected")
// 			}

// 		case thrift.Type_INT96:
// 			panic("not supported type int96")
// 		default:
// 		}
// 	case thrift.Encoding_RLE:

// 	case thrift.Encoding_RLE_DICTIONARY:
// 		fallthrough
// 	case thrift.Encoding_PLAIN_DICTIONARY:
// 		// The bit width used to encode the entry ids
// 		// stored as 1 byte (max bit width = 32),
// 		// followed by the values encoded using RLE/Bit packed
// 		// described above (with the given bit width).

// 		bitWidth, err := rb.ReadByte()
// 		if err != nil {
// 			return err
// 		}

// 		dec := rle.NewHybridBitPackingRLEDecoder(rb)
// 		out := make([]uint64, 0, count)
// 		err := dec.Read(out, bitWidth)
// 		if err != nil {
// 			return err
// 		}
// 		if page == nil {
// 			return fmt.Errorf("data page in dictionary page format but no dictionary was defined")
// 		}
// 		for i := 0; i < len(out); i++ {
// 			value := page.Get(i)
// 		}

// 	default:
// 		panic("Not supported type for " + header.GetEncoding().String())
// 	}

// 	return nil
// }
