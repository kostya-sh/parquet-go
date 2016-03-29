package page

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/kostya-sh/parquet-go/parquet/datatypes"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
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
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	for i := 0; i+4 < len(b); i += 4 {
		fmt.Printf("%.4d: %.2x %.2x %.2x %.2x\n", i, b[i], b[i+1], b[i+2], b[i+3])
	}

	p.rb = bufio.NewReader(bytes.NewReader(b))
	return nil
}

func (p *DataPage) readDefinitionAndRepetitionLevels(rb *bufio.Reader) (repetition []uint64, defintion []uint64, err error) {

	var length uint32
	err = binary.Read(rb, binary.LittleEndian, &length)
	if err != nil {
		return nil, nil, err
	}
	// err = binary.Read(rb, binary.LittleEndian, &length)
	// if err != nil {
	// 	return nil, nil, err
	// }

	return nil, nil, err // #FIXME

	// Definition Levels
	// For data that is required, the definition levels are skipped.
	// If encoded, it will always have the value of the max definition level.
	if p.schema.GetRepetitionType() != thrift.FieldRepetitionType_REQUIRED {
		log.Println(p.schema, p.schema.GetRepetitionType())
		defEnc := p.header.GetDefinitionLevelEncoding()
		switch defEnc {
		case thrift.Encoding_RLE:
			p.maxDefinitionLevels = 0

			// bitWidth := encoding.GetBitWidthFromMaxInt(p.maxDefinitionLevels)
			// var length uint32

			// err := binary.Read(rb, binary.LittleEndian, length)
			// if err != nil {
			// 	return nil, nil, err
			// }

			// log.Println("length", length)

			// for i := uint32(0); i < length; i++ {
			// 	if _, err := rb.ReadByte(); err != nil {
			// 		return nil, nil, err
			// 	}
			// }

		default:
			return nil, nil, fmt.Errorf("WARNING could not handle %s", defEnc)
		}
	}

	// Repetition Levels
	// only levels that are repeated need a Repetition level:
	// optional or required fields are never repeated
	// and can be skipped while attributing repetition levels.
	if p.schema.GetRepetitionType() == thrift.FieldRepetitionType_REPEATED {
		repEnc := p.header.GetRepetitionLevelEncoding()
		switch repEnc {
		case thrift.Encoding_BIT_PACKED:
			dec := bitpacking.NewDecoder(rb, 1) // FIXME 1 ?
			for dec.Scan() {
				log.Println("repetition level decoding:", dec.Value())
			}
			if err := dec.Err(); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("WARNING could not handle %s", repEnc)
		}
	}

	for i := 0; i < 0; i++ {
		c, err := rb.ReadByte()
		log.Println(c, err)
	}
	// FIXME there is something at the beginning of the data page. 4 bytes.. ?
	// var dummy int32
	// if err := binary.Read(rb, binary.LittleEndian, &dummy); err != nil {
	// }
	// log.Printf(" dangling value %d", dummy)

	return []uint64{}, []uint64{}, nil
}

func (p *DataPage) createDecoder(rb *bufio.Reader, page *DictionaryPage) (encoding.Decoder, error) {

	numValues := uint(p.header.NumValues)

	log.Println("encoding:", p.header.Encoding.String(), " :", numValues)

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

func (p *DataPage) Decode(page *DictionaryPage, accumulator datatypes.Accumulator) error {

	p.readDefinitionAndRepetitionLevels(p.rb)
	d, err := p.createDecoder(p.rb, page)
	if err != nil {
		return fmt.Errorf("could not create error %s", err)
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
