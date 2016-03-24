package page

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/kostya-sh/parquet-go/encoding/bitpacking"
	"github.com/kostya-sh/parquet-go/encoding/rle"
	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

type DataPage interface {
}

// readDataPage
func readDataPage(schema *thrift.SchemaElement, header *thrift.DataPageHeader, rb *bufio.Reader) (*DataPage, error) {

	count := int(header.GetNumValues())

	// only levels that are repeated need a Repetition level:
	// optional or required fields are never repeated
	// and can be skipped while attributing repetition levels.
	if schema.GetRepetitionType() == thrift.FieldRepetitionType_REPEATED {
		repEnc := header.GetRepetitionLevelEncoding()
		switch repEnc {
		case thrift.Encoding_BIT_PACKED:
			dec := bitpacking.NewDecoder(rb, 1) // FIXME 1 ?
			for dec.Scan() {
				log.Println("repetition level decoding:", dec.Value())
			}

			if err := dec.Err(); err != nil {
				return nil, err
			}
		default:
			log.Println("WARNING could not handle %s", repEnc)
			return nil, err
		}
	}

	// a required field is always defined and does not need a definition level.
	if schema.GetRepetitionType() != thrift.FieldRepetitionType_REQUIRED {
		defEnc := header.GetDefinitionLevelEncoding()
		switch defEnc {
		case thrift.Encoding_RLE:
			dec := rle.NewDecoder(rb)

			for dec.Scan() {
				log.Println("definition level decoding:", dec.Value())
			}

			if err := dec.Err(); err != nil {
				return nil, err
			}

		default:
			log.Println("WARNING could not handle %s", defEnc)
			return nil, err
		}
	}

	// FIXME there is something at the beginning of the data page. 4 bytes.. ?
	var dummy int32
	err := binary.Read(rb, binary.LittleEndian, &dummy)
	if err != nil {
		log.Printf("column chunk: %s\n", err)
	}

	// Handle DataPageEncoding
	switch header.Encoding {
	case thrift.Encoding_BIT_PACKED:

	case thrift.Encoding_DELTA_BINARY_PACKED:
	case thrift.Encoding_DELTA_BYTE_ARRAY:
	case thrift.Encoding_DELTA_LENGTH_BYTE_ARRAY:
	case thrift.Encoding_PLAIN:
		d := encoding.NewPlainDecoder(rb, meta.GetType(), int(header.NumValues))
		switch meta.GetType() {

		case thrift.Type_INT32:
			out := make([]int32, 0, count)
			read, err := d.DecodeInt32(out)
			if err != nil || read != count {
				panic("unexpected")
			}
			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}

		case thrift.Type_INT64:
			out := make([]int64, 0, count)

			read, err := d.DecodeInt64(out)
			if err != nil || read != count {
				panic("unexpected")
			}
			for idx, value := range out {
				log.Printf("%d %d", idx, value)
			}

		case thrift.Type_BYTE_ARRAY, thrift.Type_FIXED_LEN_BYTE_ARRAY:
			s.dictionaryLUT = make([]string, 0, count)
			read, err := d.DecodeStr(s.dictionaryLUT)
			if err != nil || read != count {
				panic("unexpected")
			}

		case thrift.Type_INT96:
			panic("not supported type int96")
		default:
		}
	case thrift.Encoding_RLE:

	case thrift.Encoding_RLE_DICTIONARY:
		fallthrough
	case thrift.Encoding_PLAIN_DICTIONARY:
		b, err := rb.ReadByte()
		if err != nil {
			panic(err)
		}

		dec := rle.NewHybridBitPackingRLEDecoder(rb, int(b))

		for dec.Scan() {
			log.Println(meta.GetPathInSchema(), dec.Value())
		}

		if err := dec.Err(); err != nil {
			panic(fmt.Errorf("%s: plain_dictionary: %s", meta.GetPathInSchema(), err))
		}

	default:
		panic("Not supported type for " + header.GetEncoding().String())
	}
}
