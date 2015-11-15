package parquet

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/golang/snappy"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

var Config = struct {
	Debug bool
}{
	Debug: false,
}

// Scanner provides a convenient interface for reading data such as
// a file of newline-delimited lines of text.

// ColumnScanner implements the logic to deserialize columns in the parquet format
type ColumnScanner struct {
	rs             io.ReadSeeker // The reader provided by the client.
	r              io.Reader
	chunk          *parquetformat.ColumnChunk
	schema         *parquetformat.SchemaElement
	totalPagesRead int64 // number of pages read in this chunk
	totalBytesRead int64
	err            error
}

// NewColumnScanner returns a ColumnScanner that reads from r
// and interprets the stream as described in the ColumnChunk parquet format
func NewColumnScanner(rs io.ReadSeeker, chunk *parquetformat.ColumnChunk, schema *parquetformat.SchemaElement) *ColumnScanner {
	return &ColumnScanner{rs, nil, chunk, schema, 0, 0, nil}
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
	if s.totalPagesRead == 0 {
		_, err := s.rs.Seek(s.chunk.MetaData.DataPageOffset, os.SEEK_SET)
		if err != nil {
			s.setErr(err)
			return false
		}

		// substitute the original reader with a limited one to get io.EOF
		// when the chunk is read
		s.r = io.LimitReader(s.rs, s.chunk.MetaData.TotalCompressedSize)
	}

	for {
		if err := s.nextPage(); err != nil {
			s.setErr(err)
			return false
		}

		s.totalPagesRead++
		break
	}

	return true
}

func (s *ColumnScanner) nextPage() (err error) {
	r := s.r

	if s.totalBytesRead >= s.chunk.MetaData.TotalCompressedSize {
		return io.EOF
	}

	var header parquetformat.PageHeader
	err = header.Read(r)
	if err != nil {
		if strings.HasSuffix(err.Error(), "EOF") { // FIXME: find a better way to detect io.EOF
			return io.EOF
		}
		return err
	}

	if Config.Debug {
		log.Printf("\t\tType:%s\n", header.Type)
		log.Printf("\t\tUncompressed:%d\n", header.UncompressedPageSize)
		log.Printf("\t\tCompressed:%d\n", header.CompressedPageSize)
		log.Printf("\t\tCRC:%d\n", header.Crc)

		switch header.Type {
		case parquetformat.PageType_DATA_PAGE:
			log.Printf("\t\tDataPage:%s\n", header.DataPageHeader)
			log.Printf("\t\t\tnum_values:%d\n", header.DataPageHeader.NumValues)
			log.Printf("\t\t\tencoding:%s\n", header.DataPageHeader.Encoding)
			log.Printf("\t\t\tdefinition_level_encoding:%s\n", header.DataPageHeader.DefinitionLevelEncoding)
			log.Printf("\t\t\trepetition_level_encoding:%s\n", header.DataPageHeader.RepetitionLevelEncoding)
			// start reading numValues for each definition?

		case parquetformat.PageType_INDEX_PAGE:
			log.Printf("\t\tIndexPage:%s\n", header.IndexPageHeader)

		case parquetformat.PageType_DICTIONARY_PAGE:
			log.Printf("\t\tDictionaryPage:%s\n", header.DictionaryPageHeader)

		case parquetformat.PageType_DATA_PAGE_V2:
			log.Printf("\t\tDataPageV2:%s\n", header.DataPageHeaderV2)
		default:
			panic("Unsupported PageHeader.Type")
		}
	}

	// handle compressed data
	// setup codec
	switch s.chunk.MetaData.Codec {
	case parquetformat.CompressionCodec_GZIP:
		r, err := gzip.NewReader(s.r)
		if err != nil {
			return err
		}
		s.r = r
	case parquetformat.CompressionCodec_LZO:
		// https://github.com/rasky/go-lzo/blob/master/decompress.go#L149			s.r = r
		panic("NYI")

	case parquetformat.CompressionCodec_SNAPPY:
		s.r = snappy.NewReader(s.r)
	case parquetformat.CompressionCodec_UNCOMPRESSED:
		// use the limit reader
	}

	switch header.Type {
	case parquetformat.PageType_DATA_PAGE:
		if !header.IsSetDataPageHeader() {
			panic("unexpected DataPageHeader was not set")
		}

		// n := header.DataPageHeader.NumValues
		// // read definition levels.
		// rle.NewDecoder(}, 1)

		// read repetition levels.

		log.Println("encoding:", header.DataPageHeader.Encoding)

		switch header.DataPageHeader.Encoding {
		case parquetformat.Encoding_BIT_PACKED:
		case parquetformat.Encoding_DELTA_BINARY_PACKED:
		case parquetformat.Encoding_DELTA_BYTE_ARRAY:
		case parquetformat.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		case parquetformat.Encoding_PLAIN:
		case parquetformat.Encoding_PLAIN_DICTIONARY:
		case parquetformat.Encoding_RLE:
		case parquetformat.Encoding_RLE_DICTIONARY:

		default:
			panic("Not supported")
		}

	case parquetformat.PageType_INDEX_PAGE:
	case parquetformat.PageType_DICTIONARY_PAGE:
	case parquetformat.PageType_DATA_PAGE_V2:

	default:
		panic("parquet.ColumnScanner: unknown PageHeader.PageType")
	}

	var bytesRead = int64(0)
	if bytesRead, err = io.CopyN(ioutil.Discard, s.r, int64(header.CompressedPageSize)); err != nil {
		return err
	}
	s.totalBytesRead += bytesRead

	// while (true) {
	//     int bytes_read = 0;
	//     const uint8_t* buffer = stream_->Peek(DATA_PAGE_SIZE, &bytes_read);
	//     if (bytes_read == 0) return false;
	//     uint32_t header_size = bytes_read;
	//     DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
	//     stream_->Read(header_size, &bytes_read);

	//     int compressed_len = current_page_header_.compressed_page_size;
	//     int uncompressed_len = current_page_header_.uncompressed_page_size;

	//     // Read the compressed data page.
	//     buffer = stream_->Read(compressed_len, &bytes_read);
	//     if (bytes_read != compressed_len) ParquetException::EofException();

	//     // Uncompress it if we need to
	//     if (decompressor_ != NULL) {
	//       // Grow the uncompressed buffer if we need to.
	//       if (uncompressed_len > decompression_buffer_.size()) {
	//         decompression_buffer_.resize(uncompressed_len);
	//       }
	//       decompressor_->Decompress(
	//           compressed_len, buffer, uncompressed_len, &decompression_buffer_[0]);
	//       buffer = &decompression_buffer_[0];
	//     }

	//     if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
	//       boost::unordered_map<Encoding::type, boost::shared_ptr<Decoder> >::iterator it =
	//           decoders_.find(Encoding::RLE_DICTIONARY);
	//       if (it != decoders_.end()) {
	//         throw ParquetException("Column cannot have more than one dictionary.");
	//       }

	//       PlainDecoder dictionary(schema_->type);
	//       dictionary.SetData(current_page_header_.dictionary_page_header.num_values,
	//           buffer, uncompressed_len);
	//       boost::shared_ptr<Decoder> decoder(
	//           new DictionaryDecoder(schema_->type, &dictionary));
	//       decoders_[Encoding::RLE_DICTIONARY] = decoder;
	//       current_decoder_ = decoders_[Encoding::RLE_DICTIONARY].get();
	//       continue;
	//     } else if (current_page_header_.type == PageType::DATA_PAGE) {
	//       // Read a data page.
	//       num_buffered_values_ = current_page_header_.data_page_header.num_values;

	//       // Read definition levels.
	//       if (schema_->repetition_type != FieldRepetitionType::REQUIRED) {
	//         int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
	//         buffer += sizeof(uint32_t);
	//         definition_level_decoder_.reset(
	//             new impala::RleDecoder(buffer, num_definition_bytes, 1));
	//         buffer += num_definition_bytes;
	//         uncompressed_len -= sizeof(uint32_t);
	//         uncompressed_len -= num_definition_bytes;
	//       }

	//       // TODO: repetition levels

	//       // Get a decoder object for this page or create a new decoder if this is the
	//       // first page with this encoding.
	//       Encoding::type encoding = current_page_header_.data_page_header.encoding;
	//       if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

	//       boost::unordered_map<Encoding::type, boost::shared_ptr<Decoder> >::iterator it =
	//           decoders_.find(encoding);
	//       if (it != decoders_.end()) {
	//         current_decoder_ = it->second.get();
	//       } else {
	//         switch (encoding) {
	//           case Encoding::PLAIN: {
	//             boost::shared_ptr<Decoder> decoder;
	//             if (schema_->type == Type::BOOLEAN) {
	//               decoder.reset(new BoolDecoder());
	//             } else {
	//               decoder.reset(new PlainDecoder(schema_->type));
	//             }
	//             decoders_[encoding] = decoder;
	//             current_decoder_ = decoder.get();
	//             break;
	//           }
	//           case Encoding::RLE_DICTIONARY:
	//             throw ParquetException("Dictionary page must be before data page.");

	//           case Encoding::DELTA_BINARY_PACKED:
	//           case Encoding::DELTA_LENGTH_BYTE_ARRAY:
	//           case Encoding::DELTA_BYTE_ARRAY:
	//             ParquetException::NYI("Unsupported encoding");

	//           default:
	//             throw ParquetException("Unknown encoding type.");
	//         }
	//       }
	//       current_decoder_->SetData(num_buffered_values_, buffer, uncompressed_len);
	//       return true;
	//     } else {
	//       // We don't know what this page type is. We're allowed to skip non-data pages.
	//       continue;
	//     }
	//   }
	//   return true;
	// }
	// cr, err := parquet.NewBooleanColumnChunkReader(r, schema, chunks)
	// if err != nil {
	// 	return err
	// }
	// for cr.Next() {
	// 	fmt.Println(cr.Boolean())
	// }
	// if cr.Err() != nil {
	// 	return cr.Err()
	// }
	return nil
}

// func byteSizeForType() {
// switch (metadata->type) {
//     case parquet::Type::BOOLEAN:
//       value_byte_size = 1;
//       break;
//     case parquet::Type::INT32:
//       value_byte_size = sizeof(int32_t);
//       break;
//     case parquet::Type::INT64:
//       value_byte_size = sizeof(int64_t);
//       break;
//     case parquet::Type::FLOAT:
//       value_byte_size = sizeof(float);
//       break;
//     case parquet::Type::DOUBLE:
//       value_byte_size = sizeof(double);
//       break;
//     case parquet::Type::BYTE_ARRAY:
//       value_byte_size = sizeof(ByteArray);
//       break;
//     default:
//       ParquetException::NYI("Unsupported type");
//   }
// }

// switch (metadata->codec) {
//     case CompressionCodec::UNCOMPRESSED:
//       break;
//     case CompressionCodec::SNAPPY:
//       decompressor_.reset(new SnappyCodec());
//       break;
//     default:
//       ParquetException::NYI("Reading compressed data");
//   }

//   config_ = Config::DefaultConfig();
//   values_buffer_.resize(config_.batch_size * value_byte_size);
