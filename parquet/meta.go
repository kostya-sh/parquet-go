package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

var (
	PARQUET_MAGIC = []byte{'P', 'A', 'R', '1'}
)

const (
	FOOTER_SIZE = 8 // bytes
	MAGIC_SIZE  = 4 // bytes
)

// ReadFileMetaData reads parquetformat.FileMetaData object from r that provides
// read interface to data in parquet format.
//
// Parquet format is described here:
// https://github.com/apache/parquet-format/blob/master/README.md
// Note that the File Metadata is at the END of the file.
//
func ReadFileMetaData(r io.ReadSeeker) (*parquetformat.FileMetaData, error) {
	_, err := r.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to header: %s", err)
	}

	buf := make([]byte, MAGIC_SIZE, MAGIC_SIZE)
	// read and validate header
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading header: %s", err)
	}
	if !bytes.Equal(buf, PARQUET_MAGIC) {
		return nil, fmt.Errorf("Not a parquet file (invalid header)")
	}

	// read and validate footer
	_, err = r.Seek(-MAGIC_SIZE, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to footer: %s", err)
	}
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading footer: %s", err)
	}
	if !bytes.Equal(buf, PARQUET_MAGIC) {
		return nil, fmt.Errorf("Not a parquet file (invalid footer)")
	}

	_, err = r.Seek(-FOOTER_SIZE, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to footer length: %s", err)
	}
	var footerLength int32
	err = binary.Read(r, binary.LittleEndian, &footerLength)
	if err != nil {
		return nil, fmt.Errorf("Error reading footer length: %s", err)
	}
	if footerLength <= 0 {
		return nil, fmt.Errorf("Invalid footer length %d", footerLength)
	}

	// read file metadata
	_, err = r.Seek(-FOOTER_SIZE-int64(footerLength), os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to file metadata: %s", err)
	}
	var meta parquetformat.FileMetaData
	err = meta.Read(io.LimitReader(r, int64(footerLength)))
	if err != nil {
		return nil, fmt.Errorf("Error reading file metadata: %s", err)
	}

	return &meta, nil
}
