package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

const (
	footerSize = 8 // bytes
	magicSize  = 4 // bytes
)

var (
	parquetMagic      = []byte{'P', 'A', 'R', '1'}
	ErrNotParquetFile = errors.New("not a parquet file: invalid header")
)

// writeFile
// Create a DataPage of Type
// Compress it
// Fill stats
// Write it to the file and record the set.
// Plain Encoder needs only data pages
//  WriteInt()
func writeFile(w io.Writer, meta *parquetformat.FileMetaData) error {

	// write header
	_, err := w.Write(parquetMagic)
	if err != nil {
		return fmt.Errorf("codec: header write error: %s", err)
	}

	/*
		// for each rowGroup
		for idx, rowGroup := range rowGroups {

		}
	*/

	// write metadata
	n, err := meta.Write(w)
	if err != nil {
		return fmt.Errorf("codec: filemetadata write error: %s", err)
	}

	// write metadata size
	if err := binary.Write(w, binary.LittleEndian, int32(n)); err != nil {
		return fmt.Errorf("codec: filemetadata size write error: %s", err)
	}

	// write footer
	_, err = w.Write(parquetMagic)
	if err != nil {
		return fmt.Errorf("codec: footer write error: %s", err)
	}

	return nil
}

// readFileMetaData reads parquetformat.FileMetaData object from r that provides
// read interface to data in parquet format.
//
// Parquet format is described here:
// https://github.com/apache/parquet-format/blob/master/README.md
// Note that the File Metadata is at the END of the file.
//
func readFileMetaData(r io.ReadSeeker) (*parquetformat.FileMetaData, error) {
	_, err := r.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error seeking to header: %s", err)
	}

	buf := make([]byte, magicSize, magicSize)
	// read and validate header
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading header: %s", err)
	}
	if !bytes.Equal(buf, parquetMagic) {
		return nil, ErrNotParquetFile
	}

	// read and validate footer
	_, err = r.Seek(-magicSize, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error seeking to footer: %s", err)
	}
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading footer: %s", err)
	}

	if !bytes.Equal(buf, parquetMagic) {
		return nil, ErrNotParquetFile
	}

	_, err = r.Seek(-footerSize, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error seeking to footer length: %s", err)
	}
	var footerLength int32
	err = binary.Read(r, binary.LittleEndian, &footerLength)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading footer length: %s", err)
	}
	if footerLength <= 0 {
		return nil, fmt.Errorf("read metadata: invalid footer length %d", footerLength)
	}

	// read file metadata
	_, err = r.Seek(-footerSize-int64(footerLength), os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error seeking to file: %s", err)
	}
	var meta parquetformat.FileMetaData
	err = meta.Read(io.LimitReader(r, int64(footerLength)))
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading file: %s", err)
	}

	return &meta, nil
}
