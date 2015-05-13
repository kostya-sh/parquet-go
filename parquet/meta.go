package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"git-wip-us.apache.org/repos/asf/thrift.git/lib/go/thrift"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

var magic = []byte{'P', 'A', 'R', '1'}

// ReadFileMetaData reads parquetformat.FileMetaData object from r that provides
// read interface to data in parquet format.
//
// Parquet format is described here:
// https://github.com/apache/parquet-format/blob/master/README.md
func ReadFileMetaData(r io.ReadSeeker) (*parquetformat.FileMetaData, error) {
	_, err := r.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to header: %s", err)
	}

	buf := make([]byte, 4, 4)
	// read and validate header
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading header: %s", err)
	}
	if !bytes.Equal(buf, magic) {
		return nil, fmt.Errorf("Not a parquet file (invalid header)")
	}

	// read and validate footer
	_, err = r.Seek(-4, 2)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to footer: %s", err)
	}
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading footer: %s", err)
	}
	if !bytes.Equal(buf, magic) {
		return nil, fmt.Errorf("Not a parquet file (invalid footer)")
	}

	// read footer length
	_, err = r.Seek(-8, 2)
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
	_, err = r.Seek(-8-int64(footerLength), 2)
	if err != nil {
		return nil, fmt.Errorf("Error seeking to file metadata: %s", err)
	}
	var ttransport = thrift.NewStreamTransportR(r)
	var tprotocol = thrift.NewTCompactProtocol(ttransport)
	var meta parquetformat.FileMetaData
	err = meta.Read(tprotocol)
	if err != nil {
		return nil, fmt.Errorf("Error reading file metadata: %s", err)
	}

	return &meta, nil
}
