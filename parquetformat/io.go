package parquetformat

import (
	"io"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// FileMetaData.Read reads the object from a io.Reader
func (meta *FileMetaData) Read(r io.Reader) error {
	var ttransport = thrift.NewStreamTransportR(r)
	var tprotocol = thrift.NewTCompactProtocol(ttransport)
	return meta.read(tprotocol)
}
