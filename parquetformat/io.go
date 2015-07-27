package parquetformat

import (
	"io"

	"git.apache.org/thrift.git/lib/go/thrift"
)

func newProtocol(r io.Reader) *thrift.TCompactProtocol {
	ttransport := thrift.NewStreamTransportR(r)
	return thrift.NewTCompactProtocol(ttransport)
}

// FileMetaData.Read reads the object from a io.Reader
func (meta *FileMetaData) Read(r io.Reader) error {
	return meta.read(newProtocol(r))
}

// PageHeader.Read reads the object from a io.Reader
func (ph *PageHeader) Read(r io.Reader) error {
	return ph.read(newProtocol(r))
}
