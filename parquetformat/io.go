package parquetformat

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquetformat/internal/thrift"
)

func newProtocol(r io.Reader) *thrift.TCompactProtocol {
	ttransport := &thrift.StreamTransport{Reader: r}
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
