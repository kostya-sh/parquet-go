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

// FileMetaData.Write writes the object to a io.Writer.
func (meta *FileMetaData) Write(w io.Writer) (int64, error) {
	wc := NewCountingWriter(w)
	ttransport := &thrift.StreamTransport{Writer: wc}
	proto := thrift.NewTCompactProtocol(ttransport)
	err := meta.write(proto)
	return wc.N, err
}

// CountingWriter counts the number of bytes written to it.
type CountingWriter struct {
	W io.Writer // underlying writer
	N int64     // total # of bytes written
}

// CountingWriter wraps an existing io.Writer
func NewCountingWriter(w io.Writer) *CountingWriter {
	return &CountingWriter{W: w, N: 0}
}

// Write implements the io.Writer interface.
func (wc *CountingWriter) Write(p []byte) (int, error) {
	n, err := wc.W.Write(p)
	wc.N += int64(n)
	return n, err
}
