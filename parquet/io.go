package parquet

import "io"

// // CountingWriter counts the number of bytes written to it.
// type CountingWriter struct {
// 	W io.Writer // underlying writer
// 	N int64     // total # of bytes written
// }

// // CountingWriter wraps an existing io.Writer
// func NewCountingWriter(w io.Writer) *CountingWriter {
// 	return &CountingWriter{W: w, N: 0}
// }

// // Write implements the io.Writer interface.
// func (wc *CountingWriter) Write(p []byte) (int, error) {
// 	n, err := wc.W.Write(p)
// 	wc.N += int64(n)
// 	return n, err
// }

// ReadSeekCloser
type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// NopCloser returns a WriteCloser with a no-op Close method wrapping
// the provided Reader r.
func NopCloser(r io.Writer) io.WriteCloser {
	return nopCloser{r}
}
