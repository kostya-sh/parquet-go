package datatypes

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

type f func(interface{}) error

// RecordBuffer
type RecordBuffer struct {
	elements []*thrift.SchemaElement
	table    map[string]*Buffer
	//a accumulator
	length int
	err    error
}

func NewRecordbuffer(elements []*thrift.SchemaElement) *RecordBuffer {
	records := &RecordBuffer{elements: elements, table: make(map[string]*Buffer)}

	for _, schema := range elements {
		records.table[schema.Name] = NewBufferWithType(schema, 1024)
	}

	return records
}

func (rb *RecordBuffer) Err() error {
	return rb.err
}

// WriteBuffer
func (rb *RecordBuffer) Append(record map[string]interface{}) error {

	for colname, value := range record {
		buffer, ok := rb.table[colname]
		if !ok {
			return fmt.Errorf("invalid column name %s", colname)
		}

		if err := buffer.Append(value); err != nil {
			return fmt.Errorf("could not append to buffer: %s", err)
		}
	}

	rb.length++

	return nil
}

func (rb *RecordBuffer) Write(w BufferWriter) error {
	for name, buffer := range rb.table {
		if err := w.WriteBuffer(name, buffer); err != nil {
			return err
		}
	}

	return nil
}

func (rb *RecordBuffer) Reset() {
	for _, b := range rb.table {
		b.Reset()
	}
	rb.length = 0
}

func (rb *RecordBuffer) Len() int {
	return rb.length
}
