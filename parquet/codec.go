package parquet

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

type Encoder struct {
	version string
}

func NewEncoder(schema string) *Encoder {
	// parse schema

	schema_elements := []string{}

	for range schema_elements {

	}

	return &Encoder{
		version: "parquet-go",
	}
}

func (c *Encoder) AddRowGroup() {

}

func (c *Encoder) Write(w io.Writer) error {
	// write header
	_, err := w.Write(PARQUET_MAGIC)
	if err != nil {
		return fmt.Errorf("codec: header write error: %s", err)
	}

	meta := parquetformat.FileMetaData{
		Version:          0,
		Schema:           []*parquetformat.SchemaElement{},
		RowGroups:        []*parquetformat.RowGroup{},
		KeyValueMetadata: []*parquetformat.KeyValue{},
		CreatedBy:        &c.version,
	}

	n, err := meta.Write(w)
	if err != nil {
		return fmt.Errorf("codec: filemetadata write error: %s", err)
	}

	if err := binary.Write(w, binary.LittleEndian, int32(n)); err != nil {
		return fmt.Errorf("codec: filemetadata size write error: %s", err)
	}

	// write footer
	_, err = w.Write(PARQUET_MAGIC)
	if err != nil {
		return fmt.Errorf("codec: footer write error: %s", err)
	}

	return nil
}

type Decoder struct {
	r      io.ReadSeeker
	meta   *parquetformat.FileMetaData
	schema *Schema
}

func NewDecoder(r io.ReadSeeker) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) ReadSchema() (err error) {
	d.meta, err = ReadFileMetaData(d.r)
	if err != nil {
		return err
	}

	d.schema, err = SchemaFromFileMetaData(d.meta)

	return err
}
