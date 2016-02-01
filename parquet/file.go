package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

const (
	FOOTER_SIZE = 8 // bytes
	MAGIC_SIZE  = 4 // bytes
)

var (
	PARQUET_MAGIC     = []byte{'P', 'A', 'R', '1'}
	ErrNotParquetFile = errors.New("not a parquet file: invalid header")
)

func (c *Encoder) Write(w io.Writer, chunks []bytes.Buffer) error {

	// write header
	_, err := w.Write(PARQUET_MAGIC)
	if err != nil {
		return fmt.Errorf("codec: header write error: %s", err)
	}

	root_children := int32(1)

	root := parquetformat.NewSchemaElement()
	root.Name = "root"
	root.NumChildren = &root_children

	// the root of the schema does not have to have a repetition type.
	// All the other elements do.
	schema := []*parquetformat.SchemaElement{root}
	group := parquetformat.NewRowGroup()

	typeint := parquetformat.Type_INT32

	offset := len(PARQUET_MAGIC)

	// for row group
	for idx, cc := range c.columns {
		cc.FileOffset = int64(offset)
		// n, err := cc.Write(w)
		// if err != nil {
		// 	return fmt.Errorf("chunk writer: could not write chunk for column %d: %s", idx, err)
		// }
		// offset += n
		cc.MetaData.DataPageOffset = int64(offset)

		n1, err := io.Copy(w, &chunks[0])
		if err != nil {
			return fmt.Errorf("chunk writer: could not write chunk for column %d: %s", idx, err)
		}

		log.Println("wrote:", n1)

		offset += int(n1)

		group.AddColumn(cc)

		columnSchema := parquetformat.NewSchemaElement()
		columnSchema.Name = cc.GetMetaData().PathInSchema[0]
		columnSchema.NumChildren = nil
		columnSchema.Type = &typeint
		required := parquetformat.FieldRepetitionType_REQUIRED
		columnSchema.RepetitionType = &required

		schema = append(schema, columnSchema)
	}

	// write metadata at then end of the file in thrift format
	meta := parquetformat.FileMetaData{
		Version:          0,
		Schema:           schema,
		RowGroups:        []*parquetformat.RowGroup{group},
		KeyValueMetadata: []*parquetformat.KeyValue{},
		CreatedBy:        &c.version, // go-parquet version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
	}

	n, err := meta.Write(w)
	if err != nil {
		return fmt.Errorf("codec: filemetadata write error: %s", err)
	}

	// write metadata size
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
