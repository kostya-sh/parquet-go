package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// Levels struct combines definition level (D) and repetion level (R).
type Levels struct {
	// TODO: maybe use smaller type such as int8?
	D int
	R int
}

// Schema describes structure of the data that is stored in a parquet file.
//
// A Schema can be created from a parquetformat.FileMetaData. Information that
// is stored in RowGroups part of FileMetaData is not needed for the schema
// creation.
// TODO(ksh): provide a way to read FileMetaData without RowGroups.
//
// Usually FileMetaData should be read from the same file as data. When data is
// split into multiple parquet files metadata can be stored in a separate
// file. Usually this file is called "_common_metadata".
type Schema struct {
	root    group
	columns map[string]ColumnSchema
}

// ColumnSchema contains information about a single column in a parquet file.
// TODO(ksh): or maybe interface?
type ColumnSchema struct {
	// MaxLevels contains maximum definition and repetition levels for this column
	MaxLevels     Levels
	SchemaElement *parquetformat.SchemaElement

	index int
}

// ReadFileMetaData reads parquetformat.FileMetaData object from r that provides
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

	buf := make([]byte, MAGIC_SIZE, MAGIC_SIZE)
	// read and validate header
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading header: %s", err)
	}
	if !bytes.Equal(buf, PARQUET_MAGIC) {
		return nil, ErrNotParquetFile
	}

	// read and validate footer
	_, err = r.Seek(-MAGIC_SIZE, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error seeking to footer: %s", err)
	}
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("read metadata: error reading footer: %s", err)
	}

	if !bytes.Equal(buf, PARQUET_MAGIC) {
		return nil, ErrNotParquetFile
	}

	_, err = r.Seek(-FOOTER_SIZE, os.SEEK_END)
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
	_, err = r.Seek(-FOOTER_SIZE-int64(footerLength), os.SEEK_END)
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

// schemaFromFileMetaData creates a Schema from meta.
func schemaFromFileMetaData(meta *parquetformat.FileMetaData) (*Schema, error) {
	fmt.Printf("%#v\n", meta)

	s := Schema{}
	end, err := s.root.create(meta.Schema, 0)
	if err != nil {
		return nil, err
	}
	if end != len(meta.Schema) {
		return nil, fmt.Errorf("too many SchemaElements, only %d out of %d have been used",
			end, len(meta.Schema))
	}

	maxLevels := s.root.calcMaxLevels()
	schemaElements := s.root.makeSchemaElements()
	s.columns = make(map[string]ColumnSchema)
	for name, lvls := range maxLevels {
		se, ok := schemaElements[name]
		if !ok {
			panic("should not happen")
		}
		s.columns[name] = ColumnSchema{MaxLevels: lvls, SchemaElement: se}
	}

	return &s, nil
}

// ColumnByName returns a ColumnSchema with the given name (individual elements
// are separated with ".") or nil if such column does not exist in s.
func (s *Schema) ColumnByName(name string) *ColumnSchema {
	cs, ok := s.columns[name]
	if !ok {
		return nil
	}
	return &cs
}

// ColumnByPath returns a ColumnSchema for the given path or or nil if such
// column does not exist in s.
func (s *Schema) ColumnByPath(path []string) *ColumnSchema {
	return s.ColumnByName(strings.Join(path, "."))
}

// DisplayString returns a string representation of s using textual format
// similar to that described in the Dremel paper and used by parquet-mr project.
func (s *Schema) DisplayString() string {
	b := new(bytes.Buffer)
	s.writeTo(b, "")
	return b.String()
}

type schemaElement interface {
	create(schema []*parquetformat.SchemaElement, start int) (next int, err error)

	writeTo(w io.Writer, indent string)
}

// group of fields
type group struct {
	schemaElement *parquetformat.SchemaElement
	children      []schemaElement
	index         int
}

// primitive field
type primitive struct {
	index         int
	schemaElement *parquetformat.SchemaElement
}

func (g *group) create(schema []*parquetformat.SchemaElement, start int) (int, error) {
	if len(schema) == 0 {
		return 0, nil
	}

	var s = schema[start]
	if s.NumChildren == nil {
		return 0, fmt.Errorf("NumChildren must be defined in schema[%d]", start)
	}

	if s.GetNumChildren() <= 0 {
		return 0, fmt.Errorf("Invalid NumChildren value in schema[%d]: %d", start, s.GetNumChildren())
	}

	if s.Type != nil {
		return 0, fmt.Errorf("Not null type (%s) in schema[%d]", s.Type, start)
	}
	if start != 0 {
		// TODO: check Name is not empty
		if s.RepetitionType == nil {
			return 0, fmt.Errorf("schema[%d].RepetitionType = nil", start)
		}
		// TODO: validate ConvertedType (nil, MAP, LIST, MAP_KEY_VALUE and structure)
	} else {
		// TODO: check other fields = null ?
	}

	g.schemaElement = s // TODO: deep copy?
	g.children = make([]schemaElement, *s.NumChildren, *s.NumChildren)

	i := start + 1
	var err error
	for k := 0; k < int(s.GetNumChildren()); k++ {
		if i >= len(schema) {
			// TODO: more accurate error message
			return 0, fmt.Errorf("schema[%d].NumChildren is invalid (out of bounds)", i)
		}
		if schema[i].Type == nil {
			child := group{}
			child.index = i
			i, err = child.create(schema, i)
			if err != nil {
				return 0, err
			}
			g.children[k] = &child
		} else {
			child := primitive{}
			child.index = i
			i, err = child.create(schema, i)
			if err != nil {
				return 0, err
			}
			g.children[k] = &child
		}
	}

	return i, nil
}

func (g *group) marshalChildren(w io.Writer, indent string) {
	fmt.Fprintln(w, " {")
	for _, child := range g.children {
		child.writeTo(w, indent+"  ")
	}
	fmt.Fprint(w, indent)
	fmt.Fprint(w, "}")
	if indent != "" {
		fmt.Fprintln(w)
	}
}

func (g *group) writeTo(w io.Writer, indent string) {
	var s = g.schemaElement

	fmt.Fprint(w, indent)
	fmt.Fprint(w, strings.ToLower(s.RepetitionType.String()))
	fmt.Fprint(w, " group ")
	fmt.Fprint(w, s.Name)
	if s.ConvertedType != nil {
		fmt.Fprintf(w, " (%s)", s.ConvertedType)
	}
	if s.FieldID != nil {
		fmt.Fprintf(w, " = %d", *s.FieldID)
	}

	g.marshalChildren(w, indent)
}

func (g *group) calcMaxLevels() map[string]Levels {
	lvls := make(map[string]Levels)
	for _, child := range g.children {
		switch c := child.(type) {
		case *primitive:
			s := c.schemaElement
			var levels Levels
			if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
				levels.D = 1
			}
			if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
				levels.R = 1
			}
			lvls[s.Name] = levels
		case *group:
			s := c.schemaElement
			for k, v := range c.calcMaxLevels() {
				if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
					v.D++
				}
				if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
					v.R++
				}
				lvls[s.Name+"."+k] = v
			}
		default:
			panic("unexpected child type")
		}
	}
	return lvls
}

func (g *group) makeSchemaElements() map[string]*parquetformat.SchemaElement {
	m := make(map[string]*parquetformat.SchemaElement)
	for _, child := range g.children {
		switch c := child.(type) {
		case *primitive:
			s := c.schemaElement
			m[s.Name] = s
		case *group:
			s := c.schemaElement
			for k, v := range c.makeSchemaElements() {
				m[s.Name+"."+k] = v
			}
		default:
			panic("unexpected child type")
		}
	}
	return m
}

func (s *Schema) writeTo(w io.Writer, indent string) {
	var se = s.root.schemaElement

	fmt.Fprint(w, indent)
	fmt.Fprintf(w, "message")
	if se.Name != "" {
		fmt.Fprintf(w, " %s", se.Name)
	}
	if se.ConvertedType != nil {
		fmt.Fprintf(w, " (%s)", se.ConvertedType)
	}

	s.root.marshalChildren(w, indent)
}

func (p *primitive) create(schema []*parquetformat.SchemaElement, start int) (int, error) {
	s := schema[start]

	// TODO: validate Name is not empty

	if s.RepetitionType == nil {
		return 0, fmt.Errorf("schema[%d].RepetitionType = nil", start)
	}

	t := *s.Type

	if t == parquetformat.Type_FIXED_LEN_BYTE_ARRAY {
		if s.TypeLength == nil {
			return 0, fmt.Errorf("schema[%d].TypeLength = nil for type FIXED_LEN_BYTE_ARRAY", start)
			// TODO: check length is positive
		}
	}

	if s.ConvertedType != nil {
		// validate ConvertedType
		ct := *s.ConvertedType
		switch {
		case (ct == parquetformat.ConvertedType_UTF8 && t != parquetformat.Type_BYTE_ARRAY) ||
			(ct == parquetformat.ConvertedType_MAP) ||
			(ct == parquetformat.ConvertedType_MAP_KEY_VALUE) ||
			(ct == parquetformat.ConvertedType_LIST):
			return 0, fmt.Errorf("%s field %s cannot be annotated with %s", t, s.Name, ct)
		}
		// TODO: validate U[INT]{8,16,32,64}
		// TODO: validate DECIMAL
		// TODO: validate DATE
		// TODO: validate TIME_MILLIS
		// TODO: validate TIMESTAMP_MILLIS
		// TODO: validate INTERVAL
		// TODO: validate JSON
		// TODO: validate BSON
	}

	p.schemaElement = s
	return start + 1, nil
}

func (p *primitive) writeTo(w io.Writer, indent string) {
	s := p.schemaElement

	fmt.Fprint(w, indent)
	fmt.Fprint(w, strings.ToLower(s.RepetitionType.String()))
	fmt.Fprint(w, " ")
	fmt.Fprint(w, strings.ToLower(s.Type.String()))
	if *s.Type == parquetformat.Type_FIXED_LEN_BYTE_ARRAY {
		fmt.Fprintf(w, "(%d)", *s.TypeLength)
	}
	fmt.Fprint(w, " ")
	fmt.Fprint(w, s.Name)
	if s.ConvertedType != nil {
		fmt.Fprint(w, " (")
		fmt.Fprint(w, s.ConvertedType.String())
		if *s.ConvertedType == parquetformat.ConvertedType_DECIMAL {
			fmt.Fprintf(w, "(%d,%d)", s.Precision, s.Scale)
		}
		fmt.Fprint(w, ")")
	}
	if s.FieldID != nil {
		fmt.Fprintf(w, " = %d", *s.FieldID)
	}

	fmt.Fprintln(w, ";")
}
