package parquet

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// Levels struct combines definition level (D) and repetion level (R).
type levels struct {
	// TODO: maybe use smaller type such as int8?
	d int
	r int
}

// Schema describes structure of the data that is stored in a parquet file.
//
// A Schema can be created from a parquetformat.FileMetaData. Information that
// is stored in RowGroups part of FileMetaData is not needed for the schema
// creation.
//
// TODO(ksh): provide a way to read FileMetaData without RowGroups.
//
// Usually FileMetaData should be read from the same file as data. When data is
// split into multiple parquet files metadata can be stored in a separate
// file. Usually this file is called "_common_metadata".
type Schema struct {
	root    group
	columns []Column
}

// Column contains information about a single column in a parquet file.
type Column struct {
	index         int
	name          string
	maxLevels     levels
	schemaElement *parquetformat.SchemaElement
}

// Index is a 0-based index of col in its schema.
//
// Column chunks in a row group have the same order as columns in the schema.
func (col Column) Index() int {
	return col.index
}

func (col Column) MaxD() int {
	return col.maxLevels.d
}

func (col Column) MaxR() int {
	return col.maxLevels.r
}

// MakeSchema creates a Schema from meta.
func MakeSchema(meta *parquetformat.FileMetaData) (Schema, error) {
	s := Schema{}
	end, err := s.root.create(meta.Schema, 0)
	if err != nil {
		return s, err
	}
	if end != len(meta.Schema) {
		return s, fmt.Errorf("too many SchemaElements, only %d out of %d have been used",
			end, len(meta.Schema))
	}

	s.columns = s.root.collectColumns()
	for i := range s.columns {
		s.columns[i].index = i
	}

	return s, nil
}

// ColumnByName returns a ColumnSchema with the given name (individual elements
// are separated with ".") or nil if such column does not exist in s.
func (s Schema) ColumnByName(name string) (col Column, found bool) {
	for i := range s.columns {
		if s.columns[i].name == name {
			return s.columns[i], true
		}
	}
	return Column{}, false
}

// ColumnByPath returns a ColumnSchema for the given path or or nil if such
// column does not exist in s.
func (s Schema) ColumnByPath(path []string) (col Column, found bool) {
	return s.ColumnByName(strings.Join(path, "."))
}

// Columns returns ColumnSchemas for all columns defined in s.
func (s Schema) Columns() []Column {
	return s.columns
}

// DisplayString returns a string representation of s using textual format
// similar to that described in the Dremel paper and used by parquet-mr project.
func (s Schema) DisplayString() string {
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
}

// primitive field
type primitive struct {
	schemaElement *parquetformat.SchemaElement
}

func (g *group) create(schema []*parquetformat.SchemaElement, start int) (int, error) {
	if len(schema) == 0 {
		return 0, fmt.Errorf("empty schema")
	}

	var s = schema[start]
	if s.NumChildren == nil {
		return 0, fmt.Errorf("NumChildren must be defined in schema[%d]", start)
	}
	if *s.NumChildren <= 0 {
		return 0, fmt.Errorf("Invalid NumChildren value in schema[%d]: %d", start, *s.NumChildren)
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
	for k := 0; k < int(*s.NumChildren); k++ {
		if i >= len(schema) {
			// TODO: more accurate error message
			return 0, fmt.Errorf("schema[%d].NumChildren is invalid (out of bounds)", start)
		}
		if schema[i].Type == nil {
			child := group{}
			i, err = child.create(schema, i)
			if err != nil {
				return 0, err
			}
			g.children[k] = &child
		} else {
			child := primitive{}
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

func (g *group) collectColumns() []Column {
	var cols = make([]Column, 0, len(g.children))
	for _, child := range g.children {
		switch c := child.(type) {
		case *primitive:
			s := c.schemaElement
			var levels levels
			if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
				levels.d = 1
			}
			if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
				levels.r = 1
			}
			cols = append(cols, Column{name: s.Name, maxLevels: levels, schemaElement: s})
		case *group:
			s := c.schemaElement
			for _, col := range c.collectColumns() {
				if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
					col.maxLevels.d++
				}
				if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
					col.maxLevels.r++
				}
				col.name = s.Name + "." + col.name
				cols = append(cols, col)
			}
		default:
			panic("unexpected child type")
		}
	}
	return cols
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
