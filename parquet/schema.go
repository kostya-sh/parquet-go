package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

func strptr(v string) *string {
	return &v
}

func normalizeType(str string) string {
	return strings.ToUpper(str)
}

// Levels struct combines definition level (D) and repetion level (R).
type Levels struct {
	// TODO: maybe use smaller type such as int8?
	D int
	R int
}

// Schema describes structure of the data that is stored in a parquet file.
//
// A Schema can be created from a thrift.FileMetaData. Information that
// is stored in RowGroups part of FileMetaData is not needed for the schema
// creation.
// TODO(ksh): provide a way to read FileMetaData without RowGroups.
//
// Usually FileMetaData should be read from the same file as data. When data is
// split into multiple parquet files metadata can be stored in a separate
// file. Usually this file is called "_common_metadata".
type Schema struct {
	root            group
	columns         map[string]ColumnDescriptor
	columnsSequence []string
}

var (
	ErrBadFormat = errors.New("invalid format. format is `name: type [original type] REQUIRED|OPTIONAL`")
)

// NewSchema create an empty schema.
func NewSchema() *Schema {
	return &Schema{
		root:    group{},
		columns: make(map[string]ColumnDescriptor),
	}
}

// Columns return the name of all the columns in this schema.
func (s *Schema) Columns() []string {
	return s.columnsSequence
}

// AddColumn adds a column with the given specifications format
// format is
//          name: type [original type] REQUIRED
func (s *Schema) AddColumn(format string) error {
	values := strings.SplitN(format, ":", 2)
	if len(values) != 2 {
		return ErrBadFormat
	}

	name := values[0]
	spec := values[1]

	el := thrift.NewSchemaElement()
	el.Name = name

	values = strings.Split(strings.TrimSpace(spec), " ")

	originalType, err := thrift.TypeFromString(normalizeType(values[0]))
	if err != nil {
		return fmt.Errorf("could not add column: bad type: %s (%s)", err, values[0])
	}
	el.Type = &originalType

	switch len(values) {
	case 3:
		convertedType, err := thrift.ConvertedTypeFromString(normalizeType(values[1]))
		if err != nil {
			return fmt.Errorf("could not add column: bad converted type: %s", err)
		}

		repetitionType, err := thrift.FieldRepetitionTypeFromString(normalizeType(values[2]))
		if err != nil {
			return fmt.Errorf("could not add column: bad repetition type: %s", err)
		}

		el.ConvertedType = &convertedType
		el.RepetitionType = &repetitionType
	case 2:
		repetitionType, err := thrift.FieldRepetitionTypeFromString(normalizeType(values[1]))
		if err != nil {
			return fmt.Errorf("could not add column: bad repetition type: %s", err)
		}
		el.RepetitionType = &repetitionType

	default:
		return fmt.Errorf("could not add column: invalid number of elements in format")

	}

	s.columns[el.Name] = ColumnDescriptor{
		SchemaElement: el,
	}

	return nil
}

// ColumnDescriptor contains information about a single column in a parquet file.
// TODO(ksh): or maybe interface?
type ColumnDescriptor struct {
	// MaxLevels contains maximum definition and repetition levels for this column
	MaxLevels     Levels
	SchemaElement *thrift.SchemaElement

	index int
}

func (schema *Schema) createMetadata() *thrift.FileMetaData {
	root_children := int32(1)

	root := thrift.NewSchemaElement()
	root.Name = "root"
	root.NumChildren = &root_children

	// the root of the schema does not have to have a repetition type.
	// All the other elements do.
	elements := []*thrift.SchemaElement{root}

	//typeint := thrift.Type_INT32

	//offset := len(PARQUET_MAGIC)

	// for row group
	// for idx, cc := range schema.columns {
	// 	cc.FileOffset = int64(offset)
	// 	// n, err := cc.Write(w)
	// 	// if err != nil {
	// 	// 	return fmt.Errorf("chunk writer: could not write chunk for column %d: %s", idx, err)
	// 	// }
	// 	// offset += n
	// 	cc.MetaData.DataPageOffset = int64(offset)

	// 	n1, err := io.Copy(w, &chunks[0])
	// 	if err != nil {
	// 		return fmt.Errorf("chunk writer: could not write chunk for column %d: %s", idx, err)
	// 	}

	// 	log.Println("wrote:", n1)

	// 	offset += int(n1)

	// 	group.AddColumn(cc)

	// 	columnDescriptor := thrift.NewSchemaElement()
	// 	columnDescriptor.Name = cc.GetMetaData().PathInSchema[0]
	// 	columnDescriptor.NumChildren = nil
	// 	columnDescriptor.Type = &typeint
	// 	required := thrift.FieldRepetitionType_REQUIRED
	// 	columnDescriptor.RepetitionType = &required

	// 	schema = append(schema, columnDescriptor)
	// }

	// write metadata at then end of the file in thrift format
	meta := thrift.FileMetaData{
		Version:          0,
		Schema:           elements,
		RowGroups:        []*thrift.RowGroup{},
		KeyValueMetadata: []*thrift.KeyValue{},
		CreatedBy:        strptr("go-0.1"), // go-parquet version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
	}

	return &meta
}

// schemaFromFileMetaData creates a Schema from meta.
func schemaFromFileMetaData(meta *thrift.FileMetaData) (*Schema, error) {
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
	s.columns = make(map[string]ColumnDescriptor)
	for name, lvls := range maxLevels {
		se, ok := schemaElements[name]
		if !ok {
			panic("should not happen")
		}
		s.columnsSequence = append(s.columnsSequence, name)
		s.columns[name] = ColumnDescriptor{MaxLevels: lvls, SchemaElement: se}
	}

	return &s, nil
}

// ColumnByName returns a ColumnDescriptor with the given name (individual elements
// are separated with ".") or nil if such column does not exist in s.
func (s *Schema) ColumnByName(name string) *ColumnDescriptor {
	cs, ok := s.columns[name]
	if !ok {
		return nil
	}
	return &cs
}

// ColumnByPath returns a ColumnDescriptor for the given path or or nil if such
// column does not exist in s.
func (s *Schema) ColumnByPath(path []string) *ColumnDescriptor {
	return s.ColumnByName(strings.Join(path, "."))
}

// DisplayString returns a string representation of s using textual format
// similar to that described in the Dremel paper and used by parquet-mr project.
func (s *Schema) DisplayString() string {
	var b bytes.Buffer
	s.writeTo(&b, "")
	return b.String()
}

type schemaElement interface {
	create(schema []*thrift.SchemaElement, start int) (next int, err error)

	writeTo(w io.Writer, indent string)
}

// group of fields
type group struct {
	schemaElement *thrift.SchemaElement
	children      []schemaElement
	index         int
}

// primitive field
type primitive struct {
	index         int
	schemaElement *thrift.SchemaElement
}

func (g *group) create(schema []*thrift.SchemaElement, start int) (int, error) {
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
			if *s.RepetitionType != thrift.FieldRepetitionType_REQUIRED {
				levels.D = 1
			}
			if *s.RepetitionType == thrift.FieldRepetitionType_REPEATED {
				levels.R = 1
			}
			lvls[s.Name] = levels
		case *group:
			s := c.schemaElement
			for k, v := range c.calcMaxLevels() {
				if *s.RepetitionType != thrift.FieldRepetitionType_REQUIRED {
					v.D++
				}
				if *s.RepetitionType == thrift.FieldRepetitionType_REPEATED {
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

func (g *group) makeSchemaElements() map[string]*thrift.SchemaElement {
	m := make(map[string]*thrift.SchemaElement)
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

func (p *primitive) create(schema []*thrift.SchemaElement, start int) (int, error) {
	s := schema[start]

	// TODO: validate Name is not empty

	if s.RepetitionType == nil {
		return 0, fmt.Errorf("schema[%d].RepetitionType = nil", start)
	}

	t := *s.Type

	if t == thrift.Type_FIXED_LEN_BYTE_ARRAY {
		if s.TypeLength == nil {
			return 0, fmt.Errorf("schema[%d].TypeLength = nil for type FIXED_LEN_BYTE_ARRAY", start)
			// TODO: check length is positive
		}
	}

	if s.ConvertedType != nil {
		// validate ConvertedType
		ct := *s.ConvertedType
		switch {
		case (ct == thrift.ConvertedType_UTF8 && t != thrift.Type_BYTE_ARRAY) ||
			(ct == thrift.ConvertedType_MAP) ||
			(ct == thrift.ConvertedType_MAP_KEY_VALUE) ||
			(ct == thrift.ConvertedType_LIST):
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
	if *s.Type == thrift.Type_FIXED_LEN_BYTE_ARRAY {
		fmt.Fprintf(w, "(%d)", *s.TypeLength)
	}
	fmt.Fprint(w, " ")
	fmt.Fprint(w, s.Name)
	if s.ConvertedType != nil {
		fmt.Fprint(w, " (")
		fmt.Fprint(w, s.ConvertedType.String())
		if *s.ConvertedType == thrift.ConvertedType_DECIMAL {
			fmt.Fprintf(w, "(%d,%d)", s.Precision, s.Scale)
		}
		fmt.Fprint(w, ")")
	}
	if s.FieldID != nil {
		fmt.Fprintf(w, " = %d", *s.FieldID)
	}

	fmt.Fprintln(w, ";")
}
