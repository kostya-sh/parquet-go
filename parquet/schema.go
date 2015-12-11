package parquet

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// Parquet Schema
type Schema interface {
	// MarshalDL writes schema to a given io.Writer using textual format similar
	// to that described in the Dremel paper and used by parquet-mr project.
	MarshalDL(w io.Writer) error

	// maxLevels returns maximum definition and repetition levels for a given
	// column path
	maxLevels(path []string) (definition int, repetition int)

	// TODO: better name (schemaElement?)
	element(path []string) *parquetformat.SchemaElement
}

type schemaElement interface {
	create(schema []*parquetformat.SchemaElement, start int) (next int, err error)

	marshalDL(w io.Writer, indent string)
}

// root of the schema
type message struct {
	group
	maxLevelsByPath      map[string][2]int
	schemaElementsByPath map[string]*parquetformat.SchemaElement
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
		child.marshalDL(w, indent+"  ")
	}
	fmt.Fprint(w, indent)
	fmt.Fprintln(w, "}")
}

func (g *group) marshalDL(w io.Writer, indent string) {
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

func (g *group) calcMaxLevels() map[string][2]int {
	lvls := make(map[string][2]int)
	for _, child := range g.children {
		switch c := child.(type) {
		case *primitive:
			s := c.schemaElement
			d := 0
			r := 0
			if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
				d = 1
			}
			if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
				r = 1
			}
			lvls[s.Name] = [...]int{d, r}
		case *group:
			s := c.schemaElement
			for k, v := range c.calcMaxLevels() {
				d := v[0]
				if *s.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
					d++
				}
				r := v[1]
				if *s.RepetitionType == parquetformat.FieldRepetitionType_REPEATED {
					r++
				}
				lvls[s.Name+"."+k] = [...]int{d, r}
			}
		default:
			panic("unexpected child type")
		}
	}
	return lvls
}

func (g *group) makeSchemaElementsByPath() map[string]*parquetformat.SchemaElement {
	m := make(map[string]*parquetformat.SchemaElement)
	for _, child := range g.children {
		switch c := child.(type) {
		case *primitive:
			s := c.schemaElement
			m[s.Name] = s
		case *group:
			s := c.schemaElement
			for k, v := range c.makeSchemaElementsByPath() {
				m[s.Name+"."+k] = v
			}
		default:
			panic("unexpected child type")
		}
	}
	return m
}

func (m *message) marshalDL(w io.Writer, indent string) {
	var s = m.schemaElement

	fmt.Fprint(w, indent)
	fmt.Fprintf(w, "message")
	if s.Name != "" {
		fmt.Fprintf(w, " %s", s.Name)
	}
	if s.ConvertedType != nil {
		fmt.Fprintf(w, " (%s)", s.ConvertedType)
	}

	m.group.marshalChildren(w, indent)
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

func (p *primitive) marshalDL(w io.Writer, indent string) {
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

func (m *message) MarshalDL(w io.Writer) error {
	b := bufio.NewWriter(w)
	m.marshalDL(w, "")
	return b.Flush()
}

func (m *message) maxLevels(path []string) (definition int, repetition int) {
	lvls, ok := m.maxLevelsByPath[strings.Join(path, ".")]
	if !ok {
		return -1, -1
	}
	return lvls[0], lvls[1]
}

func (m *message) element(path []string) *parquetformat.SchemaElement {
	return m.schemaElementsByPath[strings.Join(path, ".")]
}

func SchemaFromFileMetaData(meta *parquetformat.FileMetaData) (Schema, error) {
	m := message{}
	end, err := m.group.create(meta.Schema, 0)
	if err != nil {
		return nil, err
	}
	if end != len(meta.Schema) {
		return nil, fmt.Errorf("Only %d SchemaElement(s) out of %d have been used",
			end, len(meta.Schema))
	}
	m.maxLevelsByPath = m.group.calcMaxLevels()
	m.schemaElementsByPath = m.group.makeSchemaElementsByPath()

	return &m, nil
}
