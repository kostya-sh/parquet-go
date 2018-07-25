package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"unicode/utf8"

	"github.com/kostya-sh/parquet-go/parquet"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

var cmdCSV = &Command{
	Name: "csv",
	Help: "convert a parquet file (with no repeated fields) to CSV format",
}

var csvDelimiter string
var csvHeader bool

func init() {
	cmdCSV.Run = runCSV

	cmdCSV.Flag.StringVar(&csvDelimiter, "d", ",", "CSV field delimiter")
	cmdCSV.Flag.BoolVar(&csvHeader, "H", false, "Include header row")

}

type ColStrIter struct {
	f   *parquet.File
	col parquet.Column

	bools      []bool
	int32s     []int32
	int64s     []int64
	int96s     []parquet.Int96
	float32s   []float32
	float64s   []float64
	byteArrays [][]byte

	stringFunc func(i int) string

	values interface{}

	dLevels []uint16
	rLevels []uint16

	cr *parquet.ColumnChunkReader
	rg int
	n  int
	i  int
	vi int
}

func (it *ColStrIter) boolStr(i int) string {
	if it.bools[i] {
		return "true"
	}
	return "false"
}

func (it *ColStrIter) int32Str(i int) string {
	return strconv.FormatInt(int64(it.int32s[i]), 10)
}

func (it *ColStrIter) int64Str(i int) string {
	return strconv.FormatInt(it.int64s[i], 10)
}

func (it *ColStrIter) int96Str(i int) string {
	return fmt.Sprint(it.int96s[i])
}

func (it *ColStrIter) float32Str(i int) string {
	return strconv.FormatFloat(float64(it.float32s[i]), 'g', -1, 32)
}

func (it *ColStrIter) float64Str(i int) string {
	return strconv.FormatFloat(it.float64s[i], 'g', -1, 64)
}

func (it *ColStrIter) byteArrayStr(i int) string {
	return string(it.byteArrays[i])
}

func NewColStrIter(f *parquet.File, col parquet.Column) *ColStrIter {
	const batchSize = 1024

	it := ColStrIter{
		f:       f,
		col:     col,
		dLevels: make([]uint16, batchSize, batchSize),
		rLevels: make([]uint16, batchSize, batchSize),
	}
	switch col.Type() {
	case parquetformat.Type_BOOLEAN:
		it.bools = make([]bool, batchSize, batchSize)
		it.stringFunc = it.boolStr
		it.values = it.bools
	case parquetformat.Type_INT32:
		it.int32s = make([]int32, batchSize, batchSize)
		it.stringFunc = it.int32Str
		it.values = it.int32s
	case parquetformat.Type_INT64:
		it.int64s = make([]int64, batchSize, batchSize)
		it.stringFunc = it.int64Str
		it.values = it.int64s
	case parquetformat.Type_INT96:
		it.int96s = make([]parquet.Int96, batchSize, batchSize)
		it.stringFunc = it.int96Str
		it.values = it.int96s
	case parquetformat.Type_FLOAT:
		it.float32s = make([]float32, batchSize, batchSize)
		it.stringFunc = it.float32Str
		it.values = it.float32s
	case parquetformat.Type_DOUBLE:
		it.float64s = make([]float64, batchSize, batchSize)
		it.stringFunc = it.float64Str
		it.values = it.float64s
	case parquetformat.Type_BYTE_ARRAY, parquetformat.Type_FIXED_LEN_BYTE_ARRAY:
		it.byteArrays = make([][]byte, batchSize, batchSize)
		it.stringFunc = it.byteArrayStr
		it.values = it.byteArrays
	default:
		panic("unknown type")
	}

	return &it
}

func (it *ColStrIter) Next() (string, error) {
	var err error
	if it.cr == nil {
		if it.rg == len(it.f.MetaData.RowGroups) {
			return "", io.EOF
		}
		it.cr, err = it.f.NewReader(it.col, it.rg)
		if err != nil {
			return "", err
		}
		it.rg++
	}

	if it.i >= it.n {
		it.n, err = it.cr.Read(it.values, it.dLevels, it.rLevels)
		if err == parquet.EndOfChunk {
			it.cr = nil
			return it.Next()
		}
		if err != nil {
			return "", err
		}
		it.i = 0
		it.vi = 0
	}

	s := ""
	if it.dLevels[it.i] == it.col.MaxD() {
		s = it.stringFunc(it.vi)
		it.vi++
	}
	it.i++
	return s, nil
}

func runCSV(cmd *Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("csv: no files")
	}

	f, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	cols := f.Schema.Columns()
	n := len(cols)
	for _, col := range cols {
		if col.MaxR() != 0 {
			return fmt.Errorf("csv: column '%s' has repeated elements", col)
		}
	}

	colIters := make([]*ColStrIter, n, n)
	for i, col := range cols {
		colIters[i] = NewColStrIter(f, col)
	}

	out := csv.NewWriter(os.Stdout)
	out.Comma, _ = utf8.DecodeRuneInString(csvDelimiter)
	r := make([]string, n, n)
	if csvHeader {
		for i, col := range cols {
			r[i] = col.String()
		}
		if err = out.Write(r); err != nil {
			return err
		}
	}

WriteLoop:
	for {
		for i, it := range colIters {
			s, err := it.Next()
			if err != nil {
				if err == io.EOF {
					break WriteLoop
				} else {
					return err
				}
			}
			r[i] = s
		}
		if err := out.Write(r); err != nil {
			return err
		}
	}
	out.Flush()
	return out.Error()
}
