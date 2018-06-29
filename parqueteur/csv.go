package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"unicode/utf8"

	"github.com/kostya-sh/parquet-go/parquet"
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

func readAll(f *parquet.File, col parquet.Column) (allValues []interface{}, err error) {
	const batch = 16
	values := make([]interface{}, batch, batch)
	dLevels := make([]int, batch, batch)
	rLevels := make([]int, batch, batch)
	var n int
	for rg, _ := range f.MetaData.RowGroups {
		cr, err := f.NewReader(col, rg)
		if err != nil {
			return nil, err
		}

		for err != parquet.EndOfChunk {
			n, err = cr.Read(values, dLevels, rLevels)
			if err != nil && err != parquet.EndOfChunk {
				return nil, err
			}

			for i, vi := 0, 0; i < n; i++ {
				if dLevels[i] == col.MaxD() {
					allValues = append(allValues, (values[vi]))
					vi++
				} else {
					allValues = append(allValues, nil)
				}
			}
		}
	}
	return allValues, nil
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
	for _, col := range cols {
		if col.MaxR() != 0 {
			return fmt.Errorf("csv: column '%s' has repeated elements", col)
		}
	}

	// TODO: avoid reading everything to memory
	var colsData = make([][]interface{}, len(cols), len(cols))
	for i, col := range f.Schema.Columns() {
		colsData[i], err = readAll(f, col)
		if err != nil {
			return fmt.Errorf("csv: failed to read column '%s': %s", col, err)
		}
	}

	count := len(colsData[0])
	for i, colData := range colsData {
		if len(colData) != count {
			return fmt.Errorf("csv: wrong values count in column '%s': expected %d but was %d",
				cols[i], count, len(colData))
		}
	}

	out := csv.NewWriter(os.Stdout)
	out.Comma, _ = utf8.DecodeRuneInString(csvDelimiter)
	if csvHeader {
		r := make([]string, len(cols), len(cols))
		for i, col := range cols {
			r[i] = col.String()
		}
		if err = out.Write(r); err != nil {
			return err
		}
	}
	for i := 0; i < count; i++ {
		r := make([]string, len(cols), len(cols))
		for j, _ := range cols {
			r[j] = format(colsData[j][i])
		}
		if err = out.Write(r); err != nil {
			return err
		}
	}
	out.Flush()
	return out.Error()
}
