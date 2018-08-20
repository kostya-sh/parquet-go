package main

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet"
)

var cmdDump = &Command{
	Name: "dump",
	Help: "dump content of a parquet file",
}

// TODO: support various formats, e.g. CSV, fixed width, pretty print, JSON, etc
var dumpColumn string
var showLevels bool

func init() {
	cmdDump.Run = runDump

	// TODO: better usage message
	cmdDump.Flag.StringVar(&dumpColumn, "c", "", "dump content of the named `column`")
	cmdDump.Flag.BoolVar(&showLevels, "levels", false, "dump repetition and definition levels along with the column values")
}

func runDump(cmd *Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("No files")
	}

	f, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	col, found := f.Schema.ColumnByName(dumpColumn)
	if !found {
		return fmt.Errorf("no column named '%s' in schema", dumpColumn)
	}

	const batch = 16
	values := make([]interface{}, batch)
	dLevels := make([]uint16, batch)
	rLevels := make([]uint16, batch)
	var n int
	for rg, _ := range f.MetaData.RowGroups {
		cr, err := f.NewReader(col, rg)
		if err != nil {
			return err
		}

		for err != parquet.EndOfChunk {
			n, err = cr.Read(values, dLevels, rLevels)
			if err != nil && err != parquet.EndOfChunk {
				return err
			}

			for i, vi := 0, 0; i < n; i++ {
				d, r := dLevels[i], rLevels[i]
				notNull := d == col.MaxD()
				if notNull {
					fmt.Print(format(values[vi]))
					vi++
				}
				// TODO: consider customizing null value via command lines
				if showLevels {
					if notNull {
						fmt.Printf(" ")
					}
					fmt.Printf("(D:%d; R:%d)", d, r)
				}
				fmt.Println()
			}
		}
	}

	return nil
}
