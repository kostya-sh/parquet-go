package main

import (
	"fmt"
	"os"

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

	r, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	m, err := parquet.ReadFileMetaData(r)
	if err != nil {
		return err
	}
	schema, err := parquet.MakeSchema(m)
	if err != nil {
		return err
	}
	col, found := schema.ColumnByName(dumpColumn)
	if !found {
		return fmt.Errorf("no column named '%s' in schema", dumpColumn)
	}

	for i, rg := range m.RowGroups {
		if col.Index() > len(rg.Columns) {
			return fmt.Errorf("not enough column chunks in rowgroup %d", i)
		}
		var cc = rg.Columns[col.Index()]
		cr, err := parquet.NewColumnChunkReader(r, col, *cc)
		if err != nil {
			return err
		}
		for cr.Next() {
			levels := cr.Levels()
			value := cr.Value()
			notNull := !levels.IsNull(col)
			if notNull {
				fmt.Print(value)
			}
			// TODO: consider customizing null value via command lines
			if showLevels {
				if notNull {
					fmt.Printf(" ")
				}
				fmt.Printf("(D:%d; R:%d)", levels.D(), levels.R())
			}
			fmt.Println()

		}
		if cr.Err() != nil {
			return cr.Err()
		}
	}

	return nil
}
