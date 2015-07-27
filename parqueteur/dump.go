package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/kostya-sh/parquet-go/parquet"
)

var cmdDump = &Command{
	Name: "dump",
	Help: "dump content of a parquet file",
}

// TODO: support various formats, e.g. CSV, fixed width, pretty print, JSON, etc
var dumpColumn string

func init() {
	cmdDump.Run = runDump

	// TODO: better usage message
	cmdDump.Flag.StringVar(&dumpColumn, "c", "", "dump content of the named `column`")
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

	c := 0 // hardcode just the first column for now
	schema := m.Schema[c+1]
	for _, rg := range m.RowGroups {
		cc := rg.Columns[c]
		if strings.Join(cc.MetaData.PathInSchema, ".") != dumpColumn {
			return fmt.Errorf("Unable to dump column '%s'", dumpColumn)
		}
		cr, err := parquet.NewBooleanColumnChunkReader(r, schema, cc)
		if err != nil {
			return err
		}
		for cr.Next() {
			fmt.Println(cr.Boolean())
		}
		if cr.Err() != nil {
			return cr.Err()
		}
	}

	return nil
}
