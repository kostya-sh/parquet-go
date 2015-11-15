package main

import (
	"fmt"
	"log"
	"os"

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

// read The file metadata
// read the column metadata
// read the offset of the column
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

	//c := 0 // hardcode just the first column for now
	//	schema := m.Schema[c+1]

	// dump columns names
	newSchema, err := parquet.SchemaFromFileMetaData(*m)
	if err != nil {
		return err
	}

	newSchema.MarshalDL(os.Stdout)

	for rowIdx, rg := range m.RowGroups {
		log.Printf("rowGroup: %d:%s\n", rowIdx, rg)

		for _, chunk := range rg.Columns {

			scanner := parquet.NewColumnScanner(r, chunk)

			for scanner.Scan() {

			}

			if err := scanner.Err(); err != nil {
				fmt.Printf("Invalid input: %s", err)
			}
		}
	}

	return nil
}
