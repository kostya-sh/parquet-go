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
		return fmt.Errorf("%s: no files", args[0])
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

	// dump columns names
	newSchema, err := parquet.SchemaFromFileMetaData(*m)
	if err != nil {
		return err
	}

	newSchema.MarshalDL(os.Stdout)

	for _, rg := range m.GetRowGroups() {

		for c, chunk := range rg.Columns {

			log.Println(chunk.MetaData.GetPathInSchema(), chunk.MetaData.GetType(), chunk.MetaData.GetNumValues())

			log.Println(chunk.MetaData.GetEncodings())

			scanner := parquet.NewColumnScanner(r, chunk, m.Schema[c+1])

			for scanner.Scan() {

			}

			if err := scanner.Err(); err != nil {
				fmt.Printf("%s: invalid input: %s\n", os.Args[0], err)
			}
		}
	}

	return nil
}
