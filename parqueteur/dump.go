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

	decoder := parquet.NewDecoder(r)

	// log.Println(chunk.MetaData.GetPathInSchema(), chunk.MetaData.GetType(), chunk.MetaData.GetNumValues())

	for _, rowGroupScanner := range decoder.NewRowGroupScanner() {
		for _, scanner := range rowGroupScanner.NewColumnScanners() {

			for scanner.Scan() {

			}

			if err := scanner.Err(); err != nil {
				fmt.Printf("%s: invalid input: %s\n", os.Args[0], err)
			}
		}
	}

	return nil
}
