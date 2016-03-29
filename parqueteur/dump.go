package main

import (
	"fmt"
	"log"
	"math"

	"github.com/kostya-sh/parquet-go/parquet"
	"github.com/kostya-sh/parquet-go/parquet/datatypes"
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

	fd, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer fd.Close()

	rowGroup := make(map[string]datatypes.Accumulator)

	minValue := math.MaxInt32

	for _, col := range fd.Schema().Columns() {
		log.Printf("Reading %s", col)

		// will iterate across row groups
		scanner, err := fd.ColumnScanner(col)
		if err != nil {
			log.Printf("error reading %s: %s", col, err)
		}

		// provide a simple
		acc := scanner.NewAccumulator()

		// scans one chunk at the time
		for scanner.Scan() {
			// read all the data
			if err := scanner.Decode(acc); err != nil {
				log.Printf("error decoding %s", err)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("error reading %s: %s", col, err)
		}

		rowGroup[col] = acc

		if v := scanner.NumValues(); minValue > int(v) {
			minValue = int(v)
		}
	}

	for i := 0; i < minValue; i++ {
		for k, col := range rowGroup {
			if v, ok := col.Get(i); ok {
				fmt.Printf("%s: %#v \n", k, v)
			}
		}

		fmt.Printf("\n\n")
	}

	// for _, rowGroupScanner := range decoder.NewRowGroupScanner() {
	// 	for _, scanner := range rowGroupScanner.NewColumnScanners() {

	// 		for scanner.Scan() {

	// 		}

	// 		if err := scanner.Err(); err != nil {
	// 			fmt.Printf("%s: invalid input: %s\n", os.Args[0], err)
	// 		}
	// 	}
	// }

	return nil
}
