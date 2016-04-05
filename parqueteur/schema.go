package main

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet"
)

var cmdSchema = &Command{
	Name: "schema",
	Help: "display parquet file schema",
}

func init() {
	cmdSchema.Run = runSchema
}

func runSchema(cmd *Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("No files")
	}

	r, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = fmt.Println(r.Schema().DisplayString())
	return err
}
