package main

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet"
)

var cmdSchema = &Command{
	Name: "schema",
	Help: "disaply parquet file schema",
}

func init() {
	cmdSchema.Run = runSchema
}

func runSchema(cmd *Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("No files")
	}

	f, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Println(f.Schema.DisplayString())
	return err
}
