package main

import (
	"fmt"
	"os"

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

	r, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer r.Close()

	meta, err := parquet.ReadFileMetaData(r)
	if err != nil {
		return err
	}

	// fmt.Printf("%+v\n\n", meta.Schema)

	schema, err := parquet.SchemaFromFileMetaData(meta)
	if err != nil {
		return err
	}
	return schema.MarshalDL(os.Stdout)
}
