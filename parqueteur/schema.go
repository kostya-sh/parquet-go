package main

var cmdSchema = &Command{
	Name: "schema",
	Help: "display parquet file schema",
}

func init() {
	//cmdSchema.Run = runSchema
}

// func runSchema(cmd *Command, args []string) error {
// 	if len(args) != 1 {
// 		return fmt.Errorf("No files")
// 	}

// 	r, err := os.Open(args[0])
// 	if err != nil {
// 		return err
// 	}
// 	defer r.Close()

// 	meta, err := readFileMetaData(r)
// 	if err != nil {
// 		return err
// 	}

// 	schema, err := schemaFromFileMetaData(meta)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = fmt.Println(schema.DisplayString())
// 	return err
// }
