package main

var cmdMeta = &Command{
	Name: "meta",
	Help: "display parquet file meta data",
}

var metaFlagJSON bool

func init() {
	//	cmdMeta.Run = runMeta

	cmdMeta.Flag.BoolVar(&metaFlagJSON, "json", false, "print result in JSON format")
}

// func runMeta(cmd *Command, args []string) error {
// 	if len(args) != 1 {
// 		return fmt.Errorf("No files")
// 	}

// 	r, err := os.Open(args[0])
// 	if err != nil {
// 		return err
// 	}
// 	defer r.Close()

// 	m, err := parquet.ReadFileMetaData(r)
// 	if err != nil {
// 		return err
// 	}

// 	if metaFlagJSON {
// 		b, err := json.MarshalIndent(m, "", "  ")
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Println(string(b))
// 	} else {
// 		// TODO: implement
// 		fmt.Printf("%+v\n", m)
// 	}
// 	return nil
// }
