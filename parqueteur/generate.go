package main

var cmdGenerate = &Command{
	Name: "gen",
	Help: "generate single column values ",
}

func init() {
	cmdGenerate.Run = runGenerate

	// cmdGenerate.Flag.StringVar(&dumpColumn, "c", "", "dump content of the named `column`")
	// cmdGenerate.Flag.BoolVar(&showLevels, "levels", false, "dump repetition and definition levels along with the column values")
}

// read The file metadata
// read the column metadata
// read the offset of the column
func runGenerate(cmd *Command, args []string) error {

	file := parquet.NewFile()

	return nil
}
