package main

import (
	"encoding/json"
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet"
	"github.com/kostya-sh/parquet-go/parquetformat"
)

var cmdPages = &Command{
	Name: "pages",
	Help: "print all page headers from a parquet file",
}

var pagesColumn string
var pagesFlagJSON bool

func init() {
	cmdPages.Run = runPages

	cmdPages.Flag.StringVar(&pagesColumn, "c", "", "show pages of the named `column`")
	cmdPages.Flag.BoolVar(&pagesFlagJSON, "json", false, "print result in JSON format")
}

func runPages(cmd *Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("No files")
	}

	f, err := parquet.OpenFile(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	col, found := f.Schema.ColumnByName(pagesColumn)
	if !found {
		return fmt.Errorf("no column named '%s' in schema", pagesColumn)
	}

	var allPages [][]*parquetformat.PageHeader
	for rg, _ := range f.MetaData.RowGroups {
		cr, err := f.NewReader(col, rg)
		if err != nil {
			return err
		}

		var rgPages []*parquetformat.PageHeader

		if cr.DictionaryPageHeader() != nil {
			rgPages = append(rgPages, cr.DictionaryPageHeader())
		}

		for {
			// TODO: think about empty column chunk with 0 pages
			rgPages = append(rgPages, cr.PageHeader())
			err = cr.SkipPage()
			if err == parquet.EndOfChunk {
				break
			}
			if err != nil {
				return err
			}
		}

		allPages = append(allPages, rgPages)
	}

	if pagesFlagJSON {
		b, err := json.MarshalIndent(allPages, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		// TODO: implement
		fmt.Printf("%+v\n", allPages)
	}

	return nil
}
