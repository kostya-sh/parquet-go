package main

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/kostya-sh/parquet-go/parquet"
)

func genBool(num int) []bool {
	r := make([]bool, num)
	for i := 0; i < num; i++ {
		r[i] = rand.Intn(100) > 50
	}
	return r
}

func TestBooleanColumn(t *testing.T) {

	schema := parquet.NewSchema()

	//	fd := parquet.NewFile("tempfile", s)

	//	fd.Close()

	err := schema.AddColumnFromSpec("value: boolean REQUIRED")
	if err != nil {
		t.Fatal(err)
	}

	// tmpfile, err := ioutil.TempFile("", "test_parquet")
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// defer os.Remove(tmpfile.Name()) // clean up

	var b bytes.Buffer

	enc := parquet.NewEncoder(schema, &b)

	values := genBool(100)
	if err := enc.WriteBool("value", values); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	fileName := "./boolean.parquet"

	if err := ioutil.WriteFile(fileName, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	// // launch external implementation
	// cmd := exec.Command("./parquet_reader", fileName)
	// stdout, err := cmd.StdoutPipe()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// if err := cmd.Start(); err != nil {
	// 	t.Fatal(err)
	// }

	// io.Copy(os.Stdout, stdout)

	// if err := cmd.Wait(); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := tmpfile.Close(); err != nil {
	// 	t.Fatal(err)
	// }

}
