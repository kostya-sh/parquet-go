package datatypes

import "testing"

func TestBuffer(*testing.T) {

	values := []interface{}{
		[]bool{},
		[]int64{},
		[]int32{},
		[]float32{},
		[]float64{},
		[][]byte{},
		[]Int96{},
	}

	for _, v := range values {
		NewBuffer(v)
	}

}
