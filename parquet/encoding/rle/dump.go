// build +debug
package rle

// func dump(r io.Reader) io.Reader {
// 	return r
// 	b, err := ioutil.ReadAll(r)
// 	if err != nil {
// 		panic(err)
// 	}

// 	i := 0
// 	for i+3 < len(b) {
// 		fmt.Printf("%.4d: %.2x %.2x %.2x %.2x\n", i, b[i], b[i+1], b[i+2], b[i+3])
// 		i += 4
// 	}

// 	if i < len(b) {
// 		fmt.Printf("%.4d:", i)
// 		for j := i; j < len(b); j++ {
// 			fmt.Printf(" %.2x", b[j])
// 		}
// 		fmt.Printf("\n")
// 	}

// 	return bytes.NewReader(b)
// }
