package main

import "fmt"

func format(v interface{}) string {
	if v == nil {
		return ""
	}
	switch a := v.(type) {
	case []byte:
		return string(a)
	default:
		return fmt.Sprintf("%v", v)
	}
}
