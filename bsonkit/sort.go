package bsonkit

import (
	"sort"
	"unsafe"
)

// Column defines a column for ordering.
type Column struct {
	Path    string
	Reverse bool
}

// Sort will sort the list of documents in-place based on the specified columns.
// Documents with equal column values retain their original (insertion) order,
// matching MongoDB's stable-sort semantics.
func Sort(list List, columns []Column) {
	sort.SliceStable(list, func(i, j int) bool {
		return Order(list[i], list[j], columns, false) < 0
	})
}

// Order will return the order of documents based on the specified columns.
func Order(l, r Doc, columns []Column, identity bool) int {
	for _, column := range columns {
		// get values
		a := Get(l, column.Path)
		b := Get(r, column.Path)

		// compare values
		res := Compare(a, b)

		// continue if equal
		if res == 0 {
			continue
		}

		// check if reverse
		if column.Reverse {
			return res * -1
		}

		return res
	}

	// return if identity should not be checked
	if !identity {
		return 0
	}

	// get addresses
	al := uintptr(unsafe.Pointer(l))
	ar := uintptr(unsafe.Pointer(r))

	// compare identity
	if al == ar {
		return 0
	} else if al < ar {
		return -1
	} else {
		return 1
	}
}
