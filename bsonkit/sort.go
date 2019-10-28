package bsonkit

import "sort"

// Column defines a column for ordering.
type Column struct {
	// The path of the document field.
	Path string `bson:"path"`

	// Whether the ordering should be reverse.
	Reverse bool `bson:"reverse"`
}

// Sort will sort the list of documents in-place based on the specified columns.
func Sort(list List, columns []Column) {
	// sort slice by comparing values
	sort.Slice(list, func(i, j int) bool {
		return Order(list[i], list[j], columns) < 0
	})
}

// Order will return the order of  documents based on the specified columns.
func Order(l, r Doc, columns []Column) int {
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

	return 0
}
