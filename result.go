package storage

type (
	Result []map[string]any

	Table struct {
		Headers []string
		Rows    [][]any
	}
)
