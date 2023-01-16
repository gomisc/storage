package storage

type (
	Result []map[string]any

	Table struct {
		Headers []string
		Rows    [][]any
	}

	Scanner interface {
		Scan(result any) error
		ScanResult(result *Result) error
		ScanTable(table *Table) error
	}
)
