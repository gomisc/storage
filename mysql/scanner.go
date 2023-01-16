package mysql

import (
	"reflect"

	"git.eth4.dev/golibs/errors"
	"github.com/georgysavva/scany/sqlscan"
	"github.com/jmoiron/sqlx"

	"git.eth4.dev/golibs/storage"
)

type customScanner struct {
	rows *sqlx.Rows
}

func newScanner(rows *sqlx.Rows) storage.Scanner {
	return &customScanner{rows: rows}
}

func (scan *customScanner) Scan(result any) error {
	val := reflect.ValueOf(result)

	if val.Type().Elem().Kind() == reflect.Slice {
		if err := sqlscan.ScanAll(result, scan.rows.Rows); err != nil {
			return errors.Wrap(err, "scan objects to slice")
		}
	}

	if err := sqlscan.ScanOne(result, scan.rows.Rows); err != nil {
		return errors.Wrap(err, "scan row to object")
	}

	return nil
}

func (scan *customScanner) ScanResult(result *storage.Result) error {
	for scan.rows.Next() {
		row := make(map[string]any)

		if err := scan.rows.MapScan(row); err != nil {
			return errors.Wrap(err, "scan result row")
		}

		*result = append(*result, row)
	}

	return nil
}

func (scan *customScanner) ScanTable(table *storage.Table) error {
	fields, err := scan.rows.Columns()
	if err != nil {
		return errors.Wrap(err, "scan result table row")
	}

	for h := 0; h < len(fields); h++ {
		table.Headers = append(table.Headers, fields[h])
	}

	for scan.rows.Next() {
		var values []any

		values, err = scan.rows.SliceScan()
		if err != nil {
			return errors.Wrap(err, "get row values")
		}

		table.Rows = append(table.Rows, values)
	}

	return nil
}
