package pg

import (
	"reflect"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"gopkg.in/gomisc/errors.v1"

	"gopkg.in/gomisc/storage.v1"
)

type customScanner struct {
	rows pgx.Rows
}

func newScanner(rows pgx.Rows) *customScanner {
	return &customScanner{
		rows: rows,
	}
}

func (cs *customScanner) scan(result any) error {
	val := reflect.ValueOf(result)

	if val.Type().Elem().Kind() == reflect.Slice {
		if err := pgxscan.ScanAll(result, cs.rows); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrEmptyResult
			}

			return errors.Wrap(err, "scan to slice")
		}

		return nil
	}

	if err := pgxscan.ScanOne(result, cs.rows); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrEmptyResult
		}

		return errors.Wrap(err, "scan to object")
	}

	return nil
}

func (cs *customScanner) scanStorageTable(result *storage.Table) error {
	fields := cs.rows.FieldDescriptions()

	for h := 0; h < len(fields); h++ {
		result.Headers = append(result.Headers, string(fields[h].Name))
	}

	for cs.rows.Next() {
		values, err := cs.rows.Values()
		if err != nil {
			return errors.Wrap(err, "get row values")
		}

		result.Rows = append(result.Rows, values)
	}

	return nil
}

func (cs *customScanner) scanStorageResult(result *storage.Result) error {
	fields := cs.rows.FieldDescriptions()

	for cs.rows.Next() {
		values, err := cs.rows.Values()
		if err != nil {
			return errors.Wrap(err, "get row values")
		}

		row := make(map[string]any)
		for f, field := range fields {
			row[string(field.Name)] = values[f]
		}

		*result = append(*result, row)
	}

	return nil
}
