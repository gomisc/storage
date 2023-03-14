package mysql

import (
	"context"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/jmoiron/sqlx"
	"gopkg.in/gomisc/errors.v1"
)

type sqlIterator struct {
	rows    *sqlx.Rows
	scanner *sqlscan.RowScanner
}

func newIterator(rows *sqlx.Rows) *sqlIterator {
	return &sqlIterator{rows: rows}
}

func (it *sqlIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return errors.Wrap(err, "close iterator rows")
	}

	return nil
}

func (it *sqlIterator) Next(_ context.Context) bool {
	return it.rows.Next()
}

func (it *sqlIterator) Err() error {
	return it.rows.Err()
}

func (it *sqlIterator) Decode(result any) error {
	if err := it.scanner.Scan(result); err != nil {
		return errors.Wrap(err, "decode item result")
	}

	return nil
}
