package pg

import (
	"context"

	"github.com/jackc/pgx/v4"

	"git.corout.in/golibs/storage"
)

var _ storage.Iterator = (*postgresIterator)(nil)

type postgresIterator struct {
	rows pgx.Rows
}

func (iter *postgresIterator) Close() error {
	iter.rows.Close()

	return nil
}

func (iter *postgresIterator) Next(_ context.Context) bool {
	return iter.rows.Next()
}

func (iter *postgresIterator) Err() error {
	return iter.rows.Err()
}

func (iter *postgresIterator) Decode(result ...any) error {
	if err := iter.rows.Scan(result...); err != nil {
		return wrapPgErr(err, "decode item result")
	}

	return nil
}
