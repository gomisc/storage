package mysql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gopkg.in/gomisc/errors.v1"

	"gopkg.in/gomisc/storage.v1"
)

type sqlQuery struct {
	sql    string
	params []any
}

func (cli *databaseClient) prepare(query storage.Query) (*sqlQuery, error) {
	sql, isString := query.Query().(string)
	if !isString {
		return nil, errors.Ctx().Stringer("query", query).Just(errWrongQueryType)
	}

	params, ok := query.Params().([]any)
	if !ok {
		return nil, errors.Ctx().Any("params", query.Params()).Just(errWrongParameters)
	}

	return &sqlQuery{sql: sql, params: params}, nil
}

func (cli *databaseClient) exec(ctx context.Context, query storage.Query) (sql.Result, error) {
	sq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query")
	}

	var res sql.Result

	if res, err = cli.getExecutor(ctx).ExecContext(ctx, sq.sql, sq.params...); err != nil {
		return nil, errors.Wrap(err, "execute query")
	}

	return res, nil
}

func (cli *databaseClient) queryRow(ctx context.Context, query storage.Query) (*sqlx.Row, error) {
	sq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query data")
	}

	return cli.getExecutor(ctx).QueryRowxContext(ctx, sq.sql, sq.params...), nil
}

func (cli *databaseClient) query(ctx context.Context, query storage.Query) (*sqlx.Rows, error) {
	sq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query data")
	}

	var rows *sqlx.Rows

	rows, err = cli.getExecutor(ctx).QueryxContext(ctx, sq.sql, sq.params...)
	if err != nil {
		return nil, wrapMySQlErr(err, "execute query")
	}

	return rows, nil
}
