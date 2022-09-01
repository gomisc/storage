package pg

import (
	"context"

	"git.corout.in/golibs/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"git.corout.in/golibs/storage"
)

type pgQuery struct {
	sql    string
	params []interface{}
}

func (cli *databaseClient) prepare(query storage.Query) (*pgQuery, error) {
	sql, isString := query.Query().(string)
	if !isString {
		return nil, errors.Ctx().Stringer("query", query).Just(errWrongQueryType)
	}

	params, ok := query.Params().([]interface{})
	if !ok {
		return nil, errors.Ctx().Any("params", query.Params()).Just(errWrongParameters)
	}

	return &pgQuery{sql: sql, params: params}, nil
}

func (cli *databaseClient) exec(ctx context.Context, query storage.Query) (string, error) {
	pq, err := cli.prepare(query)
	if err != nil {
		return "", errors.Wrap(err, "prepare query data")
	}

	var tag pgconn.CommandTag

	if tag, err = cli.getExecutor(ctx).Exec(ctx, pq.sql, pq.params...); err != nil {
		return "", wrapPgErr(err, "execute query")
	}

	return string(tag), nil
}

func (cli *databaseClient) queryRow(ctx context.Context, query storage.Query) (pgx.Row, error) {
	pq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query data")
	}

	return cli.getExecutor(ctx).QueryRow(ctx, pq.sql, pq.params...), nil
}

func (cli *databaseClient) query(ctx context.Context, query storage.Query) (pgx.Rows, error) {
	pq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query data")
	}

	var rows pgx.Rows

	rows, err = cli.getExecutor(ctx).Query(ctx, pq.sql, pq.params...)
	if err != nil {
		return nil, wrapPgErr(err, "execute query")
	}

	return rows, nil
}
