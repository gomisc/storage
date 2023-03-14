package pg

import (
	"context"
	"database/sql"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"gopkg.in/gomisc/errors.v1"

	"gopkg.in/gomisc/storage.v1"
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

	params, ok := query.Params().([]any)
	if !ok {
		return nil, errors.Ctx().Any("params", query.Params()).Just(errWrongParameters)
	}

	return &pgQuery{sql: sql, params: params}, nil
}

func (cli *databaseClient) exec(ctx context.Context, query storage.Query) (sql.Result, error) {
	pq, err := cli.prepare(query)
	if err != nil {
		return nil, errors.Wrap(err, "prepare query data")
	}

	var (
		tag pgconn.CommandTag
	)

	if tag, err = cli.getExecutor(ctx).Exec(ctx, pq.sql, pq.params...); err != nil {
		return nil, wrapPgErr(err, "execute query")
	}

	return &execResult{tag: tag}, nil
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
