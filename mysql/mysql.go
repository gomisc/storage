package mysql

import (
	"context"
	"database/sql"

	"git.eth4.dev/golibs/errors"
	"git.eth4.dev/golibs/tracing"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"git.eth4.dev/golibs/storage"
)

const (
	DefaultScheme = "mysql"

	errWrongQueryType  = errors.Const("query.Query must be string type")
	errWrongParameters = errors.Const("parameters must be []interface{} type")
)

type (
	databaseClient struct {
		pool *sqlx.DB
	}
)

func New(ctx context.Context, dsn string) (storage.Storage, error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	pool, err := sqlx.Open(DefaultScheme, dsn)
	if err != nil {
		span.WithError(err, "connect to mysql database failed")

		return nil, errors.Wrap(err, "connect to mysql database")
	}

	return &databaseClient{
		pool: pool,
	}, nil
}

func (cli *databaseClient) Close() error {
	if err := cli.pool.Close(); err != nil {
		return errors.Wrap(err, "close database connections")
	}

	return nil
}

func (cli *databaseClient) Begin(ctx context.Context, options ...any) (transaction storage.Transaction, err error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	var sqlTx *sqlx.Tx

	if opts := getSQLTxOptions(options...); opts != nil {
		sqlTx, err = cli.pool.BeginTxx(ctx, opts)
	} else {
		sqlTx, err = cli.pool.Beginx()
	}

	if err != nil {
		err = errors.Ctx().Wrap(err, "begin transaction")
		span, err = span.WithError(err)
		return nil, err
	}

	return &mysqlTransaction{tx: sqlTx, ctx: span.Context()}, nil
}

func (cli *databaseClient) Exec(ctx context.Context, query storage.Query) (sql.Result, error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	res, err := cli.exec(span.Context(), query)
	if err != nil {
		span, err = span.WithError(err, "execution error")

		return res, err
	}

	return res, nil
}

func (cli *databaseClient) Query(ctx context.Context, query storage.Query, result any) (err error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	if result == nil {
		if _, err = cli.exec(span.Context(), query); err != nil {
			span, err = span.WithError(err)
			return err
		}

		return nil
	}

	var rows *sqlx.Rows

	if rows, err = cli.query(ctx, query); err != nil {
		span, err = span.WithError(err)
		return err
	}

	scanner := newScanner(rows)

	if res, ok := result.(*storage.Result); ok {
		if err = scanner.ScanResult(res); err != nil {
			span, err = span.WithError(err, "decode to storage result")
			return err
		}

		result = res
		return nil
	}

	if res, ok := result.(*storage.Table); ok {
		if err = scanner.ScanTable(res); err != nil {
			span, err = span.WithError(err, "decode to storage table")
			return err
		}

		result = res
		return nil
	}

	if err = scanner.Scan(result); err != nil {
		span, err = span.WithError(err, "decode to custom result")
		return err
	}

	return nil
}

func (cli *databaseClient) Iterate(ctx context.Context, query storage.Query) (storage.Iterator, error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	rows, err := cli.query(span.Context(), query)
	if err != nil {
		span, err = span.WithError(err, "get iterable query result")

		return nil, err
	}

	return newIterator(rows), nil
}

func (cli *databaseClient) getExecutor(ctx context.Context) sqlx.ExtContext {
	if tx, ok := ctx.Value(transactionKey{}).(*mysqlTransaction); ok {
		return tx.tx
	}

	return cli.pool
}

func wrapMySQlErr(err error, message string) error {
	var mysqlErr *mysql.MySQLError

	if errors.As(err, &mysqlErr) {
		return errors.Ctx().
			Pos(2).
			Uint16("code", mysqlErr.Number).
			Str("message", mysqlErr.Message).
			Wrap(err, message)
	}

	return errors.Wrap(err, message)
}
