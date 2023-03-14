package pg

import (
	"context"
	"database/sql"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"gopkg.in/gomisc/errors.v1"
	"gopkg.in/gomisc/tracing.v1"

	"gopkg.in/gomisc/storage.v1"
)

// DSN schemes
const (
	DefaultScheme = "postgres"
	ShortScheme   = "pg"
	PsqlScheme    = "psql"
)

const (
	errWrongQueryType  = errors.Const("query.Query must be string type")
	errWrongParameters = errors.Const("parameters must be []interface{} type")
)

var _ storage.Storage = (*databaseClient)(nil)

type (
	execResult struct {
		tag pgconn.CommandTag
		err error
	}

	databaseClient struct {
		pool *pgxpool.Pool
	}
)

func New(ctx context.Context, dsn string) (storage.Storage, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "configure database client")
	}

	poolConfig.ConnConfig.PreferSimpleProtocol = true

	var pool *pgxpool.Pool

	pool, err = pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgresql database")
	}

	return &databaseClient{pool: pool}, nil
}

// Close реализация io.Closer
func (cli *databaseClient) Close() error {
	cli.pool.Close()

	return nil
}

// Begin - открывает и возвращает транзакцию
func (cli *databaseClient) Begin(ctx context.Context, options ...any) (tx storage.Transaction, err error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	var pgTx pgx.Tx

	if opts := getPgTxOptions(options...); opts != nil {
		pgTx, err = cli.pool.BeginTx(span.Context(), *opts)
		if err != nil {
			err = errors.Ctx().Any("options", opts).Wrap(err, "begin transaction with opts")
			span, err = span.WithError(err)

			return nil, err
		}
	} else {
		pgTx, err = cli.pool.Begin(span.Context())
		if err != nil {
			span, err = span.WithError(err, "begin transaction")

			return nil, err
		}
	}

	return &pgTransaction{tx: pgTx, ctx: span.Context()}, nil
}

// Query - выполняет запрос производящий действия в базе, с возможностью вернуть произвольный результат
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

	var rows pgx.Rows

	if rows, err = cli.query(ctx, query); err != nil {
		span, err = span.WithError(err)
		return err
	}

	scanner := newScanner(rows)

	if res, ok := result.(*storage.Result); ok {
		if err = scanner.scanStorageResult(res); err != nil {
			span, err = span.WithError(err, "decode query result")
			return err
		}

		result = res
		return nil
	}

	if res, ok := result.(*storage.Table); ok {
		if err = scanner.scanStorageTable(res); err != nil {
			span, err = span.WithError(err, "decode query result")
			return err
		}

		result = res
		return nil
	}

	if err = scanner.scan(result); err != nil {
		span, err = span.WithError(err, "decode query result")
		return err
	}

	return nil
}

// Iterate - выполняет запрос и возвращает итератор по результатам произвольного типа из базы
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

// Exec Выполняет запрос который ничего не возвращает
func (cli *databaseClient) Exec(ctx context.Context, query storage.Query) (sql.Result, error) {
	span := tracing.SetTrace(ctx)
	defer span.End()

	msg, err := cli.exec(span.Context(), query)
	if err != nil {
		span, err = span.WithError(err, "execution error")

		return msg, err
	}

	return msg, nil
}

func (cli *databaseClient) getExecutor(ctx context.Context) pgxtype.Querier {
	if tx, ok := ctx.Value(transactionKey{}).(*pgTransaction); ok {
		return tx.tx
	}

	return cli.pool
}

func (res *execResult) LastInsertId() (int64, error) {
	return res.tag.RowsAffected(), res.err
}

func (res *execResult) RowsAffected() (int64, error) {
	return res.tag.RowsAffected(), res.err
}

func wrapPgErr(err error, message string) error {
	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) {
		return errors.Ctx().
			Pos(2).
			Str("code", pgErr.Code).
			Int32("sql-position", pgErr.Position).
			Wrap(err, message)
	}

	return errors.Wrap(err, message)
}
