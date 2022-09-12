package pg

import (
	"context"

	"git.corout.in/golibs/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"git.corout.in/golibs/tracing"

	"git.corout.in/golibs/storage"
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

type databaseClient struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (storage.Storage, error) {
	errCtx := errors.Ctx().Str("dsn", dsn)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errCtx.Wrap(err, "parse dsn")
	}

	var pool *pgxpool.Pool

	pool, err = pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.Ctx().Str("dsn", dsn).Wrap(err, "connect to postgresql database")
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
func (cli *databaseClient) Query(ctx context.Context, query storage.Query, result ...any) error {
	span := tracing.SetTrace(ctx)
	defer span.End()

	if len(result) == 0 {
		if _, err := cli.exec(span.Context(), query); err != nil {
			span, err = span.WithError(err)

			return err
		}

		return nil
	}

	row, err := cli.queryRow(span.Context(), query)
	if err != nil {
		span, err = span.WithError(err, "execute query")

		return err
	}

	if err = row.Scan(result...); err != nil {
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

	return &postgresIterator{rows: rows}, nil
}

// Exec Выполняет запрос который ничего не возвращает
func (cli *databaseClient) Exec(ctx context.Context, query storage.Query) (string, error) {
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
