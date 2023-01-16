package mysql

import (
	"context"
	"database/sql"

	"git.eth4.dev/golibs/errors"
	"git.eth4.dev/golibs/tracing"
	"github.com/jmoiron/sqlx"
)

type (
	transactionKey struct{}

	mysqlTransaction struct {
		tx  *sqlx.Tx
		ctx context.Context
	}
)

func (tx *mysqlTransaction) Context() context.Context {
	return context.WithValue(tx.ctx, transactionKey{}, tx)
}

func (tx *mysqlTransaction) Commit(ctx context.Context) error {
	span := tracing.SetTrace(ctx)
	defer span.End()

	if err := tx.tx.Commit(); err != nil {
		span.WithError(err, "commit transaction failed")

		return errors.Wrap(err, "commit transaction")
	}

	return nil
}

func (tx *mysqlTransaction) Rollback(ctx context.Context) error {
	span := tracing.SetTrace(ctx)
	defer span.End()

	if err := tx.tx.Rollback(); err != nil {
		span.WithError(err, "rollback transaction failed")

		return errors.Wrap(err, "rollback transaction")
	}

	return nil
}

func getSQLTxOptions(in ...any) *sql.TxOptions {
	if len(in) == 0 {
		return nil
	}

	if opts, ok := in[0].(*sql.TxOptions); ok {
		return opts
	}

	return nil
}
