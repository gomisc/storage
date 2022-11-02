package pg

import (
	"context"

	"git.eth4.dev/golibs/errors"
	"github.com/jackc/pgx/v4"
)

type transactionKey struct{}

type pgTransaction struct {
	ctx context.Context
	tx  pgx.Tx
}

func (tx *pgTransaction) Context() context.Context {
	return context.WithValue(tx.ctx, transactionKey{}, tx)
}

func (tx *pgTransaction) Commit(ctx context.Context) error {
	if err := tx.tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit transaction")
	}

	return nil
}

func (tx *pgTransaction) Rollback(ctx context.Context) error {
	if err := tx.tx.Rollback(ctx); err != nil {
		return errors.Wrap(err, "rollback transaction")
	}

	return nil
}

func getPgTxOptions(in ...any) *pgx.TxOptions {
	if len(in) == 0 {
		return nil
	}

	if opts, ok := in[0].(*pgx.TxOptions); ok {
		return opts
	}

	return nil
}
