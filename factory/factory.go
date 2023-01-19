package factory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"git.eth4.dev/golibs/errors"

	"git.eth4.dev/golibs/storage"
	"git.eth4.dev/golibs/storage/mysql"
	"git.eth4.dev/golibs/storage/pg"
)

const (
	errUnsupportedDriver = errors.Const("unsupported database driver")
	errDatabaseName      = errors.Const("database name not found in dsn")
)

type driversFactory struct {
	ctx context.Context

	sync.RWMutex
	drivers map[string]storage.Storage
}

// New конструктор фабрики драйверов баз данных
func New(ctx context.Context) storage.Factory {
	return &driversFactory{
		ctx:     ctx,
		drivers: make(map[string]storage.Storage),
	}
}

// Storage - возвращает расширение интерфейса клиента базы данных
func (f *driversFactory) Storage(dsn string) (storage.Storage, error) {
	if driver, ok := f.get(dsn); ok {
		return driver, nil
	}

	driver, err := f.create(dsn)
	if err != nil {
		return nil, errors.Ctx().
			Str("dsn", dsn).
			Wrap(err, "create client connection")
	}

	return driver, nil
}

func (f *driversFactory) get(dsn string) (storage.Storage, bool) {
	f.RLock()

	defer f.RUnlock()

	if driver, ok := f.drivers[dsn]; ok {
		return driver, ok
	}

	return nil, false
}

func (f *driversFactory) create(dsn string) (storage.Storage, error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "parse dsn")
	}

	errCtx := errors.Ctx().Str("uri", uri.Redacted())
	parts := strings.Split(uri.Path, "/")
	if len(parts) == 0 {
		return nil, errCtx.Just(errDatabaseName)
	}

	var driver storage.Storage

	switch uri.Scheme {
	case pg.DefaultScheme, pg.PsqlScheme, pg.ShortScheme:
		uri.Scheme = pg.DefaultScheme

		driver, err = pg.New(f.ctx, uri.String())
		if err != nil {
			return nil, errCtx.Wrap(err, "create postgres connection")
		}
	case mysql.DefaultScheme:
		paswd, _ := uri.User.Password()

		driver, err = mysql.New(f.ctx, fmt.Sprintf("%s:%s@tcp(%s)/%s?%s",
			uri.User.Username(),
			paswd,
			uri.Host,
			uri.Path,
			uri.RawQuery,
		))
		if err != nil {
			return nil, errCtx.Wrap(err, "create mysql connection")
		}
	default:
		return nil, errUnsupportedDriver
	}

	f.Lock()
	defer f.Unlock()

	f.drivers[dsn] = driver

	return driver, nil
}
