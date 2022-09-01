package storage

import (
	"context"
	"fmt"
	"io"
)

type (
	Query interface {
		fmt.Stringer
		// Query - возвращает запрос
		Query() interface{}
		// Params Возвращает параметры запроса
		Params() interface{}
	}

	// Transaction интерфейс транзакции базы данных
	Transaction interface {
		// Context - возвращает контекст транзакции
		Context() context.Context
		// Commit Фиксирует текущую транзакцию
		Commit(ctx context.Context) error
		// Rollback Откатывает текущую транзакцию
		Rollback(ctx context.Context) error
	}

	// Iterator интерфейс итератора по многоэлементному результату запроса
	Iterator interface {
		io.Closer
		// Next - перемещает курсор итератора на следующий элемент
		Next(ctx context.Context) bool
		// Err - возвращает ошибку итератора, если такая имела место
		Err() error
		// Decode приводит значение текущего элемента итерации к указанному типу
		Decode(result interface{}) error
	}

	Storage interface {
		// Begin Стартует и возвращает новую транзакцию
		Begin(ctx context.Context, opts ...any) (transaction Transaction, err error)
		// Query - выполняет запрос производящий действия в базе, с возможностью вернуть произвольный результат
		Query(ctx context.Context, query Query, result any) error
		// Iterate возвращает итератор по результату запроса
		Iterate(ctx context.Context, query Query) (Iterator, error)
		// Exec Выполняет запрос который ничего не возвращает
		Exec(ctx context.Context, query Query) (string, error)
	}

	// Factory - абстрактная фабрика клиентов
	Factory interface {
		// Storage - возвращает клиента базы данных
		Storage(dsn string) (Storage, error)
	}
)
