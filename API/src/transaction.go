package main

import (
	"context"
	"net/http"
	"errors"
	"time"
	"database/sql"
)

// Transaction for nested scopes.
type Transaction struct {
	tx *sql.Tx
	commitKey int64
	complete bool
}

// Start starts a transaction with default isolation level.
// It returns a unique key required to commit the transaction. Returns 0 if the transaction is already started.
func (t *Transaction) Start(db *sql.DB) (int64, error) {
	t.complete = false
	if t.commitKey == 0 {
		t.commitKey = time.Now().Unix()
		var err error
		t.tx, err = db.Begin()
		return t.commitKey, err
	}
	return 0, nil
}

// Commit commits the transaction.
// It requires the same key generated when the transaction was started. The action is ignored if 0 is provided.
func (t *Transaction) Commit(key int64) error {
	if t.commitKey == 0 {
		err := errors.New("transaction has not been started")
		return err
	} else if key == 0 {
		t.complete = true
		return nil
	} else if t.commitKey == key {
		t.complete = true
		t.commitKey = 0
		return t.tx.Commit()
	}
	err := errors.New("invalid key")
	return err
}

// Rollback aborts the transaction.
func (t *Transaction) Rollback() error {
	if t.commitKey != 0 {
		t.commitKey = 0
		return t.tx.Rollback()
	}
	err := errors.New("transaction has not been started")
	return err
}

// Query executes a query that returns rows, typically a SELECT.
func (t *Transaction) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if t.commitKey != 0 {
		return t.tx.Query(query, args...)
	}
	err := errors.New("transaction has not been started")
	return nil, err
}

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	if t.commitKey != 0 {
		return t.tx.Exec(query, args...)
	}
	err := errors.New("transaction has not been started")
	return nil, err
}

// Complete returns the state of the Transaction.
func (t *Transaction) Complete() (bool) {
	return t.complete
}

// LoadTransaction loads a Transaction from an http context. Returns a new Transaction if none is found.
func LoadTransaction(r *http.Request, db *sql.DB) (*Transaction, int64, error) {
	if r.Context().Value("tx") != nil {
		tx := r.Context().Value("tx").(*Transaction)
		return tx, 0, nil
	}
	var newTx Transaction
	key, err := newTx.Start(db)
	return &newTx, key, err
}

// WithTransaction starts a new Transaction and add it to an HTTP context
func WithTransaction(r *http.Request, tx *Transaction) (*http.Request) {
	var ctx context.Context
	ctx = context.WithValue(ctx, "tx", tx)
	return r.WithContext(ctx)
}