package main

import (
	"strings"
	"regexp"
	"fmt"
	"context"
	"net/http"
	"errors"
	"time"
	"database/sql"
	log "github.com/sirupsen/logrus"
)

// Transaction for nested scopes.
type Transaction struct {
	tx *sql.Tx
	commitKey int64
	err error
}

// Start starts a transaction with default isolation level.
// It returns a unique key required to commit the transaction. Returns 0 if the transaction is already started.
func (t *Transaction) Start(db *sql.DB) (int64, error) {
	t.err = errors.New("transaction did not complete properly")
	if t.commitKey == 0 {
		log.Debug("Creating new transaction")
		t.commitKey = time.Now().Unix()
		t.tx, t.err = db.Begin()
		//_, t.err = t.tx.Exec("set transaction isolation level serializable")
		return t.commitKey, t.err
	}
	return 0, t.err
}

// Commit commits the transaction.
// It requires the same key generated when the transaction was started. The action is ignored if 0 is provided.
func (t *Transaction) Commit(key int64) error {
	if t.commitKey == 0 {
		t.err = errors.New("transaction has not been started")
		return t.err
	} else if key == 0 {
		t.err = nil
		return nil
	} else if t.commitKey == key {
		t.err = nil
		t.commitKey = 0
		return t.tx.Commit()
	}
	t.err = errors.New("invalid key")
	return t.err
}

// Rollback aborts the transaction.
func (t *Transaction) Rollback(key int64) error {
	if key == 0 {
		return nil
	}
	if t.commitKey != 0 {
		t.commitKey = 0
		return t.tx.Rollback()
	}
	t.err = errors.New("transaction has not been started")
	return t.err
}

// Savepoint creates a Transaction savepoint
func (t *Transaction) Savepoint(savepoint string) error {
	if t.commitKey != 0 {
		re := regexp.MustCompile("[-]")
		savepoint = re.ReplaceAllLiteralString(savepoint, "_")
		_, t.err = t.tx.Exec(fmt.Sprintf("SAVEPOINT %s;", savepoint))
		return t.err
	}
	t.err = errors.New("transaction has not been started")
	return t.err
}

// RollbackToSavepoint reverts the Trasaction to a savepoint
func (t *Transaction) RollbackToSavepoint(savepoint string) error {
	if t.commitKey != 0 {
		_, t.err = t.tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s;", savepoint))
		return t.err
	}
	t.err = errors.New("transaction has not been started")
	return t.err
}

// Query executes a query that returns rows, typically a SELECT.
func (t *Transaction) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if t.commitKey != 0 {
		var rows *sql.Rows

		// [EXPERIMENTAL] Tries to automatically order queries by the first column
		if !strings.Contains(query, "order by") && !strings.Contains(query, ";") {
			query = fmt.Sprintf("select * from (%s) as t order by 1", query)
		}

		rows, t.err = t.tx.Query(query, args...)
		return rows, t.err
	}
	t.err = errors.New("transaction has not been started")
	return nil, t.err
}

// QueryRow executes a query that returns one row, typically a SELECT.
func (t *Transaction) QueryRow(query string, args ...interface{}) (*sql.Row) {
	if t.commitKey != 0 {
		row := t.tx.QueryRow(query, args...)
		return row
	}
	t.err = errors.New("transaction has not been started")
	return nil
}

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	if t.commitKey != 0 {
		var result sql.Result
		result, t.err = t.tx.Exec(query, args...)
		return result, t.err
	}
	t.err = errors.New("transaction has not been started")
	return nil, t.err
}

// Prepare creates a prepared statement for use within a Transaction.
func (t *Transaction) Prepare(query string) (*sql.Stmt, error) {
	if t.commitKey != 0 {
		return t.tx.Prepare(query)
	}
	t.err = errors.New("transaction has not been started")
	return nil, t.err
}

// Complete returns the state of the Transaction.
func (t *Transaction) Complete() (bool) {
	return t.err == nil
}

// Continue the Transaction setting compete as false.
func (t *Transaction) Continue() {
	t.err = errors.New("transaction did not complete properly")
}

// Report an error on the Transaction
func (t *Transaction) Report(message string) {
	t.err = errors.New(message)
}

// Error returns the latast error in the transaction
func (t *Transaction) Error() (error) {
	return t.err
}

// LoadTransaction loads a Transaction from an http context. Returns a new Transaction if none is found.
func LoadTransaction(r *http.Request, db *sql.DB) (*Transaction, int64, error) {
	if r.Context().Value("tx") != nil {
		log.Debug("Reusing transaction")
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