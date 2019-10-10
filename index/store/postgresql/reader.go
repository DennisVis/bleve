//  Copyright (c) 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresql

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/blevesearch/bleve/index/store"
	"github.com/lib/pq"
)

// Reader is an abstraction of an **ISOLATED** reader
// In this context isolated is defined to mean that
// writes/deletes made after the Reader is opened
// are not observed.
// Because there is usually a cost associated with
// keeping isolated readers active, users should
// close them as soon as they are no longer needed.
type Reader struct {
	db   *sql.DB
	tx   *sql.Tx
	rows *sql.Rows

	table  string
	keyCol string
	valCol string
}

// Get returns the value associated with the key
// If the key does not exist, nil is returned.
// The caller owns the bytes returned.
func (r *Reader) Get(key []byte) ([]byte, error) {
	r.Reset()

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = $1;",
		r.valCol,
		r.table,
		r.keyCol,
	)

	stmt, err := r.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for Get: %v", err)
		return nil, err
	}

	var bts []byte
	err = stmt.QueryRow(key).Scan(&bts)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil && err != sql.ErrNoRows {
		log.Printf("could not query row for Get: %v", err)
		return nil, err
	}

	rv := make([]byte, len(bts))
	copy(rv, bts)

	return rv, nil
}

// MultiGet retrieves multiple values in one call.
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	r.Reset()

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = ANY($1);",
		r.valCol,
		r.table,
		r.keyCol,
	)

	stmt, err := r.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for MultiGet: %v", err)
		return nil, err
	}

	r.rows, err = stmt.Query(pq.Array(keys))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Printf("could not query for MultiGet: %v", err)
		return nil, err
	}
	defer r.rows.Close()

	vals := make([][]byte, 0)
	for r.rows.Next() {
		var bts []byte

		err := r.rows.Scan(&bts)
		if err != nil {
			log.Printf("could not scan row for MultiGet: %v", err)
			return nil, err
		}

		val := make([]byte, len(bts))
		copy(val, bts)

		vals = append(vals, val)
	}

	return vals, nil
}

func doPrefixQuery(tx *sql.Tx, table, keyCol, valCol string, prefix []byte) (*sql.Rows, error) {
	if prefix != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s LIKE $1 ORDER BY %s;",
			keyCol,
			valCol,
			table,
			keyCol,
			keyCol,
		)

		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement in doPrefixQuery: %v", err)
			return nil, err
		}

		return stmt.Query(append(prefix, '%'))
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s ORDER BY %s;",
		keyCol,
		valCol,
		table,
		keyCol,
	)

	stmt, err := tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement in doPrefixQuery: %v", err)
		return nil, err
	}

	return stmt.Query()
}

func doRangeQuery(tx *sql.Tx, table, keyCol, valCol string, start, end []byte) (*sql.Rows, error) {
	if start != nil && end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY %s;",
			keyCol,
			valCol,
			table,
			keyCol,
			keyCol,
			keyCol,
		)

		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement in doRangeQuery: %v", err)
			return nil, err
		}

		return stmt.Query(start, end)
	}

	if start != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 ORDER BY %s;",
			keyCol,
			valCol,
			table,
			keyCol,
			keyCol,
		)

		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement in doRangeQuery: %v", err)
			return nil, err
		}

		return stmt.Query(start)
	}

	if end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s < $1 ORDER BY %s;",
			keyCol,
			valCol,
			table,
			keyCol,
			keyCol,
		)

		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement in doRangeQuery: %v", err)
			return nil, err
		}

		return stmt.Query(end)
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s ORDER BY %s;",
		keyCol,
		valCol,
		table,
		keyCol,
	)

	stmt, err := tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement in doRangeQuery: %v", err)
		return nil, err
	}

	return stmt.Query()
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	it := &Iterator{
		tx:   r.tx,
		rows: r.rows,
		DoQuery: func(tx *sql.Tx) (*sql.Rows, error) {
			return doPrefixQuery(tx, r.table, r.keyCol, r.valCol, prefix)
		},
	}

	it.Reset()

	return it
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	it := &Iterator{
		tx:   r.tx,
		rows: r.rows,
		DoQuery: func(tx *sql.Tx) (*sql.Rows, error) {
			return doRangeQuery(tx, r.table, r.keyCol, r.valCol, start, end)
		},
	}

	it.Reset()

	return it
}

// Reset frees resources for this reader and allows reuse
func (r *Reader) Reset() {
	if r.rows != nil {
		err := r.rows.Close()
		if err != nil {
			log.Printf("could not close rows in Reset: %v", err)
		}
		r.rows = nil
	}
}

// Close closes the reader
func (r *Reader) Close() error {
	err := r.tx.Rollback()
	if err != nil {
		log.Printf("could not tollback transaction in Close: %v", err)
	}
	return err
}
