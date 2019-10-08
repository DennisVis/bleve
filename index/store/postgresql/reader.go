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
	tx *sql.Tx

	table  string
	keyCol string
	valCol string
}

// Get returns the value associated with the key
// If the key does not exist, nil is returned.
// The caller owns the bytes returned.
func (r *Reader) Get(key []byte) ([]byte, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = $1;",
		r.valCol,
		r.table,
		r.keyCol,
	)

	var bts []byte

	stmt, err := r.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for Get: %v", err)
		return nil, err
	}

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

	rows, err := stmt.Query(pq.Array(keys))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Printf("could not query for MultiGet: %v", err)
		return nil, err
	}
	defer rows.Close()

	vals := make([][]byte, 0)
	for rows.Next() {
		var bts []byte

		err := rows.Scan(&bts)
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

func (r *Reader) createPrefixStmt(prefix []byte) (*sql.Stmt, []interface{}, error) {
	if prefix != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s LIKE $1 ORDER BY %s;",
			r.keyCol,
			r.valCol,
			r.table,
			r.keyCol,
			r.keyCol,
		)

		stmt, err := r.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for PrefixIterator: %v", err)
			return nil, nil, err
		}

		return stmt, []interface{}{append(prefix, '%')}, nil
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s ORDER BY %s;",
		r.keyCol,
		r.valCol,
		r.table,
		r.keyCol,
	)

	stmt, err := r.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for PrefixIterator: %v", err)
		return nil, nil, err
	}

	return stmt, []interface{}{}, nil
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	stmt, args, err := r.createPrefixStmt(prefix)
	if err != nil {
		return nil
	}

	it, err := NewIterator(stmt, args...)
	if err != nil {
		log.Printf("could not create iterator in PrefixIterator: %v", err)
		return nil
	}

	return it
}

func (r *Reader) createRangeStmt(start, end []byte) (*sql.Stmt, []interface{}, error) {
	if start != nil && end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY %s;",
			r.keyCol,
			r.valCol,
			r.table,
			r.keyCol,
			r.keyCol,
			r.keyCol,
		)

		stmt, err := r.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for RangeIterator: %v", err)
			return nil, nil, err
		}

		return stmt, []interface{}{start, end}, nil
	}

	if start != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 ORDER BY %s;",
			r.keyCol,
			r.valCol,
			r.table,
			r.keyCol,
			r.keyCol,
		)

		stmt, err := r.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for RangeIterator: %v", err)
			return nil, nil, err
		}

		return stmt, []interface{}{start}, nil
	}

	if end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s < $1 ORDER BY %s;",
			r.keyCol,
			r.valCol,
			r.table,
			r.keyCol,
			r.keyCol,
		)

		stmt, err := r.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for RangeIterator: %v", err)
			return nil, nil, err
		}

		return stmt, []interface{}{end}, nil
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s ORDER BY %s;",
		r.keyCol,
		r.valCol,
		r.table,
		r.keyCol,
	)

	stmt, err := r.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for RangeIterator: %v", err)
		return nil, nil, err
	}

	return stmt, []interface{}{}, nil
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	stmt, args, err := r.createRangeStmt(start, end)
	if err != nil {
		return nil
	}

	it, err := NewIterator(stmt, args...)
	if err != nil {
		log.Printf("could not create iterator in PrefixIterator: %v", err)
		return nil
	}

	return it
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.tx.Rollback()
}
