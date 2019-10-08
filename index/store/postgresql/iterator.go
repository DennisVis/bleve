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
	"bytes"
	"database/sql"
	"log"
)

// Iterator is an abstraction around key iteration
type Iterator struct {
	stmt *sql.Stmt
	args []interface{}

	rows *sql.Rows

	key []byte
	val []byte

	err error
}

func NewIterator(stmt *sql.Stmt, args ...interface{}) (*Iterator, error) {
	rows, err := stmt.Query(args...)
	if err != nil {
		log.Printf("could not execute statement in NewIterator: %v", err)
		return nil, err
	}

	it := &Iterator{
		stmt: stmt,
		args: args,
		rows: rows,
	}

	it.Next()

	return it, nil
}

// Seek will advance the iterator to the specified key
func (i *Iterator) Seek(key []byte) {
	if i.key != nil && bytes.Compare(key, i.key) <= 0 {
		return
	}

	i.Close()

	i.rows, i.err = i.stmt.Query(i.args...)
	if i.err != nil {
		log.Printf("could not execute statement in Seek: %err", i.err)
		return
	}

	for ; i.Valid(); i.Next() {
		if bytes.Compare(i.key, key) >= 0 {
			break
		}
	}
}

// Next will advance the iterator to the next key
func (i *Iterator) Next() {
	if !i.rows.Next() {
		i.err = sql.ErrNoRows
		return
	}

	i.err = i.rows.Scan(&i.key, &i.val)
	if i.err != nil {
		log.Printf("could not scan row as key/val in Next: %err", i.err)
	}
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Key() []byte {
	if i.Valid() {
		return i.key
	}
	return nil
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Value() []byte {
	if i.Valid() {
		return i.val
	}
	return nil
}

// Valid returns whether or not the iterator is in a valid state
func (i *Iterator) Valid() bool {
	return i.err == nil
}

// Current returns Key(),Value(),Valid() in a single operation
func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.Key(), i.Value(), i.Valid()
}

// Close closes the iterator
func (i *Iterator) Close() error {
	if i.rows != nil {
		return i.rows.Close()
	}
	return nil
}
