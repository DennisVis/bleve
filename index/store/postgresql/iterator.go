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
	DoQuery func(*sql.Tx) (*sql.Rows, error)

	tx   *sql.Tx
	rows *sql.Rows

	key []byte
	val []byte

	err error
}

// Seek will advance the iterator to the specified key
func (i *Iterator) Seek(key []byte) {
	for i.Reset(); i.Valid(); i.Next() {
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

// Reset frees resources for this iterator and allows reuse
func (i *Iterator) Reset() {
	i.Close()

	i.rows, i.err = i.DoQuery(i.tx)
	if i.err != nil {
		log.Printf("could not execute statement in Reset: %err", i.err)
	}

	i.Next()
}

// Close closes the iterator
func (i *Iterator) Close() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			log.Printf("could not close rows in Close: %v", err)
			return err
		}
	}

	i.rows = nil
	i.err = nil

	return nil
}
