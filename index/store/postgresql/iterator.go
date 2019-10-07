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
)

// Iterator is an abstraction around key iteration
type Iterator struct {
	rows *sql.Rows

	key []byte
	val []byte

	prefix []byte

	valid bool
}

// Seek will advance the iterator to the specified key
func (i *Iterator) Seek(key []byte) {
	if i.key != nil && bytes.Compare(key, i.key) <= 0 || i.prefix != nil && bytes.Compare(key, i.prefix) < 0 {
		return
	}

	i.Next()

	if !i.valid || bytes.Compare(i.key, key) >= 0 {
		return
	}

	i.Seek(key)
}

// Next will advance the iterator to the next key
func (i *Iterator) Next() {
	if !i.rows.Next() {
		i.valid = false
		return
	}

	err := i.rows.Scan(&i.key, &i.val)
	if err != nil {
		i.valid = false
	}
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Key() []byte {
	return i.key
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Value() []byte {
	return i.val
}

// Valid returns whether or not the iterator is in a valid state
func (i *Iterator) Valid() bool {
	return i.valid
}

// Current returns Key(),Value(),Valid() in a single operation
func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.Key(), i.Value(), i.Valid()
}

// Close closes the iterator
func (i *Iterator) Close() error {
	return i.rows.Close()
}
