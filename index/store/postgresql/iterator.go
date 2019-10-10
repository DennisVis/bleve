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
)

type Item struct {
	K []byte
	V []byte
}

// Iterator is an abstraction around key iteration
type Iterator struct {
	items []Item
	index int
}

// Seek will advance the iterator to the specified key
func (i *Iterator) Seek(key []byte) {
	i.index = 0
	for ; i.Valid(); i.Next() {
		if bytes.Compare(i.items[i.index].K, key) >= 0 {
			break
		}
	}
}

// Next will advance the iterator to the next key
func (i *Iterator) Next() {
	i.index++
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Key() []byte {
	if i.Valid() {
		return i.items[i.index].K
	}
	return nil
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Value() []byte {
	if i.Valid() {
		return i.items[i.index].V
	}
	return nil
}

// Valid returns whether or not the iterator is in a valid state
func (i *Iterator) Valid() bool {
	return i.index < len(i.items)
}

// Current returns Key(),Value(),Valid() in a single operation
func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.Key(), i.Value(), i.Valid()
}

// Close closes the iterator
func (i *Iterator) Close() error {
	i.index = 0
	i.items = nil
	return nil
}
