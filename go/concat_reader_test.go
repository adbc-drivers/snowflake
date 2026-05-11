//
// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowflake

import (
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReaderIter struct {
	readers   []array.RecordReader
	nextCalls int
}

func (m *mockReaderIter) Release() {}

func (m *mockReaderIter) Next() (array.RecordReader, error) {
	m.nextCalls++
	if len(m.readers) == 0 {
		return nil, io.EOF
	}
	reader := m.readers[0]
	m.readers = m.readers[1:]
	return reader, nil
}

type mockRecordReader struct {
	schema *arrow.Schema
	next   []bool
	err    error
}

func (m *mockRecordReader) Release() {}

func (m *mockRecordReader) Retain() {}

func (m *mockRecordReader) Schema() *arrow.Schema {
	return m.schema
}

func (m *mockRecordReader) Next() bool {
	if len(m.next) == 0 {
		return false
	}
	result := m.next[0]
	m.next = m.next[1:]
	return result
}

func (m *mockRecordReader) Record() arrow.RecordBatch {
	return nil
}

func (m *mockRecordReader) RecordBatch() arrow.RecordBatch {
	return nil
}

func (m *mockRecordReader) Err() error {
	return m.err
}

func TestConcatReaderPreservesInnerReaderError(t *testing.T) {
	innerErr := errors.New("inner reader failed")
	iter := &mockReaderIter{
		readers: []array.RecordReader{
			&mockRecordReader{
				schema: testSchema(),
				next:   []bool{false},
				err:    innerErr,
			},
			&mockRecordReader{
				schema: testSchema(),
				next:   []bool{false},
			},
		},
	}

	rdr := &concatReader{}
	require.NoError(t, rdr.Init(iter))
	defer rdr.Release()

	assert.False(t, rdr.Next())
	require.Error(t, rdr.Err())
	assert.ErrorIs(t, rdr.Err(), innerErr)
	assert.Equal(t, 1, iter.nextCalls, "concatReader should not advance past a failing inner reader")
}
