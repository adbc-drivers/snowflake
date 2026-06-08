// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package snowflake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestBatchedParquetWithFileLimit(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ingestOpts := DefaultIngestOptions()
	parquetProps, arrowProps := newWriterProps(mem, &ingestOpts)

	nCols := 3
	nRecs := 10
	nRows := 1000
	targetFileSize := 10000

	rec := makeRec(mem, nCols, nRows)
	defer rec.Release()

	// Create a temporary parquet writer and write a single row group so we know
	// approximately how many bytes it should take
	tempWriter, err := pqarrow.NewFileWriter(rec.Schema(), &buf, parquetProps, arrowProps)
	require.NoError(t, err)

	// Write 1 record and check the size before closing so footer bytes are not included
	require.NoError(t, tempWriter.Write(rec))
	expectedRowGroupSize := buf.Len()
	require.NoError(t, tempWriter.Close())

	recs := make([]arrow.RecordBatch, nRecs)
	for i := range nRecs {
		recs[i] = rec
	}

	rdr, err := array.NewRecordReader(rec.Schema(), recs)
	require.NoError(t, err)
	defer rdr.Release()

	records := make(chan arrow.RecordBatch)
	go func() { assert.NoError(t, readRecords(ctx, rdr, records)) }()

	buf.Reset()
	// Expected to read multiple records but then stop after targetFileSize, indicated by nil error
	require.NoError(t, writeParquet(rdr.Schema(), &buf, records, targetFileSize, parquetProps, arrowProps))

	// Expect to exceed the targetFileSize but by no more than the size of 1 row group
	assert.Greater(t, buf.Len(), targetFileSize)
	assert.Less(t, buf.Len(), targetFileSize+expectedRowGroupSize)

	// Drain the remaining records with no limit on file size, expect EOF
	require.ErrorIs(t, writeParquet(rdr.Schema(), &buf, records, -1, parquetProps, arrowProps), io.EOF)
}

func TestQualifiedTableName(t *testing.T) {
	tests := []struct {
		name           string
		targetCatalog  string
		targetDbSchema string
		targetTable    string
		expected       string
	}{
		{
			name:        "table only",
			targetTable: "my_table",
			expected:    `"my_table"`,
		},
		{
			name:           "schema and table (2-part)",
			targetDbSchema: "my_schema",
			targetTable:    "my_table",
			expected:       `"my_schema"."my_table"`,
		},
		{
			name:           "catalog, schema, and table (3-part)",
			targetCatalog:  "my_catalog",
			targetDbSchema: "my_schema",
			targetTable:    "my_table",
			expected:       `"my_catalog"."my_schema"."my_table"`,
		},
		{
			name:          "catalog and table (no schema)",
			targetCatalog: "my_catalog",
			targetTable:   "my_table",
			expected:      `"my_catalog"."my_table"`,
		},
		{
			name:           "identifiers with special characters",
			targetCatalog:  `my"catalog`,
			targetDbSchema: `my"schema`,
			targetTable:    `my"table`,
			expected:       `"my""catalog"."my""schema"."my""table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &statement{
				targetCatalog:  tt.targetCatalog,
				targetDbSchema: tt.targetDbSchema,
				targetTable:    tt.targetTable,
			}
			assert.Equal(t, tt.expected, st.qualifiedTableName())
		})
	}
}

// TestReadRecordsRecoversFromSchemaMismatch exercises the panic-to-error
// contract of readRecords. A producer-supplied RecordReader whose advertised
// schema disagrees with the batch it yields will cause arrow-go's cdata
// import to panic inside Next(); readRecords must turn that into an ADBC
// error rather than letting it abort the host process.
func TestReadRecordsRecoversFromSchemaMismatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Reader advertises two columns...
	advertised := arrow.NewSchema([]arrow.Field{
		{Name: "name1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "name2", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// ...but the batch it actually produces only contains one.
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.Append("aaa")
	bldr.Append("bbb")
	col := bldr.NewArray()
	defer col.Release()

	batchSchema := arrow.NewSchema([]arrow.Field{
		{Name: "name1", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	batch := array.NewRecordBatch(batchSchema, []arrow.Array{col}, 2)
	defer batch.Release()

	src := &mismatchedReader{advertisedSchema: advertised, batch: batch}

	// Round-trip through the C Data interface so Next() goes through the
	// cdata import path where the panic on mismatch originates.
	var cstream cdata.CArrowArrayStream
	cdata.ExportRecordReader(src, &cstream)

	importedRdr, err := cdata.ImportCRecordReader(&cstream, advertised)
	require.NoError(t, err)
	imported, ok := importedRdr.(array.RecordReader)
	require.True(t, ok, "imported reader does not implement array.RecordReader: %T", importedRdr)
	defer imported.Release()

	// A regression would manifest as a panic propagating out of Next().
	out := make(chan arrow.RecordBatch, 1)
	go func() {
		for rec := range out {
			rec.Release()
		}
	}()
	err = readRecords(context.Background(), imported, out)

	require.Error(t, err, "expected a clean error from a mismatched stream")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	assert.Contains(t, adbcErr.Msg, "mismatch")
}

// mismatchedReader advertises one schema but yields a batch with a different
// column count. Used to drive the C Data import path through readRecords.
type mismatchedReader struct {
	advertisedSchema *arrow.Schema
	batch            arrow.RecordBatch
	emitted          bool
}

func (r *mismatchedReader) Retain()                        {}
func (r *mismatchedReader) Release()                       {}
func (r *mismatchedReader) Schema() *arrow.Schema          { return r.advertisedSchema }
func (r *mismatchedReader) Err() error                     { return nil }
func (r *mismatchedReader) RecordBatch() arrow.RecordBatch { return r.batch }
func (r *mismatchedReader) Record() arrow.RecordBatch      { return r.batch }
func (r *mismatchedReader) Next() bool {
	if r.emitted {
		return false
	}
	r.emitted = true
	return true
}

func makeRec(mem memory.Allocator, nCols, nRows int) arrow.RecordBatch {
	vals := make([]int8, nRows)
	for val := range nRows {
		vals[val] = int8(val)
	}

	bldr := array.NewInt8Builder(mem)
	defer bldr.Release()

	bldr.AppendValues(vals, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	fields := make([]arrow.Field, nCols)
	cols := make([]arrow.Array, nCols)
	for i := range nCols {
		fields[i] = arrow.Field{Name: fmt.Sprintf("field_%d", i), Type: arrow.PrimitiveTypes.Int8}
		cols[i] = arr // array.NewRecordBatch will retain these
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecordBatch(schema, cols, int64(nRows))
}

func TestParquetLargeList(t *testing.T) {
	// Test that upstream is broken
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "values",
			Type:     arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
			Nullable: true,
		},
	}, nil)
	batch := testutil.RecordFromJSON(t, mem, schema, `[{"values": [1, 2, 3]}, {"values": null}, {"values": [4, 5]}]`)
	ch := make(chan arrow.RecordBatch, 1)
	ch <- batch

	var buf bytes.Buffer
	parquetProps, arrowProps := newWriterProps(mem, new(DefaultIngestOptions()))

	err := writeParquet(batch.Schema(), &buf, ch, -1, parquetProps, arrowProps)
	require.ErrorContains(t, err, "type mismatch, column is int32 writer, arrow array is large_list, and not a compatible type")
}
