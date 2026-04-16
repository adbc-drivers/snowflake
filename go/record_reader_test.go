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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBatch implements batchStreamer for testing.
type mockBatch struct {
	streams []func() (io.ReadCloser, error)
	call    int
}

func (m *mockBatch) GetStream(ctx context.Context) (io.ReadCloser, error) {
	if m.call >= len(m.streams) {
		return nil, fmt.Errorf("no more streams configured")
	}
	fn := m.streams[m.call]
	m.call++
	return fn()
}

// buildIPCBytes writes Arrow IPC record batches to a byte buffer.
func buildIPCBytes(alloc memory.Allocator, schema *arrow.Schema, records []arrow.RecordBatch) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	for _, rec := range records {
		_ = w.Write(rec)
	}
	_ = w.Close()
	return buf.Bytes()
}

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

func buildTestRecord(alloc memory.Allocator, schema *arrow.Schema, values []int64) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()
	for _, v := range values {
		bldr.Field(0).(*array.Int64Builder).Append(v)
	}
	return bldr.NewRecordBatch()
}

func identityTransform(_ context.Context, r arrow.RecordBatch) (arrow.RecordBatch, error) {
	r.Retain()
	return r, nil
}

func failingTransform(msg string) recordTransformer {
	return func(_ context.Context, r arrow.RecordBatch) (arrow.RecordBatch, error) {
		return nil, fmt.Errorf("%s", msg)
	}
}

func streamFromBytes(data []byte) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

func streamError(err error) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) {
		return nil, err
	}
}

// --- tryReadBatch tests ---

func TestTryReadBatch_Success(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1, 2, 3})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()

	assert.EqualValues(t, 3, recs[0].NumRows())
	col := recs[0].Column(0).(*array.Int64)
	assert.EqualValues(t, 1, col.Value(0))
	assert.EqualValues(t, 2, col.Value(1))
	assert.EqualValues(t, 3, col.Value(2))
}

func TestTryReadBatch_MultipleRecords(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec1 := buildTestRecord(alloc, schema, []int64{10, 20})
	defer rec1.Release()
	rec2 := buildTestRecord(alloc, schema, []int64{30, 40})
	defer rec2.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec1, rec2})
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.NoError(t, err)
	require.Len(t, recs, 2)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	assert.EqualValues(t, 2, recs[0].NumRows())
	assert.EqualValues(t, 2, recs[1].NumRows())
}

func TestTryReadBatch_EmptyStream(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	data := buildIPCBytes(alloc, schema, nil) // no records, just schema
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.NoError(t, err)
	assert.Empty(t, recs)
}

func TestTryReadBatch_GetStreamError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("network down")),
	}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network down")
	assert.Nil(t, recs)
}

func TestTryReadBatch_TransformError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, failingTransform("bad transform"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad transform")
	// partial recs may be returned; caller is responsible for releasing them
	for _, r := range recs {
		r.Release()
	}
}

func TestTryReadBatch_CancelledContext(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(ctx, batch, alloc, identityTransform)
	// Either GetStream or context check will surface the error
	if err != nil {
		for _, r := range recs {
			r.Release()
		}
		assert.ErrorIs(t, err, context.Canceled)
		return
	}
	for _, r := range recs {
		r.Release()
	}
}

// --- readBatchRecords tests ---

func TestReadBatchRecords_SuccessFirstAttempt(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{5, 6})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, identityTransform, 3)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()

	assert.EqualValues(t, 2, recs[0].NumRows())
}

func TestReadBatchRecords_SuccessAfterRetries(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{7, 8, 9})
	defer rec.Release()

	goodData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})

	// First two calls fail, third succeeds
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("fail 1")),
		streamError(fmt.Errorf("fail 2")),
		streamFromBytes(goodData),
	}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, identityTransform, 3)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()

	assert.EqualValues(t, 3, recs[0].NumRows())
}

func TestReadBatchRecords_ExhaustsRetries(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	maxRetries := 2
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("fail 1")),
		streamError(fmt.Errorf("fail 2")),
		streamError(fmt.Errorf("fail 3")),
	}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, identityTransform, maxRetries)
	require.Error(t, err)
	assert.Nil(t, recs)
	assert.Contains(t, err.Error(), "failed to read Arrow batch after 3 attempts")
	assert.Contains(t, err.Error(), "fail 3")
}

func TestReadBatchRecords_ZeroRetries(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("only chance")),
	}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, identityTransform, 0)
	require.Error(t, err)
	assert.Nil(t, recs)
	assert.Contains(t, err.Error(), "failed to read Arrow batch after 1 attempts")
}

func TestReadBatchRecords_CancelledContextSkipsRetries(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("should not reach")),
	}}

	recs, err := readBatchRecords(ctx, batch, alloc, identityTransform, 3)
	require.Error(t, err)
	assert.Nil(t, recs)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReadBatchRecords_PartialRecordsReleasedOnRetry(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()

	// Build a good IPC stream for the success case (one record)
	goodRec := buildTestRecord(alloc, schema, []int64{100})
	defer goodRec.Release()
	goodData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{goodRec})

	// First attempt: IPC stream with two records. Transform succeeds on
	// the first record but fails on the second, simulating a partial-read
	// scenario where readBatchRecords must release the already-accumulated
	// records before retrying.
	partialRec1 := buildTestRecord(alloc, schema, []int64{42})
	defer partialRec1.Release()
	partialRec2 := buildTestRecord(alloc, schema, []int64{43})
	defer partialRec2.Release()
	failData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{partialRec1, partialRec2})

	transformCall := 0
	failOnSecondRecord := func(ctx context.Context, r arrow.RecordBatch) (arrow.RecordBatch, error) {
		transformCall++
		if transformCall == 2 {
			// Fail on the second record of the first attempt
			return nil, fmt.Errorf("mid-stream failure")
		}
		r.Retain()
		return r, nil
	}

	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamFromBytes(failData),
		streamFromBytes(goodData),
	}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, failOnSecondRecord, 3)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()

	// The allocator check in defer will catch any leaked memory from the
	// partial records of the failed first attempt.
	assert.EqualValues(t, 1, recs[0].NumRows())
}
