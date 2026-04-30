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

// mockResettableBatch implements both batchStreamer and batchResetter.
// Reset() clears the cached stream, simulating what gosnowflake's
// ArrowStreamBatch.Reset() does (clears the cached rr field).
type mockResettableBatch struct {
	mockBatch
	resetCalls int
}

func (m *mockResettableBatch) Reset() error {
	m.resetCalls++
	return nil
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

// truncatedReader returns partial data then an error, simulating a TCP RST
type truncatedReader struct {
	data     []byte
	offset   int
	errAfter int
	err      error
}

func (t *truncatedReader) Read(p []byte) (int, error) {
	if t.offset >= t.errAfter {
		return 0, t.err
	}
	end := min(t.offset+len(p), t.errAfter)
	n := copy(p, t.data[t.offset:end])
	t.offset += n
	if t.offset >= t.errAfter {
		return n, t.err
	}
	return n, nil
}

func (t *truncatedReader) Close() error { return nil }

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

func TestTryReadBatch_TruncatedStream(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1, 2, 3})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})

	// Simulate a TCP RST after reading only half the data
	tcpErr := fmt.Errorf("read tcp: wsarecv: connection forcibly closed")
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		func() (io.ReadCloser, error) {
			return &truncatedReader{
				data:     data,
				errAfter: len(data) / 2,
				err:      tcpErr,
			}, nil
		},
	}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to buffer stream body")
	assert.Contains(t, err.Error(), "connection forcibly closed")
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

func TestReadBatchRecords_CancelledBeforeRetry(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	ctx, cancel := context.WithCancel(context.Background())

	// First call fails, context cancelled before retry
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		func() (io.ReadCloser, error) {
			cancel() // cancel after first failure
			return nil, fmt.Errorf("fail 1")
		},
		streamError(fmt.Errorf("should not reach")),
	}}

	recs, err := readBatchRecords(ctx, batch, alloc, identityTransform, 3)
	require.Error(t, err)
	assert.Nil(t, recs)
	assert.ErrorIs(t, err, context.Canceled)
}

// --- bufferBatchBody + batchResetter tests ---

func TestBufferBatchBody_CallsResetBeforeRetry(t *testing.T) {
	expected := []byte("good data")
	batch := &mockResettableBatch{
		mockBatch: mockBatch{streams: []func() (io.ReadCloser, error){
			streamError(fmt.Errorf("fail 1")),
			streamError(fmt.Errorf("fail 2")),
			func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(expected)), nil
			},
		}},
	}

	data, err := bufferBatchBody(context.Background(), batch, 3)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
	// Reset should have been called before attempt 1 and attempt 2 (not before attempt 0)
	assert.Equal(t, 2, batch.resetCalls)
}

func TestBufferBatchBody_NoResetOnFirstAttempt(t *testing.T) {
	expected := []byte("first try works")
	batch := &mockResettableBatch{
		mockBatch: mockBatch{streams: []func() (io.ReadCloser, error){
			func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(expected)), nil
			},
		}},
	}

	data, err := bufferBatchBody(context.Background(), batch, 3)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
	assert.Equal(t, 0, batch.resetCalls) // no retries needed, no Reset calls
}

func TestBufferBatchBody_ResetNotCalledWithoutInterface(t *testing.T) {
	// mockBatch does NOT implement batchResetter
	expected := []byte("good data")
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("fail")),
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(expected)), nil
		},
	}}

	data, err := bufferBatchBody(context.Background(), batch, 1)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
	// No panic, no error — gracefully skips Reset when not available
}

// --- countingReadCloser tests ---

func TestCountingReadCloser(t *testing.T) {
	data := []byte("hello world")
	rc := &countingReadCloser{inner: io.NopCloser(bytes.NewReader(data))}

	buf := make([]byte, 5)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.EqualValues(t, 5, rc.bytesRead)

	n, err = rc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.EqualValues(t, 10, rc.bytesRead)

	require.NoError(t, rc.Close())
}

// --- bufferBatchBody tests ---

func TestBufferBatchBody_Success(t *testing.T) {
	expected := []byte("hello arrow data")
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(expected)), nil
		},
	}}

	data, err := bufferBatchBody(context.Background(), batch, 3)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
}

func TestBufferBatchBody_SuccessAfterRetries(t *testing.T) {
	expected := []byte("good data")
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("net fail 1")),
		streamError(fmt.Errorf("net fail 2")),
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(expected)), nil
		},
	}}

	data, err := bufferBatchBody(context.Background(), batch, 3)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
}

func TestBufferBatchBody_ExhaustsRetries(t *testing.T) {
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("fail 1")),
		streamError(fmt.Errorf("fail 2")),
	}}

	data, err := bufferBatchBody(context.Background(), batch, 1)
	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "failed to buffer batch body after 2 attempts")
}

func TestBufferBatchBody_TruncatedStream(t *testing.T) {
	fullData := []byte("this is a fairly long payload that will be truncated")
	tcpErr := fmt.Errorf("read tcp: wsarecv: connection forcibly closed")
	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		func() (io.ReadCloser, error) {
			return &truncatedReader{
				data:     fullData,
				errAfter: 10,
				err:      tcpErr,
			}, nil
		},
	}}

	data, err := bufferBatchBody(context.Background(), batch, 0)
	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "failed to buffer stream body")
	assert.Contains(t, err.Error(), "connection forcibly closed")
}

func TestBufferBatchBody_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := &mockBatch{streams: []func() (io.ReadCloser, error){
		streamError(fmt.Errorf("should not reach")),
	}}

	data, err := bufferBatchBody(ctx, batch, 3)
	require.Error(t, err)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, context.Canceled)
}
