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
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBatch implements batchStreamer for testing.
type mockBatch struct {
	streams []func(context.Context) (io.ReadCloser, error)
	call    int
	numRows int64
}

func (m *mockBatch) GetStream(ctx context.Context) (io.ReadCloser, error) {
	if m.call >= len(m.streams) {
		return nil, fmt.Errorf("no more streams configured")
	}
	fn := m.streams[m.call]
	m.call++
	return fn(ctx)
}

func (m *mockBatch) NumRows() int64 {
	return m.numRows
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

func truncateIPCStream(data []byte) []byte {
	if len(data) <= 8 {
		return append([]byte(nil), data...)
	}
	return append([]byte(nil), data[:len(data)-8]...)
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

func streamFromBytes(data []byte) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

func streamError(err error) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return nil, err
	}
}

type contextEOFStream struct {
	ctx context.Context

	prefix bytes.Reader

	blockOnce sync.Once
	blocked   chan struct{}
}

func newContextEOFStream(ctx context.Context, prefix []byte) *contextEOFStream {
	return &contextEOFStream{
		ctx:     ctx,
		prefix:  *bytes.NewReader(prefix),
		blocked: make(chan struct{}),
	}
}

func (s *contextEOFStream) Read(p []byte) (int, error) {
	if s.prefix.Len() > 0 {
		return s.prefix.Read(p)
	}
	s.blockOnce.Do(func() {
		close(s.blocked)
	})
	<-s.ctx.Done()
	return 0, io.EOF
}

func (s *contextEOFStream) Close() error {
	return nil
}

// --- tryReadBatch tests ---

func TestTryReadBatch_Success(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1, 2, 3})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{numRows: 3, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

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
	batch := &mockBatch{numRows: 4, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

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
	batch := &mockBatch{numRows: 0, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.NoError(t, err)
	assert.Empty(t, recs)
}

func TestTryReadBatch_GetStreamError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	batch := &mockBatch{streams: []func(context.Context) (io.ReadCloser, error){
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
	batch := &mockBatch{numRows: 1, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

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
	batch := &mockBatch{numRows: 1, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

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
	batch := &mockBatch{numRows: 2, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

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
	batch := &mockBatch{numRows: 3, streams: []func(context.Context) (io.ReadCloser, error){
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
	batch := &mockBatch{streams: []func(context.Context) (io.ReadCloser, error){
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

	batch := &mockBatch{streams: []func(context.Context) (io.ReadCloser, error){
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

	batch := &mockBatch{streams: []func(context.Context) (io.ReadCloser, error){
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

	batch := &mockBatch{numRows: 1, streams: []func(context.Context) (io.ReadCloser, error){
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

// TestFixedToFloat64Transformer covers the driver code path that was changed by
// the NUMBER(38, 11) fix.
//
// getTransformer wires fixedToFloat64Transformer for every FIXED Snowflake column
// with useHighPrecision=false and scale != 0. The function chooses between two
// internal paths based on precision:
//
//   - precision <= 15: scale the int64 in place via compute.Divide (the unscaled
//     value safely fits in float64).
//   - precision >  15: widen to Decimal128 first, because the unscaled int64 can
//     exceed 2^53 and a direct int64->float64 safe cast would fail with
//     "integer value ... not in range: -9007199254740992 to 9007199254740992".
//
// Before the fix the precision>15 path was gated by an extra "precision < 19"
// upper bound, so NUMBER(p, s) columns with p >= 19 (e.g. NUMBER(38, 11)) fell
// through to compute.Divide and crashed on any unscaled value > 2^53. This test
// exercises both branches of the guard fixedToFloat64Transformer now owns.
func TestFixedToFloat64Transformer(t *testing.T) {
	cases := []struct {
		name          string
		precision     int32
		scale         int32
		unscaledValue int64
		want          float64
	}{
		{
			// Regression case: NUMBER(38, 11) with unscaled value > 2^53
			// (9007199254740992). Pre-fix this crashed; post-fix it goes through
			// the Decimal128 intermediate path.
			name:          "precision38_unscaledExceeds2Pow53",
			precision:     38,
			scale:         11,
			unscaledValue: 42135425651100000,
			want:          421354.256511,
		},
		{
			// Happy path that was always working: precision <= 15 uses the
			// in-place compute.Divide path. Included to guard against the
			// refactor accidentally rerouting small precisions.
			name:          "precision10_compactValue",
			precision:     10,
			scale:         2,
			unscaledValue: 12345,
			want:          123.45,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)

			bldr := array.NewInt64Builder(alloc)
			defer bldr.Release()
			bldr.Append(tc.unscaledValue)
			int64Arr := bldr.NewInt64Array()
			defer int64Arr.Release()

			transformer := fixedToFloat64Transformer(tc.precision, tc.scale)
			out, err := transformer(context.Background(), int64Arr)
			require.NoError(t, err)
			defer out.Release()

			require.IsType(t, (*array.Float64)(nil), out)
			assert.InDelta(t, tc.want, out.(*array.Float64).Value(0), 1e-4)
		})
	}
}

func TestDecimalScale0ToInt64(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantErr bool
		want    int64
	}{
		{name: "zero", value: "0", want: 0},
		{name: "positive", value: "1", want: 1},
		{name: "negative", value: "-1", want: -1},
		{name: "int64Max", value: "9223372036854775807", want: math.MaxInt64},
		{name: "int64Min", value: "-9223372036854775808", want: math.MinInt64},
		{name: "int64MaxPlusOne", value: "9223372036854775808", wantErr: true},
		{name: "int64MinMinusOne", value: "-9223372036854775809", wantErr: true},
		{name: "farOverflow", value: "12345678901234567890123456789012345678", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)

			bldr := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 38, Scale: 0})
			defer bldr.Release()
			v, err := decimal.Decimal128FromString(tc.value, 38, 0)
			require.NoError(t, err)
			bldr.Append(v)
			bldr.AppendNull()
			arr := bldr.NewArray()
			defer arr.Release()

			ctx := compute.WithAllocator(context.Background(), alloc)
			out, err := decimalScale0ToInt64("col")(ctx, arr)
			if tc.wantErr {
				var adbcErr adbc.Error
				require.ErrorAs(t, err, &adbcErr)
				assert.Equal(t, adbc.StatusInvalidData, adbcErr.Code)
				assert.Nil(t, out)
				return
			}

			require.NoError(t, err)
			defer out.Release()
			require.IsType(t, (*array.Int64)(nil), out)
			result := out.(*array.Int64)
			require.Equal(t, 2, result.Len())
			assert.Equal(t, tc.want, result.Value(0))
			assert.True(t, result.IsNull(1))
		})
	}
}

func TestReadBatchRecords_RetriesAfterRowCountMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	shortRec := buildTestRecord(alloc, schema, []int64{1})
	defer shortRec.Release()
	goodRec := buildTestRecord(alloc, schema, []int64{1, 2})
	defer goodRec.Release()

	shortData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{shortRec})
	goodData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{goodRec})
	batch := &mockBatch{numRows: 2, streams: []func(context.Context) (io.ReadCloser, error){
		streamFromBytes(shortData),
		streamFromBytes(goodData),
	}}

	recs, err := readBatchRecords(context.Background(), batch, alloc, identityTransform, 1)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()

	assert.EqualValues(t, 2, recs[0].NumRows())
	assert.Equal(t, 2, batch.call)
}

func TestTryReadBatch_TruncatedStreamFailsRowCountValidation(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{numRows: 3, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch stream row count mismatch: expected 3 rows, got 1")
	for _, r := range recs {
		r.Release()
	}
}

func TestStreamBatchToChannel_TruncatedStreamFailsRowCountValidation(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{numRows: 3, streams: []func(context.Context) (io.ReadCloser, error){streamFromBytes(data)}}
	out := make(chan arrow.RecordBatch, 1)

	err := streamBatchToChannel(context.Background(), 2, batch, alloc, identityTransform, newBatchStreamTarget(2, batch, out, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch[2] row count mismatch: expected 3 rows, got 1")

	select {
	case rec := <-out:
		assert.EqualValues(t, 1, rec.NumRows())
		rec.Release()
	default:
		require.FailNow(t, "streamBatchToChannel should have emitted the partial record before reporting the row count mismatch")
	}
}

func TestValidateRowCount_UsesNeutralMismatchMessage(t *testing.T) {
	err := validateRowCount("result set", 5, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "result set row count mismatch: expected 5 rows, got 3")
	assert.NotContains(t, err.Error(), "ended early")
}

func TestStreamBatchToChannel_CancellationReturnsContextError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	schemaOnly := truncateIPCStream(buildIPCBytes(alloc, schema, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamCh := make(chan *contextEOFStream, 1)
	batch := &mockBatch{
		numRows: 0,
		streams: []func(context.Context) (io.ReadCloser, error){
			func(ctx context.Context) (io.ReadCloser, error) {
				stream := newContextEOFStream(ctx, schemaOnly)
				streamCh <- stream
				return stream, nil
			},
		},
	}

	out := make(chan arrow.RecordBatch, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- streamBatchToChannel(ctx, 0, batch, alloc, identityTransform, newBatchStreamTarget(0, batch, out, nil))
	}()

	stream := <-streamCh
	<-stream.blocked
	cancel()

	err := <-errCh
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReaderLateCancellationAfterLastRecordReturnsSuccess(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1})
	defer rec.Release()

	recordThenBlockAtEOF := truncateIPCStream(buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec}))

	ctx, cancel := context.WithCancel(context.Background())
	streamCh := make(chan *contextEOFStream, 1)
	chs := []chan arrow.RecordBatch{make(chan arrow.RecordBatch, 1)}
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		cancelFn: cancel,
		done:     make(chan struct{}),
	}

	batch := &mockBatch{
		numRows: 1,
		streams: []func(context.Context) (io.ReadCloser, error){
			func(ctx context.Context) (io.ReadCloser, error) {
				stream := newContextEOFStream(ctx, recordThenBlockAtEOF)
				streamCh <- stream
				return stream, nil
			},
		},
	}

	go func() {
		rdr.setErr(streamBatchToChannel(ctx, 0, batch, alloc, identityTransform, newBatchStreamTarget(0, batch, chs[0], nil)))
		close(chs[0])
		close(rdr.done)
	}()

	stream := <-streamCh
	require.True(t, rdr.Next(), "reader should emit the completed batch before cancellation")
	assert.EqualValues(t, 1, rdr.RecordBatch().NumRows())

	<-stream.blocked
	cancel()

	nextCh := make(chan bool, 1)
	go func() {
		nextCh <- rdr.Next()
	}()

	select {
	case got := <-nextCh:
		assert.False(t, got)
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "reader.Next should finish after late cancellation")
	}

	require.NoError(t, rdr.Err(), "late cancellation after the final batch should not surface as reader error")

	releaseDone := make(chan struct{})
	go func() {
		rdr.Release()
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "reader.Release should not block after late cancellation")
	}
}

func TestReaderStopsAfterEarlierBatchFailureDespiteLaterPrefetch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	failingRec := buildTestRecord(alloc, schema, []int64{1})
	defer failingRec.Release()
	laterRec := buildTestRecord(alloc, schema, []int64{2})
	defer laterRec.Release()

	shortData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{failingRec})
	laterData := buildIPCBytes(alloc, schema, []arrow.RecordBatch{laterRec})

	ctx, cancel := context.WithCancel(context.Background())
	chs := []chan arrow.RecordBatch{
		make(chan arrow.RecordBatch, 1),
		make(chan arrow.RecordBatch, 1),
	}
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		cancelFn: cancel,
		done:     make(chan struct{}),
	}

	failingBatch := &mockBatch{
		numRows: 2,
		streams: []func(context.Context) (io.ReadCloser, error){
			streamFromBytes(shortData),
		},
	}
	laterBatch := &mockBatch{
		numRows: 1,
		streams: []func(context.Context) (io.ReadCloser, error){
			streamFromBytes(laterData),
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := streamBatchToChannel(ctx, 1, laterBatch, alloc, identityTransform, newBatchStreamTarget(1, laterBatch, chs[1], nil))
		rdr.setErr(err)
		close(chs[1])
	}()

	require.Eventually(t, func() bool {
		return len(chs[1]) == 1
	}, 200*time.Millisecond, 10*time.Millisecond, "later batch should prefetch before the earlier batch fails")

	go func() {
		defer wg.Done()
		err := streamBatchToChannel(ctx, 0, failingBatch, alloc, identityTransform, newBatchStreamTarget(0, failingBatch, chs[0], nil))
		rdr.setErr(err)
		close(chs[0])
	}()

	go func() {
		wg.Wait()
		close(rdr.done)
	}()

	require.True(t, rdr.Next(), "reader should emit the partial record from the failing batch first")
	assert.EqualValues(t, 1, rdr.RecordBatch().NumRows())

	assert.False(t, rdr.Next(), "reader should stop before yielding rows from later prefetched batches")
	require.Error(t, rdr.Err())
	assert.Contains(t, rdr.Err().Error(), "batch[0] row count mismatch: expected 2 rows, got 1")
	assert.Len(t, chs[1], 1, "later prefetched rows should remain queued for release, not be returned by Next")

	releaseDone := make(chan struct{})
	go func() {
		rdr.Release()
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "reader.Release should not block after suppressing later prefetched rows")
	}
}

func TestReaderCancellationSetsErrBeforeNextAndReleaseReturns(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	schemaOnly := truncateIPCStream(buildIPCBytes(alloc, schema, nil))

	ctx, cancel := context.WithCancel(context.Background())
	streamCh := make(chan *contextEOFStream, 1)
	chs := []chan arrow.RecordBatch{make(chan arrow.RecordBatch)}
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		cancelFn: cancel,
		done:     make(chan struct{}),
	}

	batch := &mockBatch{
		numRows: 0,
		streams: []func(context.Context) (io.ReadCloser, error){
			func(ctx context.Context) (io.ReadCloser, error) {
				stream := newContextEOFStream(ctx, schemaOnly)
				streamCh <- stream
				return stream, nil
			},
		},
	}

	go func() {
		rdr.setErr(streamBatchToChannel(ctx, 0, batch, alloc, identityTransform, newBatchStreamTarget(0, batch, chs[0], nil)))
		close(chs[0])
		close(rdr.done)
	}()

	nextCh := make(chan bool, 1)
	go func() {
		nextCh <- rdr.Next()
	}()

	stream := <-streamCh
	<-stream.blocked
	cancel()

	select {
	case got := <-nextCh:
		assert.False(t, got)
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "reader.Next should not block after cancellation")
	}
	require.Error(t, rdr.Err())
	assert.ErrorIs(t, rdr.Err(), context.Canceled)

	releaseDone := make(chan struct{})
	go func() {
		rdr.Release()
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "reader.Release should not block after cancellation")
	}
}

// shortReadStream delivers data in chunks no larger than chunkSize per Read.
// If errAtOffset > 0, it returns injectedErr once that many bytes have been
// delivered, simulating mid-frame truncation (gosnowflake#1781).
type shortReadStream struct {
	data        []byte
	pos         int
	chunkSize   int
	errAtOffset int
	injectedErr error
}

func (s *shortReadStream) Read(p []byte) (int, error) {
	if s.errAtOffset > 0 && s.pos >= s.errAtOffset {
		return 0, s.injectedErr
	}
	if s.pos >= len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), s.chunkSize)
	remaining := len(s.data) - s.pos
	if n > remaining {
		n = remaining
	}
	if s.errAtOffset > 0 && s.pos+n > s.errAtOffset {
		n = s.errAtOffset - s.pos
	}
	copy(p, s.data[s.pos:s.pos+n])
	s.pos += n
	return n, nil
}

func (s *shortReadStream) Close() error { return nil }

func streamShortReads(data []byte, chunkSize int) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return &shortReadStream{data: data, chunkSize: chunkSize}, nil
	}
}

func streamShortReadsThenError(data []byte, chunkSize, errAtOffset int, err error) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return &shortReadStream{
			data:        data,
			chunkSize:   chunkSize,
			errAtOffset: errAtOffset,
			injectedErr: err,
		}, nil
	}
}

// Sanity check that legal short reads alone (n < len(p), nil err) don't break
// IPC decoding — the failure in gosnowflake#1781 requires a mid-frame error.
func TestTryReadBatch_ShortReadsSucceed(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := testSchema()
	rec := buildTestRecord(alloc, schema, []int64{1, 2, 3, 4, 5})
	defer rec.Release()

	data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
	batch := &mockBatch{numRows: 5, streams: []func(context.Context) (io.ReadCloser, error){
		streamShortReads(data, 1),
	}}

	recs, err := tryReadBatch(context.Background(), batch, alloc, identityTransform)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	defer recs[0].Release()
	assert.EqualValues(t, 5, recs[0].NumRows())
}

// Reproduces gosnowflake#1781 and validates the streamRetryEnabled flag:
// with retries disabled (matching the production "no retry" branch in
// newRecordReader) the mid-frame IPC error surfaces; with retries enabled
// the same broken first stream is recovered by a second attempt.
func TestStreamRetryEnabled_RecoversFromShortReadMidFrameError(t *testing.T) {
	schema := testSchema()

	cases := []struct {
		name               string
		streamRetryEnabled bool
		expectErr          bool
	}{
		{name: "disabled_surfacesIPCError", streamRetryEnabled: false, expectErr: true},
		{name: "enabled_recovers", streamRetryEnabled: true, expectErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)

			rec := buildTestRecord(alloc, schema, []int64{11, 22, 33, 44})
			defer rec.Release()

			data := buildIPCBytes(alloc, schema, []arrow.RecordBatch{rec})
			require.Greater(t, len(data), 32)
			truncationOffset := len(data) - 8

			batch := &mockBatch{numRows: 4, streams: []func(context.Context) (io.ReadCloser, error){
				streamShortReadsThenError(data, 16, truncationOffset, io.ErrUnexpectedEOF),
				streamShortReads(data, 16),
			}}

			// Mirror the production dispatch in newRecordReader.
			ctx := context.Background()
			var recs []arrow.RecordBatch
			var err error
			if tc.streamRetryEnabled {
				recs, err = readBatchRecords(ctx, batch, alloc, identityTransform, defaultStreamMaxRetries)
			} else {
				out := make(chan arrow.RecordBatch, 4)
				target := newBatchStreamTarget(0, batch, out, nil)
				err = streamBatchToChannel(ctx, 0, batch, alloc, identityTransform, target)
				close(out)
				for r := range out {
					recs = append(recs, r)
				}
			}

			if tc.expectErr {
				require.Error(t, err, "expected IPC error to surface without retry")
				msg := err.Error()
				assert.True(t,
					strings.Contains(msg, "could not read message body") ||
						strings.Contains(msg, "unexpected EOF") ||
						strings.Contains(msg, "row count mismatch"),
					"expected IPC body-read failure, got: %s", msg,
				)
				for _, r := range recs {
					r.Release()
				}
				return
			}

			require.NoError(t, err, "retry should recover from mid-frame truncation")
			require.Len(t, recs, 1)
			defer recs[0].Release()
			assert.EqualValues(t, 4, recs[0].NumRows())
			assert.Equal(t, 2, batch.call, "retry should have been invoked")
		})
	}
}
