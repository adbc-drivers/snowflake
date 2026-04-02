// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowflake

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertTimestampNanosecondUTC(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	now := time.Date(2025, 6, 15, 10, 30, 0, 123456789, time.UTC)
	tsVal, err := arrow.TimestampFromTime(now, arrow.Nanosecond)
	require.NoError(t, err)
	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{tsVal}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	require.Len(t, params, 1)

	nt, ok := params[0].Value.(sql.NullTime)
	require.True(t, ok)
	assert.True(t, nt.Valid)
	assert.True(t, now.Equal(nt.Time), "expected %v, got %v", now, nt.Time)
	assert.Equal(t, 1, params[0].Ordinal)
}

func TestConvertTimestampMicrosecondWithTimezone(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts_tz", Type: tsType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	now := time.Date(2025, 6, 15, 10, 30, 0, 0, loc)
	tsVal, err := arrow.TimestampFromTime(now, arrow.Microsecond)
	require.NoError(t, err)
	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{tsVal}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	require.Len(t, params, 1)

	nt, ok := params[0].Value.(sql.NullTime)
	require.True(t, ok)
	assert.True(t, nt.Valid)
	assert.True(t, now.Equal(nt.Time), "expected %v, got %v", now, nt.Time)
	assert.Equal(t, "America/New_York", nt.Time.Location().String())
}

func TestConvertTimestampSecond(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Second}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts_s", Type: tsType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tsVal, err := arrow.TimestampFromTime(now, arrow.Second)
	require.NoError(t, err)
	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{tsVal}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.True(t, now.Equal(nt.Time))
}

func TestConvertTimestampNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Microsecond}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.TimestampBuilder).AppendNull()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.False(t, nt.Valid)
}

func TestConvertBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	expected := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	bldr.Field(0).(*array.BinaryBuilder).Append(expected)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	require.Len(t, params, 1)

	val, ok := params[0].Value.(string)
	require.True(t, ok)
	assert.Equal(t, "deadbeef", val)
	assert.Equal(t, 1, params[0].Ordinal)
}

func TestConvertBinaryNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.BinaryBuilder).AppendNull()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	assert.Nil(t, params[0].Value)
}

func TestConvertLargeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "lbin", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	expected := []byte{0xCA, 0xFE, 0xBA, 0xBE}
	bldr.Field(0).(*array.BinaryBuilder).Append(expected)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	val, ok := params[0].Value.(string)
	require.True(t, ok)
	assert.Equal(t, "cafebabe", val)
}

func TestConvertDecimal128WholeNumber(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "dec", Type: dt, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(12345))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	require.Len(t, params, 1)

	ns, ok := params[0].Value.(sql.NullString)
	require.True(t, ok)
	assert.True(t, ns.Valid)
	assert.Equal(t, "12345", ns.String)
	assert.Equal(t, 1, params[0].Ordinal)
}

func TestConvertDecimal128Fractional(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "dec", Type: dt, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	num, err := decimal128.FromString("456.78", 38, 2)
	require.NoError(t, err)
	bldr.Field(0).(*array.Decimal128Builder).Append(num)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	ns := params[0].Value.(sql.NullString)
	assert.True(t, ns.Valid)
	assert.Equal(t, "456.78", ns.String)
}

func TestConvertDecimal128Null(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "dec", Type: dt, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Decimal128Builder).AppendNull()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	ns := params[0].Value.(sql.NullString)
	assert.False(t, ns.Valid)
}

func TestConvertDecimal256(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.Decimal256Type{Precision: 50, Scale: 3}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "dec256", Type: dt, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	num := decimal256.FromI64(123456)
	bldr.Field(0).(*array.Decimal256Builder).Append(num)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	ns := params[0].Value.(sql.NullString)
	assert.True(t, ns.Valid)
	assert.Equal(t, "123.456", ns.String)
}

func TestConvertMixedTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Microsecond}
	decType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "ts", Type: tsType, Nullable: true},
		{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "amount", Type: decType, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	tsVal, err := arrow.TimestampFromTime(now, arrow.Microsecond)
	require.NoError(t, err)

	num, err := decimal128.FromString("99.99", 10, 2)
	require.NoError(t, err)

	bldr.Field(0).(*array.Int64Builder).Append(42)
	bldr.Field(1).(*array.TimestampBuilder).Append(tsVal)
	bldr.Field(2).(*array.BinaryBuilder).Append([]byte{0x01, 0x02})
	bldr.Field(3).(*array.Decimal128Builder).Append(num)
	bldr.Field(4).(*array.StringBuilder).Append("hello")

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	require.Len(t, params, 5)

	assert.Equal(t, sql.NullInt64{Int64: 42, Valid: true}, params[0].Value)
	assert.Equal(t, 1, params[0].Ordinal)

	nt := params[1].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.True(t, now.Equal(nt.Time))
	assert.Equal(t, 2, params[1].Ordinal)

	assert.Equal(t, "0102", params[2].Value)
	assert.Equal(t, 3, params[2].Ordinal)

	assert.Equal(t, sql.NullString{String: "99.99", Valid: true}, params[3].Value)
	assert.Equal(t, 4, params[3].Ordinal)

	assert.Equal(t, sql.NullString{String: "hello", Valid: true}, params[4].Value)
	assert.Equal(t, 5, params[4].Ordinal)
}

func TestConvertMultipleRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Millisecond}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	ts1, err := arrow.TimestampFromTime(t1, arrow.Millisecond)
	require.NoError(t, err)
	ts2, err := arrow.TimestampFromTime(t2, arrow.Millisecond)
	require.NoError(t, err)

	bldr.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{ts1, ts2}, nil)
	bldr.Field(1).(*array.BinaryBuilder).AppendValues([][]byte{{0xAA}, {0xBB}}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params0, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)
	nt0 := params0[0].Value.(sql.NullTime)
	assert.True(t, t1.Equal(nt0.Time))
	assert.Equal(t, "aa", params0[1].Value)

	params1, err := convertArrowToNamedValue(rec, 1, nil)
	require.NoError(t, err)
	nt1 := params1[0].Value.(sql.NullTime)
	assert.True(t, t2.Equal(nt1.Time))
	assert.Equal(t, "bb", params1[1].Value)
}

func TestConvertParamsReuse(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.BinaryBuilder).Append([]byte{0xFF})

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	existing := make([]driver.NamedValue, 1)
	params, err := convertArrowToNamedValue(rec, 0, existing)
	require.NoError(t, err)

	assert.Equal(t, existing[:1], params)
	assert.Equal(t, "ff", params[0].Value)
}

func TestConvertTimestampInvalidTimezone(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tsType := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "Invalid/Timezone"}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.TimestampBuilder).Append(0)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	_, err := convertArrowToNamedValue(rec, 0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid timezone")
}

func TestConvertUnsupportedTypeReturnsError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "dur", Type: arrow.FixedWidthTypes.Duration_us, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.DurationBuilder).Append(42)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	_, err := convertArrowToNamedValue(rec, 0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unsupported bind param")
}

func TestConvertStringView(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "sv", Type: arrow.BinaryTypes.StringView, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.StringViewBuilder).Append("hello view")

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	assert.Equal(t, sql.NullString{String: "hello view", Valid: true}, params[0].Value)
}

func TestConvertFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "fsb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.FixedSizeBinaryBuilder).Append([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	assert.Equal(t, "deadbeef", params[0].Value)
}

func TestConvertDate32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	expected := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	bldr.Field(0).(*array.Date32Builder).Append(arrow.Date32FromTime(expected))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.True(t, expected.Equal(nt.Time))
}

func TestConvertDate64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "d", Type: arrow.FixedWidthTypes.Date64, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	expected := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	bldr.Field(0).(*array.Date64Builder).Append(arrow.Date64FromTime(expected))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.True(t, expected.Equal(nt.Time))
}

func TestConvertTime32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "t", Type: &arrow.Time32Type{Unit: arrow.Second}, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Time32Builder).Append(arrow.Time32(43200)) // 12:00:00

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.Equal(t, 12, nt.Time.Hour())
	assert.Equal(t, 0, nt.Time.Minute())
}

func TestConvertTime64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "t", Type: &arrow.Time64Type{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	// 12:30:45.123456
	us := int64(12*3600+30*60+45)*1e6 + 123456
	bldr.Field(0).(*array.Time64Builder).Append(arrow.Time64(us))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	params, err := convertArrowToNamedValue(rec, 0, nil)
	require.NoError(t, err)

	nt := params[0].Value.(sql.NullTime)
	assert.True(t, nt.Valid)
	assert.Equal(t, 12, nt.Time.Hour())
	assert.Equal(t, 30, nt.Time.Minute())
	assert.Equal(t, 45, nt.Time.Second())
}
