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
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// decimalToString converts an arrow Decimal128 or Decimal256 value to its string
// representation using the type's scale for proper formatting.
func decimalToString(field arrow.Field, col arrow.Array, index int) string {
	switch c := col.(type) {
	case *array.Decimal128:
		dt := field.Type.(*arrow.Decimal128Type)
		return c.Value(index).ToString(dt.Scale)
	case *array.Decimal256:
		dt := field.Type.(*arrow.Decimal256Type)
		return c.Value(index).ToString(dt.Scale)
	default:
		panic(fmt.Sprintf("decimalToString called with non-decimal type: %T", col))
	}
}

func convertArrowToNamedValue(batch arrow.RecordBatch, index int, params []driver.NamedValue) ([]driver.NamedValue, error) {
	// see goTypeToSnowflake in gosnowflake
	// technically, snowflake can bind an array of values at once, but
	// only for INSERT, so we can't take advantage of that without
	// analyzing the query ourselves
	if cap(params) >= int(batch.NumCols()) {
		params = params[:batch.NumCols()]
	} else {
		params = make([]driver.NamedValue, batch.NumCols())
	}
	for i, field := range batch.Schema().Fields() {
		rawColumn := batch.Column(i)
		params[i].Ordinal = i + 1
		switch column := rawColumn.(type) {
		case *array.Boolean:
			params[i].Value = sql.NullBool{
				Bool:  column.Value(index),
				Valid: column.IsValid(index),
			}
		case *array.Float32:
			// Snowflake only recognizes float64
			params[i].Value = sql.NullFloat64{
				Float64: float64(column.Value(index)),
				Valid:   column.IsValid(index),
			}
		case *array.Float64:
			params[i].Value = sql.NullFloat64{
				Float64: column.Value(index),
				Valid:   column.IsValid(index),
			}
		case *array.Int8:
			// Snowflake only recognizes int64
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int16:
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int32:
			params[i].Value = sql.NullInt64{
				Int64: int64(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Int64:
			params[i].Value = sql.NullInt64{
				Int64: column.Value(index),
				Valid: column.IsValid(index),
			}
		case *array.String:
			params[i].Value = sql.NullString{
				String: column.Value(index),
				Valid:  column.IsValid(index),
			}
		case *array.LargeString:
			params[i].Value = sql.NullString{
				String: column.Value(index),
				Valid:  column.IsValid(index),
			}
		case *array.StringView:
			params[i].Value = sql.NullString{
				String: column.Value(index),
				Valid:  column.IsValid(index),
			}
		case *array.Timestamp:
			tsType := field.Type.(*arrow.TimestampType)
			toTime, err := tsType.GetToTimeFunc()
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[Snowflake] Invalid timezone for bind param '%s': %s", field.Name, err),
				}
			}
			params[i].Value = sql.NullTime{
				Time:  toTime(column.Value(index)),
				Valid: column.IsValid(index),
			}
		case *array.Date32:
			params[i].Value = sql.NullTime{
				Time:  column.Value(index).ToTime(),
				Valid: column.IsValid(index),
			}
		case *array.Date64:
			params[i].Value = sql.NullTime{
				Time:  column.Value(index).ToTime(),
				Valid: column.IsValid(index),
			}
		case *array.Time32:
			unit := field.Type.(*arrow.Time32Type).Unit
			params[i].Value = sql.NullTime{
				Time:  column.Value(index).ToTime(unit),
				Valid: column.IsValid(index),
			}
		case *array.Time64:
			unit := field.Type.(*arrow.Time64Type).Unit
			params[i].Value = sql.NullTime{
				Time:  column.Value(index).ToTime(unit),
				Valid: column.IsValid(index),
			}
		case *array.Binary:
			// gosnowflake's goTypeToSnowflake misclassifies []byte as arrayType
			// (JSON array of numbers) unless tsmode is explicitly set to binaryType.
			// Since we can't control tsmode through the driver.NamedValue interface,
			// hex-encode the bytes as a string instead. Snowflake implicitly converts
			// TEXT to BINARY for BINARY columns, and this matches the wire format
			// gosnowflake uses internally (converter.go valueToString with binaryType).
			if column.IsValid(index) {
				params[i].Value = hex.EncodeToString(column.Value(index))
			} else {
				params[i].Value = nil
			}
		case *array.LargeBinary:
			// Same as Binary — see comment above.
			if column.IsValid(index) {
				params[i].Value = hex.EncodeToString(column.Value(index))
			} else {
				params[i].Value = nil
			}
		case *array.BinaryView:
			// Same as Binary — see comment above.
			if column.IsValid(index) {
				params[i].Value = hex.EncodeToString(column.Value(index))
			} else {
				params[i].Value = nil
			}
		case *array.FixedSizeBinary:
			// Same as Binary — see comment above.
			if column.IsValid(index) {
				params[i].Value = hex.EncodeToString(column.Value(index))
			} else {
				params[i].Value = nil
			}
		case *array.Decimal128:
			params[i].Value = sql.NullString{
				String: decimalToString(field, rawColumn, index),
				Valid:  column.IsValid(index),
			}
		case *array.Decimal256:
			params[i].Value = sql.NullString{
				String: decimalToString(field, rawColumn, index),
				Valid:  column.IsValid(index),
			}
		default:
			return nil, adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[Snowflake] Unsupported bind param '%s' type %s", field.Name, field.Type.String()),
			}
		}
	}
	return params, nil
}

type snowflakeBindReader struct {
	doQuery      func([]driver.NamedValue) (array.RecordReader, error)
	currentBatch arrow.RecordBatch
	nextIndex    int64
	params       []driver.NamedValue
	// may be nil if we bound only a batch
	stream array.RecordReader
}

func (r *snowflakeBindReader) Release() {
	if r.currentBatch != nil {
		r.currentBatch.Release()
		r.currentBatch = nil
	}
	if r.stream != nil {
		r.stream.Release()
		r.stream = nil
	}
}

func (r *snowflakeBindReader) Next() (array.RecordReader, error) {
	params, err := r.NextParams()
	if err != nil {
		// includes EOF
		return nil, err
	}
	return r.doQuery(params)
}

func (r *snowflakeBindReader) NextParams() ([]driver.NamedValue, error) {
	for r.currentBatch == nil || r.nextIndex >= r.currentBatch.NumRows() {
		// We can be used both by binding a stream or by binding a
		// batch. In the latter case, we have to release the batch,
		// but not in the former case. Unify the cases by always
		// releasing the batch, adding an "extra" retain so that the
		// release does not cause issues.
		if r.currentBatch != nil {
			r.currentBatch.Release()
		}
		r.currentBatch = nil
		if r.stream != nil && r.stream.Next() {
			r.currentBatch = r.stream.RecordBatch()
			r.currentBatch.Retain()
			r.nextIndex = 0
			continue
		} else if r.stream != nil && r.stream.Err() != nil {
			return nil, r.stream.Err()
		} else {
			// no more params
			return nil, io.EOF
		}
	}

	params, err := convertArrowToNamedValue(r.currentBatch, int(r.nextIndex), r.params)
	r.params = params
	r.nextIndex++
	return params, err
}
