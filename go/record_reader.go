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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const MetadataKeySnowflakeType = "SNOWFLAKE_TYPE"

// EWKB extension flags. The high four bits of the geometry-type word in the
// PostGIS-style EWKB encoding carry SRID/Z/M information; the low 28 bits are
// the OGC geometry type (with optional ISO Z/M offsets).
const (
	ewkbSRIDFlag uint32 = 0x20000000
	ewkbMFlag    uint32 = 0x40000000
	ewkbZFlag    uint32 = 0x80000000
)

// geoColumnInfo captures what the EWKB peek learned about a binary column.
// A zero srid means "looks like WKB/EWKB but no usable column-level CRS"
// (either every value was plain WKB without an SRID prefix, or rows disagreed
// on SRID); the column is still tagged as geoarrow.wkb.
type geoColumnInfo struct {
	srid int
}

func identCol(_ context.Context, a arrow.Array) (arrow.Array, error) {
	a.Retain()
	return a, nil
}

// peekEWKBGeo inspects the first bytes of a value and returns whether they
// look like a valid OGC/EWKB geometry header. When the EWKB SRID flag is set,
// the embedded SRID is returned with hasSRID=true.
func peekEWKBGeo(b []byte) (srid uint32, hasSRID bool, ok bool) {
	if len(b) < 5 {
		return 0, false, false
	}
	var bo binary.ByteOrder
	switch b[0] {
	case 0x00:
		bo = binary.BigEndian
	case 0x01:
		bo = binary.LittleEndian
	default:
		return 0, false, false
	}
	typeWord := bo.Uint32(b[1:5])
	base := typeWord & 0x0FFFFFFF
	if !validGeoTypeCode(base) {
		return 0, false, false
	}
	if typeWord&ewkbSRIDFlag == 0 {
		return 0, false, true
	}
	if len(b) < 9 {
		return 0, false, false
	}
	return bo.Uint32(b[5:9]), true, true
}

// validGeoTypeCode reports whether t is one of the 28 OGC/ISO geometry type
// codes (Point..GeometryCollection, optionally with Z/M/ZM extensions).
func validGeoTypeCode(t uint32) bool {
	switch {
	case t >= 1 && t <= 7,
		t >= 1001 && t <= 1007,
		t >= 2001 && t <= 2007,
		t >= 3001 && t <= 3007:
		return true
	}
	return false
}

// stripEWKBSRID converts an EWKB geometry that carries an SRID prefix into
// plain ISO/OGC WKB by clearing the SRID flag and removing the four SRID
// bytes. Bytes that do not have the SRID flag set are returned unchanged.
func stripEWKBSRID(b []byte) []byte {
	if len(b) < 9 {
		return b
	}
	var bo binary.ByteOrder
	switch b[0] {
	case 0x00:
		bo = binary.BigEndian
	case 0x01:
		bo = binary.LittleEndian
	default:
		return b
	}
	typeWord := bo.Uint32(b[1:5])
	if typeWord&ewkbSRIDFlag == 0 {
		return b
	}
	out := make([]byte, len(b)-4)
	out[0] = b[0]
	bo.PutUint32(out[1:5], typeWord&^ewkbSRIDFlag)
	copy(out[5:], b[9:])
	return out
}

// stripEWKBSRIDColumn returns a column transformer that rewrites every
// non-null value through stripEWKBSRID, producing standard WKB on the wire.
func stripEWKBSRIDColumn(_ context.Context, a arrow.Array) (arrow.Array, error) {
	ba, ok := a.(*array.Binary)
	if !ok {
		a.Retain()
		return a, nil
	}
	bldr := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer bldr.Release()
	bldr.Reserve(ba.Len())
	for i := range ba.Len() {
		if ba.IsNull(i) {
			bldr.AppendNull()
			continue
		}
		bldr.Append(stripEWKBSRID(ba.Value(i)))
	}
	return bldr.NewArray(), nil
}

// analyzeGeoFromBatch walks the binary columns of a record batch and returns
// a map of column-name → geoColumnInfo for any column whose first non-null
// value parses as an OGC/EWKB geometry header. The first SRID seen is taken
// as the column-level SRID; columns whose subsequent rows disagree are still
// tagged as geo but without column-level CRS metadata.
func analyzeGeoFromBatch(rec arrow.RecordBatch) map[string]geoColumnInfo {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	out := make(map[string]geoColumnInfo)
	sc := rec.Schema()
	for i, col := range rec.Columns() {
		ba, ok := col.(*array.Binary)
		if !ok {
			continue
		}
		var (
			firstSRID  uint32
			haveSRID   bool
			mixed      bool
			isGeo      bool
			classified bool
		)
		for j := range ba.Len() {
			if ba.IsNull(j) {
				continue
			}
			srid, has, ok := peekEWKBGeo(ba.Value(j))
			if !classified {
				classified = true
				if !ok {
					break
				}
				isGeo = true
				if has {
					firstSRID = srid
					haveSRID = true
				}
				continue
			}
			if !ok {
				mixed = true
				continue
			}
			if has {
				if !haveSRID {
					firstSRID = srid
					haveSRID = true
				} else if srid != firstSRID {
					mixed = true
				}
			}
		}
		if !isGeo {
			continue
		}
		info := geoColumnInfo{}
		if haveSRID && !mixed {
			info.srid = int(firstSRID)
		}
		out[sc.Field(i).Name] = info
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type recordTransformer = func(context.Context, arrow.RecordBatch) (arrow.RecordBatch, error)
type colTransformer = func(context.Context, arrow.Array) (arrow.Array, error)

func getRecTransformer(sc *arrow.Schema, tr []colTransformer) recordTransformer {
	return func(ctx context.Context, r arrow.RecordBatch) (arrow.RecordBatch, error) {
		if len(tr) != int(r.NumCols()) {
			return nil, adbc.Error{
				Msg:  "mismatch in record cols and transformers",
				Code: adbc.StatusInvalidState,
			}
		}

		var (
			err  error
			cols = make([]arrow.Array, r.NumCols())
		)
		defer func() {
			for _, col := range cols {
				if col != nil {
					col.Release()
				}
			}
		}()

		for i, col := range r.Columns() {
			if cols[i], err = tr[i](ctx, col); err != nil {
				return nil, errToAdbcErr(adbc.StatusInternal, err)
			}
		}

		return array.NewRecordBatch(sc, cols, r.NumRows()), nil
	}
}

func getTransformer(sc *arrow.Schema, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision, geoCols map[string]geoColumnInfo) (*arrow.Schema, recordTransformer) {
	loc, types := ld.Location(), ld.RowTypes()

	fields := make([]arrow.Field, len(sc.Fields()))
	transformers := make([]func(context.Context, arrow.Array) (arrow.Array, error), len(sc.Fields()))
	for i, f := range sc.Fields() {
		srcMeta := types[i]
		originalArrowUnit := arrow.TimeUnit(srcMeta.Scale / 3)

		// Snowflake reports GEOGRAPHY/GEOMETRY columns as srcMeta.Type "binary"
		// when the GEO*_OUTPUT_FORMAT session option is WKB or EWKB. The geoCols
		// map is built by analyzeGeoFromBatch on the first record batch — any
		// binary column whose values look like an OGC/EWKB geometry header lands
		// here, with the column-level SRID lifted out of the EWKB prefix. Each
		// row goes through stripEWKBSRID so the value on the Arrow wire is
		// standard ISO/OGC WKB.
		if geoCol, ok := geoCols[f.Name]; ok {
			f.Type = arrow.BinaryTypes.Binary
			meta := map[string]string{
				"ARROW:extension:name": "geoarrow.wkb",
			}
			if geoCol.srid != 0 {
				meta["ARROW:extension:metadata"] = fmt.Sprintf(`{"crs":"EPSG:%d"}`, geoCol.srid)
			}
			f.Metadata = arrow.MetadataFrom(meta)
			transformers[i] = stripEWKBSRIDColumn
			fields[i] = f
			continue
		}

		switch strings.ToUpper(srcMeta.Type) {
		case "FIXED":
			switch f.Type.ID() {
			case arrow.DECIMAL, arrow.DECIMAL256:
				if useHighPrecision {
					transformers[i] = identCol
				} else {
					if srcMeta.Scale == 0 {
						f.Type = arrow.PrimitiveTypes.Int64
					} else {
						f.Type = arrow.PrimitiveTypes.Float64
					}
					dt := f.Type
					transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
						return compute.CastArray(ctx, a, compute.UnsafeCastOptions(dt))
					}
				}
			default:
				if useHighPrecision {
					dt := &arrow.Decimal128Type{
						Precision: int32(srcMeta.Precision),
						Scale:     int32(srcMeta.Scale),
					}
					f.Type = dt
					transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
						return integerToDecimal128(ctx, a, dt)
					}
				} else {
					if srcMeta.Scale != 0 {
						f.Type = arrow.PrimitiveTypes.Float64
						// For precisions of 16, 17 and 18, a conversion from int64 to float64 fails with an error
						// So for these precisions, we instead convert first to a decimal128 and then to a float64.
						if srcMeta.Precision > 15 && srcMeta.Precision < 19 {
							transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
								result, err := integerToDecimal128(ctx, a, &arrow.Decimal128Type{
									Precision: int32(srcMeta.Precision),
									Scale:     int32(srcMeta.Scale),
								})
								if err != nil {
									return nil, err
								}
								defer result.Release()
								return compute.CastArray(ctx, result, compute.UnsafeCastOptions(f.Type))
							}
						} else {
							// For precisions less than 16, we can simply scale the integer value appropriately
							transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
								result, err := compute.Divide(ctx, compute.ArithmeticOptions{NoCheckOverflow: true},
									&compute.ArrayDatum{Value: a.Data()},
									compute.NewDatum(math.Pow10(int(srcMeta.Scale))))
								if err != nil {
									return nil, err
								}
								defer result.Release()
								return result.(*compute.ArrayDatum).MakeArray(), nil
							}
						}
					} else {
						f.Type = arrow.PrimitiveTypes.Int64
						transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
							return compute.CastArray(ctx, a, compute.SafeCastOptions(arrow.PrimitiveTypes.Int64))
						}
					}
				}
			}
		case "TIME":
			var dt arrow.DataType
			if srcMeta.Scale < 6 {
				dt = &arrow.Time32Type{Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
			} else {
				dt = &arrow.Time64Type{Unit: arrow.TimeUnit(srcMeta.Scale / 3)}
			}
			f.Type = dt
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				return compute.CastArray(ctx, a, compute.SafeCastOptions(dt))
			}
		case "TIMESTAMP_NTZ":
			dt := &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
			f.Type = dt
			fractionMultiplier := int64(math.Pow10(9 - int(srcMeta.Scale)))
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {

				if a.DataType().ID() != arrow.STRUCT {
					return compute.CastArray(ctx, a, compute.SafeCastOptions(dt))
				}

				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				structData := a.(*array.Struct)
				epoch := structData.Field(0).(*array.Int64).Int64Values()
				fraction := structData.Field(1).(*array.Int32).Int32Values()
				for i := range a.Len() {
					if a.IsNull(i) {
						tb.AppendNull()
						continue
					}

					nanoseconds := int64(fraction[i]) * fractionMultiplier
					v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds), dt.TimeUnit(), originalArrowUnit, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					tb.Append(v)
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_LTZ":
			dt := &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision), TimeZone: loc.String()}
			f.Type = dt
			fractionMultiplier := int64(math.Pow10(9 - int(srcMeta.Scale)))
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				if a.DataType().ID() == arrow.STRUCT {
					structData := a.(*array.Struct)
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					fraction := structData.Field(1).(*array.Int32).Int32Values()
					for i := range a.Len() {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						nanoseconds := int64(fraction[i]) * fractionMultiplier
						v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds), dt.TimeUnit(), originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				} else {
					for i, t := range a.(*array.Int64).Int64Values() {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						tb.Append(arrow.Timestamp(t))
					}
				}
				return tb.NewArray(), nil
			}
		case "TIMESTAMP_TZ":
			// we convert each value to UTC since we have timezone information
			// with the data that lets us do so.
			dt := &arrow.TimestampType{TimeZone: "UTC", Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
			f.Type = dt
			fractionMultiplier := int64(math.Pow10(9 - int(srcMeta.Scale)))
			transformers[i] = func(ctx context.Context, a arrow.Array) (arrow.Array, error) {
				pool := compute.GetAllocator(ctx)
				tb := array.NewTimestampBuilder(pool, dt)
				defer tb.Release()

				structData := a.(*array.Struct)
				if structData.NumField() == 2 {
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					tzoffset := structData.Field(1).(*array.Int32).Int32Values()
					for i := range a.Len() {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						loc := gosnowflake.Location(int(tzoffset[i]) - 1440)
						// When there's no fraction field, epoch contains the timestamp in the scale's unit
						// For scale 0-2: seconds, scale 3-5: milliseconds, scale 6-8: microseconds, scale 9: nanoseconds
						var t time.Time
						switch srcMeta.Scale {
						case 0, 1, 2:
							t = time.Unix(epoch[i], 0).In(loc)
						case 3, 4, 5:
							t = time.UnixMilli(epoch[i]).In(loc)
						case 6, 7, 8:
							t = time.UnixMicro(epoch[i]).In(loc)
						case 9:
							t = time.Unix(0, epoch[i]).In(loc)
						default:
							t = time.Unix(epoch[i], 0).In(loc)
						}
						v, err := getArrowTimestampFromTime(t, dt.Unit, originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				} else {
					epoch := structData.Field(0).(*array.Int64).Int64Values()
					fraction := structData.Field(1).(*array.Int32).Int32Values()
					tzoffset := structData.Field(2).(*array.Int32).Int32Values()
					for i := range a.Len() {
						if a.IsNull(i) {
							tb.AppendNull()
							continue
						}

						nanoseconds := int64(fraction[i]) * fractionMultiplier
						loc := gosnowflake.Location(int(tzoffset[i]) - 1440)
						v, err := getArrowTimestampFromTime(time.Unix(epoch[i], nanoseconds).In(loc), dt.Unit, originalArrowUnit, maxTimestampPrecision)
						if err != nil {
							return nil, err
						}
						tb.Append(v)
					}
				}
				return tb.NewArray(), nil
			}
		default:
			transformers[i] = identCol
		}

		fields[i] = f
	}

	meta := sc.Metadata()
	out := arrow.NewSchema(fields, &meta)
	return out, getRecTransformer(out, transformers)
}

func getArrowTimeUnit(scale int64, maxTimestampPrecision MaxTimestampPrecision) arrow.TimeUnit {
	if scale == 9 && maxTimestampPrecision == Microseconds {
		return arrow.Microsecond
	} else {
		return arrow.TimeUnit(scale / 3)
	}
}

func getArrowTimestampFromTime(val time.Time, unit arrow.TimeUnit, originalArrowUnit arrow.TimeUnit, maxTimestampPrecision MaxTimestampPrecision) (arrow.Timestamp, error) {
	if maxTimestampPrecision == NanosecondsNoOverflow && originalArrowUnit == arrow.Nanosecond {
		sec := float64(val.Unix())
		maxSeconds := math.MaxInt64 / 1e9
		minSeconds := math.MinInt64 / 1e9
		if sec > maxSeconds || sec < minSeconds {
			return 0, errToAdbcErr(adbc.StatusInvalidData, fmt.Errorf("timestamp %v overflows when converted to nanoseconds", val))
		}
	}

	if maxTimestampPrecision == Microseconds && originalArrowUnit == arrow.Nanosecond {
		return arrow.TimestampFromTime(time.UnixMicro(val.UnixMicro()), arrow.Microsecond)
	}

	return arrow.TimestampFromTime(val, unit)
}

func integerToDecimal128(ctx context.Context, a arrow.Array, dt *arrow.Decimal128Type) (arrow.Array, error) {
	// We can't do a cast directly into the destination type because the numbers we get from Snowflake
	// are scaled integers. So not only would the cast produce the wrong value, it also risks producing
	// an error of precisions which e.g. can't hold every int64. To work around these problems, we instead
	// cast into a decimal type of a precision and scale which we know will hold all values and won't
	// require scaling, We then substitute the type on this array with the actual return type.

	dt0 := &arrow.Decimal128Type{
		Precision: int32(20),
		Scale:     int32(0),
	}
	result, err := compute.CastArray(ctx, a, compute.SafeCastOptions(dt0))
	if err != nil {
		return nil, err
	}

	data := result.Data()
	result.Data().Reset(dt, data.Len(), data.Buffers(), data.Children(), data.NullN(), data.Offset())
	return result, err
}

func rowTypesToArrowSchema(_ context.Context, ld gosnowflake.ArrowStreamLoader, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision) (*arrow.Schema, error) {
	var loc *time.Location

	metadata := ld.RowTypes()
	fields := make([]arrow.Field, len(metadata))
	for i, srcMeta := range metadata {
		fields[i] = arrow.Field{
			Name:     srcMeta.Name,
			Nullable: srcMeta.Nullable,
			Metadata: arrow.NewMetadata(
				[]string{MetadataKeySnowflakeType},
				[]string{srcMeta.Type},
			),
		}
		switch srcMeta.Type {
		case "fixed":
			if useHighPrecision {
				fields[i].Type = &arrow.Decimal128Type{
					Precision: int32(srcMeta.Precision),
					Scale:     int32(srcMeta.Scale),
				}
			} else {
				// Check scale to determine if this is an integer or decimal
				if srcMeta.Scale == 0 {
					fields[i].Type = arrow.PrimitiveTypes.Int64
				} else {
					fields[i].Type = arrow.PrimitiveTypes.Float64
				}
			}
		case "real":
			fields[i].Type = arrow.PrimitiveTypes.Float64
		case "date":
			fields[i].Type = arrow.PrimitiveTypes.Date32
		case "time":
			fields[i].Type = arrow.FixedWidthTypes.Time64ns
		case "timestamp_ntz":
			fields[i].Type = &arrow.TimestampType{Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
		case "timestamp_tz":
			fields[i].Type = &arrow.TimestampType{TimeZone: "UTC", Unit: getArrowTimeUnit(srcMeta.Scale, maxTimestampPrecision)}
		case "timestamp_ltz":
			if loc == nil {
				loc = ld.Location()
			}
			if maxTimestampPrecision == Microseconds {
				fields[i].Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}
			} else {
				fields[i].Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
			}
		case "binary":
			fields[i].Type = arrow.BinaryTypes.Binary
		case "boolean":
			fields[i].Type = arrow.FixedWidthTypes.Boolean
		default:
			fields[i].Type = arrow.BinaryTypes.String
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

func extractTimestamp(src *string) (sec, nsec int64, err error) {
	s, ms, hasFraction := strings.Cut(*src, ".")
	sec, err = strconv.ParseInt(s, 10, 64)
	if err != nil {
		return
	}

	if !hasFraction {
		return
	}

	nsec, err = strconv.ParseInt(ms+strings.Repeat("0", 9-len(ms)), 10, 64)
	return
}

func jsonDataToArrow(_ context.Context, bldr *array.RecordBuilder, rawData [][]*string, maxTimestampPrecision MaxTimestampPrecision) (arrow.RecordBatch, error) {
	fieldBuilders := bldr.Fields()
	for _, rec := range rawData {
		for i, col := range rec {
			field := fieldBuilders[i]

			if col == nil {
				field.AppendNull()
				continue
			}

			switch fb := field.(type) {
			case *array.Time64Builder:
				sec, nsec, err := extractTimestamp(col)
				if err != nil {
					return nil, err
				}

				fb.Append(arrow.Time64(sec*1e9 + nsec))
			case *array.TimestampBuilder:
				snowflakeType, ok := bldr.Schema().Field(i).Metadata.GetValue(MetadataKeySnowflakeType)
				if !ok {
					return nil, errToAdbcErr(
						adbc.StatusInvalidData,
						fmt.Errorf("key %s not found in metadata for field %s", MetadataKeySnowflakeType, bldr.Schema().Field(i).Name),
					)
				}

				if snowflakeType == "timestamp_tz" {
					// "timestamp_tz" should be value + offset separated by space
					tm := strings.Split(*col, " ")
					if len(tm) != 2 {
						return nil, adbc.Error{
							Msg:        "invalid TIMESTAMP_TZ data. value doesn't consist of two numeric values separated by a space: " + *col,
							SqlState:   [5]byte{'2', '2', '0', '0', '7'},
							VendorCode: 268000,
							Code:       adbc.StatusInvalidData,
						}
					}

					sec, nsec, err := extractTimestamp(&tm[0])
					if err != nil {
						return nil, err
					}
					offset, err := strconv.ParseInt(tm[1], 10, 64)
					if err != nil {
						return nil, adbc.Error{
							Msg:        "invalid TIMESTAMP_TZ data. offset value is not an integer: " + tm[1],
							SqlState:   [5]byte{'2', '2', '0', '0', '7'},
							VendorCode: 268000,
							Code:       adbc.StatusInvalidData,
						}
					}

					loc := gosnowflake.Location(int(offset) - 1440)
					tt := time.Unix(sec, nsec).In(loc)

					var unit arrow.TimeUnit
					originalArrowUnit := arrow.Nanosecond
					if maxTimestampPrecision == Microseconds {
						unit = arrow.Microsecond
					} else {
						unit = arrow.Nanosecond
					}

					ts, err := getArrowTimestampFromTime(tt, unit, originalArrowUnit, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					fb.Append(ts)
					break
				}

				// otherwise timestamp_ntz or timestamp_ltz, which have the same physical representation
				sec, nsec, err := extractTimestamp(col)
				if err != nil {
					return nil, err
				}

				if maxTimestampPrecision == Microseconds {
					tt := time.Unix(sec, nsec)
					ts, err := getArrowTimestampFromTime(tt, arrow.Microsecond, arrow.Nanosecond, maxTimestampPrecision)
					if err != nil {
						return nil, err
					}
					fb.Append(ts)
				} else {
					fb.Append(arrow.Timestamp(sec*1e9 + nsec))
				}
			case *array.BinaryBuilder:
				b, err := hex.DecodeString(*col)
				if err != nil {
					return nil, adbc.Error{
						Msg:        err.Error(),
						VendorCode: 268002,
						SqlState:   [5]byte{'2', '2', '0', '0', '3'},
						Code:       adbc.StatusInvalidData,
					}
				}
				fb.Append(b)
			default:
				if err := fb.AppendValueFromString(*col); err != nil {
					return nil, err
				}
			}
		}
	}
	return bldr.NewRecordBatch(), nil
}

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	err        error

	cancelFn context.CancelFunc
	done     chan struct{} // signals all producer goroutines have finished
}

func newRecordReader(ctx context.Context, alloc memory.Allocator, ld gosnowflake.ArrowStreamLoader, bufferSize, prefetchConcurrency int, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision) (array.RecordReader, error) {
	batches, err := ld.GetBatches()
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err)
	}

	// if the first chunk was JSON, that means this was a metadata query which
	// is only returning JSON data rather than Arrow
	rawData := ld.JSONData()
	if len(rawData) > 0 {
		// construct an Arrow schema based on reading the JSON metadata description of the
		// result type schema
		schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision, maxTimestampPrecision)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInternal,
			}
		}

		if ld.TotalRows() == 0 {
			return array.NewRecordReader(schema, []arrow.RecordBatch{})
		}

		bldr := array.NewRecordBuilder(alloc, schema)
		defer bldr.Release()

		rec, err := jsonDataToArrow(ctx, bldr, rawData, maxTimestampPrecision)
		if err != nil {
			return nil, err
		}
		defer rec.Release()

		results := []arrow.RecordBatch{rec}
		for _, b := range batches {
			rdr, err := b.GetStream(ctx)
			if err != nil {
				return nil, adbc.Error{
					Msg:  err.Error(),
					Code: adbc.StatusInternal,
				}
			}

			// the "JSON" data returned isn't valid JSON. Instead it is a list of
			// comma-delimited JSON lists containing every value as a string, except
			// for a JSON null to represent nulls. Thus we can't just use the existing
			// JSON parsing code in Arrow.
			data, err := io.ReadAll(rdr)
			rdrErr := rdr.Close()
			if err != nil {
				return nil, adbc.Error{
					Msg:  err.Error(),
					Code: adbc.StatusInternal,
				}
			} else if rdrErr != nil {
				return nil, rdrErr
			}

			if cap(rawData) >= int(b.NumRows()) {
				rawData = rawData[:b.NumRows()]
			} else {
				rawData = make([][]*string, b.NumRows())
			}
			bldr.Reserve(int(b.NumRows()))

			// we grab the entire JSON message and create a bytes reader
			offset, buf := int64(0), bytes.NewReader(data)
			for i := range b.NumRows() {
				// we construct a decoder from the bytes.Reader to read the next JSON list
				// of columns (one row) from the input
				dec := json.NewDecoder(buf)
				if err = dec.Decode(&rawData[i]); err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}

				// dec.InputOffset() now represents the index of the ',' so we skip the comma
				offset += dec.InputOffset() + 1
				// then seek the buffer to that spot. we have to seek based on the start
				// because json.Decoder can read from the buffer more than is necessary to
				// process the JSON data.
				if _, err = buf.Seek(offset, 0); err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}
			}

			// now that we have our [][]*string of JSON data, we can pass it to get converted
			// to an Arrow record batch and appended to our slice of batches
			rec, err := jsonDataToArrow(ctx, bldr, rawData, maxTimestampPrecision)
			if err != nil {
				return nil, err
			}
			defer rec.Release()

			results = append(results, rec)
		}

		return array.NewRecordReader(schema, results)
	}

	// Handle empty batches case early
	if len(batches) == 0 {
		schema, err := rowTypesToArrowSchema(ctx, ld, useHighPrecision, maxTimestampPrecision)
		if err != nil {
			return nil, err
		}
		_, cancelFn := context.WithCancel(ctx)
		rdr := &reader{
			refCount: 1,
			chs:      nil,
			err:      nil,
			cancelFn: cancelFn,
			done:     make(chan struct{}),
		}
		close(rdr.done) // No goroutines to wait for
		rdr.schema, _ = getTransformer(schema, ld, useHighPrecision, maxTimestampPrecision, nil)
		return rdr, nil
	}

	// Do all error-prone initialization first, before starting goroutines
	r, err := batches[0].GetStream(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	rr, err := ipc.NewReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		_ = r.Close() // Clean up the stream
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidState,
		}
	}

	// Peek the first record from the first IPC batch so we can identify any
	// binary columns that carry EWKB geometries (and lift their SRID into
	// geoarrow.wkb field metadata) before fixing the result schema. The
	// geometry shape is determined per-query from the data itself, so this
	// works for any SQL — table scans, joins, CTEs, ST_Transform, etc. —
	// without needing to parse the user's query text.
	var firstRec arrow.RecordBatch
	if rr.Next() {
		firstRec = rr.RecordBatch()
	}
	geoCols := analyzeGeoFromBatch(firstRec)

	// Now setup concurrency primitives after error-prone operations
	group, ctx := errgroup.WithContext(compute.WithAllocator(ctx, alloc))
	ctx, cancelFn := context.WithCancel(ctx)
	group.SetLimit(prefetchConcurrency)

	// Initialize all channels upfront to avoid race condition
	chs := make([]chan arrow.RecordBatch, len(batches))
	for i := range chs {
		chs[i] = make(chan arrow.RecordBatch, bufferSize)
	}

	rdr := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		done:     make(chan struct{}),
	}

	var recTransform recordTransformer
	rdr.schema, recTransform = getTransformer(rr.Schema(), ld, useHighPrecision, maxTimestampPrecision, geoCols)

	if firstRec != nil {
		transformed, err := recTransform(ctx, firstRec)
		if err != nil {
			rr.Release()
			_ = r.Close()
			cancelFn()
			return nil, errToAdbcErr(adbc.StatusInternal, err)
		}
		// chs[0] is buffered (bufferSize >= 1), so this never blocks.
		chs[0] <- transformed
	}

	group.Go(func() (err error) {
		defer rr.Release()
		defer func() {
			err = errors.Join(err, r.Close())
		}()
		if len(batches) > 1 {
			defer close(chs[0])
		}

		for rr.Next() && ctx.Err() == nil {
			rec := rr.RecordBatch()
			rec, err = recTransform(ctx, rec)
			if err != nil {
				return err
			}

			// Use context-aware send to prevent deadlock
			select {
			case chs[0] <- rec:
				// Successfully sent
			case <-ctx.Done():
				// Context cancelled, clean up and exit
				rec.Release()
				return ctx.Err()
			}
		}
		return rr.Err()
	})

	lastChannelIndex := len(chs) - 1
	go func() {
		for i, b := range batches[1:] {
			batch, batchIdx := b, i+1
			// Channels already initialized above, no need to create them here
			group.Go(func() (err error) {
				// close channels (except the last) so that Next can move on to the next channel properly
				if batchIdx != lastChannelIndex {
					defer close(chs[batchIdx])
				}

				rdr, err := batch.GetStream(ctx)
				if err != nil {
					return err
				}
				defer func() {
					err = errors.Join(err, rdr.Close())
				}()

				rr, err := ipc.NewReader(rdr, ipc.WithAllocator(alloc))
				if err != nil {
					return err
				}
				defer rr.Release()

				for rr.Next() && ctx.Err() == nil {
					rec := rr.RecordBatch()
					rec, err = recTransform(ctx, rec)
					if err != nil {
						return err
					}

					// Use context-aware send to prevent deadlock
					select {
					case chs[batchIdx] <- rec:
						// Successfully sent
					case <-ctx.Done():
						// Context cancelled, clean up and exit
						rec.Release()
						return ctx.Err()
					}
				}

				return rr.Err()
			})
		}

		// place this here so that we always clean up, but they can't be in a
		// separate goroutine. Otherwise we'll have a race condition between
		// the call to wait and the calls to group.Go to kick off the jobs
		// to perform the pre-fetching (GH-1283).
		rdr.err = group.Wait()
		// don't close the last channel until after the group is finished,
		// so that Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
		// Signal that all producer goroutines have finished
		close(rdr.done)
	}()

	return rdr, nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}

	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()

		// Wait for all producer goroutines to finish before draining channels
		// This prevents deadlock where producers are blocked on sends
		<-r.done

		// Now safely drain remaining data from channels
		// All channels should be closed at this point
		for _, ch := range r.chs {
			if ch == nil {
				continue
			}
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

var _ array.RecordReader = (*reader)(nil)
