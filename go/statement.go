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
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/snowflakedb/gosnowflake/v2"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	OptionStatementQueryTag                = "adbc.snowflake.statement.query_tag"
	OptionStatementQueueSize               = "adbc.rpc.result_queue_size"
	OptionStatementPrefetchConcurrency     = "adbc.snowflake.rpc.prefetch_concurrency"
	OptionStatementIngestWriterConcurrency = "adbc.snowflake.statement.ingest_writer_concurrency"
	OptionStatementIngestUploadConcurrency = "adbc.snowflake.statement.ingest_upload_concurrency"
	OptionStatementIngestCopyConcurrency   = "adbc.snowflake.statement.ingest_copy_concurrency"
	OptionStatementIngestTargetFileSize    = "adbc.snowflake.statement.ingest_target_file_size"
	OptionStatementIngestCompressionCodec  = "adbc.snowflake.statement.ingest_compression_codec"
	OptionStatementIngestCompressionLevel  = "adbc.snowflake.statement.ingest_compression_level"
	OptionStatementVectorizedScanner       = "adbc.snowflake.statement.ingest_use_vectorized_scanner"
	// OptionStatementIngestGeoType controls the Snowflake type created for
	// columns with geoarrow extension types (geoarrow.wkb, geoarrow.wkt).
	// Valid values are "geography" (default) and "geometry".
	// GEOGRAPHY is always WGS84 (SRID 4326). GEOMETRY supports any SRID.
	OptionStatementIngestGeoType = "adbc.snowflake.statement.ingest_geo_type"
)

type statement struct {
	driverbase.StatementImplBase
	cnxn                  *connectionImpl
	alloc                 memory.Allocator
	queueSize             int
	prefetchConcurrency   int
	useHighPrecision      bool
	streamRetryEnabled    bool
	maxTimestampPrecision MaxTimestampPrecision

	query          string
	targetTable    string
	targetCatalog  string
	targetDbSchema string
	ingestMode     string
	ingestOptions  ingestOptions
	queryTag       string

	bound      arrow.RecordBatch
	streamBind array.RecordReader
}

func (st *statement) Base() *driverbase.StatementImplBase {
	return &st.StatementImplBase
}

// qualifiedTableName builds a fully-qualified table identifier from the
// configured catalog, schema, and table name.
func (st *statement) qualifiedTableName() string {
	parts := make([]string, 0, 3)
	if st.targetCatalog != "" {
		parts = append(parts, quoteIdentifier(st.targetCatalog))
	}
	if st.targetDbSchema != "" {
		parts = append(parts, quoteIdentifier(st.targetDbSchema))
	}
	parts = append(parts, quoteIdentifier(st.targetTable))
	return strings.Join(parts, ".")
}

// setQueryContext applies the query tag if present.
func (st *statement) setQueryContext(ctx context.Context) context.Context {
	if st.queryTag != "" {
		ctx = gosnowflake.WithQueryTag(ctx, st.queryTag)
	}
	return ctx
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close(ctx context.Context) (err error) {
	_, span := driverbase.StartSpan(ctx, "statement.Close", st)
	defer driverbase.EndSpan(span, err)

	if st.cnxn == nil {
		err = adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState}
		return err
	}

	if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	} else if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	}
	st.cnxn = nil
	return err
}

func (st *statement) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionStatementQueryTag:
		return st.queryTag, nil
	case OptionStreamRetryEnabled:
		if st.streamRetryEnabled {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	default:
		return st.Base().GetOption(ctx, key)
	}
}

func (st *statement) GetOptionBytes(ctx context.Context, key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statement) GetOptionInt(ctx context.Context, key string) (int64, error) {
	switch key {
	case OptionStatementQueueSize:
		return int64(st.queueSize), nil
	}
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statement) GetOptionDouble(ctx context.Context, key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

// parseCompressionCodec resolves a codec name (case-insensitive) via Arrow's
// canonical names. The GetCodec call rejects codecs Arrow knows by name but has
// no registered implementation for (e.g. lzo, plain lz4), which would otherwise
// fail only later at write time.
func parseCompressionCodec(val string) (compress.Compression, error) {
	var codec compress.Compression
	if err := codec.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(val)))); err != nil {
		return 0, adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] invalid compression codec '%s': must be one of uncompressed, snappy, gzip, brotli, zstd, lz4_raw", val),
			Code: adbc.StatusInvalidArgument,
		}
	}
	if _, err := compress.GetCodec(codec); err != nil {
		return 0, adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] unsupported compression codec '%s': %s", val, err.Error()),
			Code: adbc.StatusInvalidArgument,
		}
	}
	return codec, nil
}

// SetOption sets a string option on this statement
func (st *statement) SetOption(ctx context.Context, key string, val string) error {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		st.query = ""
		st.targetTable = val
	case adbc.OptionValueIngestTargetCatalog:
		st.targetCatalog = val
	case adbc.OptionValueIngestTargetDBSchema:
		st.targetDbSchema = val
	case adbc.OptionKeyIngestMode:
		switch val {
		case adbc.OptionValueIngestModeAppend:
			fallthrough
		case adbc.OptionValueIngestModeCreate:
			fallthrough
		case adbc.OptionValueIngestModeReplace:
			fallthrough
		case adbc.OptionValueIngestModeCreateAppend:
			st.ingestMode = val
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case OptionStatementQueueSize:
		sz, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(sz))
	case OptionStatementPrefetchConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(concurrency))
	case OptionStatementIngestWriterConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(concurrency))
	case OptionStatementIngestUploadConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(concurrency))
	case OptionStatementIngestCopyConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(concurrency))
	case OptionStatementIngestTargetFileSize:
		size, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(size))
	case OptionStatementIngestCompressionCodec:
		codec, err := parseCompressionCodec(val)
		if err != nil {
			return err
		}
		st.ingestOptions.compressionCodec = codec
		return nil
	case OptionStatementIngestCompressionLevel:
		level, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(ctx, key, int64(level))
	case OptionStatementQueryTag:
		st.queryTag = val
		return nil
	case OptionUseHighPrecision:
		switch val {
		case adbc.OptionValueEnabled:
			st.useHighPrecision = true
		case adbc.OptionValueDisabled:
			st.useHighPrecision = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case OptionStreamRetryEnabled:
		switch val {
		case adbc.OptionValueEnabled:
			st.streamRetryEnabled = true
		case adbc.OptionValueDisabled:
			st.streamRetryEnabled = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case OptionStatementVectorizedScanner:
		vectorized, err := strconv.ParseBool(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as bool for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.ingestOptions.vectorizedScanner = vectorized
		return nil
	case OptionStatementIngestGeoType:
		switch strings.ToLower(val) {
		case "geography", "geometry":
			st.ingestOptions.geoType = strings.ToLower(val)
			st.ingestOptions.geoTypeExplicit = true
		case "":
			st.ingestOptions.geoType = "geography"
			st.ingestOptions.geoTypeExplicit = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid geo type '%s': must be 'geography' or 'geometry'", val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	default:
		return st.Base().SetOption(ctx, key, val)
	}
	return nil
}

func (st *statement) SetOptionBytes(ctx context.Context, key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) SetOptionInt(ctx context.Context, key string, value int64) error {
	switch key {
	case OptionStatementQueueSize:
		if value <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] Invalid value for statement option '%s': '%d' is not a positive integer", OptionStatementQueueSize, value),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.queueSize = int(value)
		return nil
	case OptionStatementPrefetchConcurrency:
		if value <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be > 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.prefetchConcurrency = int(value)
		return nil
	case OptionStatementIngestWriterConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		if value == 0 {
			st.ingestOptions.writerConcurrency = defaultWriterConcurrency
			return nil
		}

		st.ingestOptions.writerConcurrency = uint(value)
		return nil
	case OptionStatementIngestUploadConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		if value == 0 {
			st.ingestOptions.uploadConcurrency = defaultUploadConcurrency
			return nil
		}

		st.ingestOptions.uploadConcurrency = uint(value)
		return nil
	case OptionStatementIngestCopyConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.ingestOptions.copyConcurrency = uint(value)
		return nil
	case OptionStatementIngestTargetFileSize:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.ingestOptions.targetFileSize = uint(value)
		return nil
	case OptionStatementIngestCompressionLevel:
		st.ingestOptions.compressionLevel = int(value)
		return nil
	}
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) SetOptionDouble(ctx context.Context, key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (st *statement) SetSqlQuery(ctx context.Context, query string) error {
	st.query = query
	st.targetTable = ""
	return nil
}

func toSnowflakeType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.EXTENSION:
		return toSnowflakeType(dt.(arrow.ExtensionType).StorageType())
	case arrow.DICTIONARY:
		return toSnowflakeType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return toSnowflakeType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return "integer"
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return "double"
	case arrow.DECIMAL, arrow.DECIMAL256:
		dec := dt.(arrow.DecimalType)
		return fmt.Sprintf("NUMERIC(%d,%d)", dec.GetPrecision(), dec.GetScale())
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "text"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return "binary"
	case arrow.FIXED_SIZE_BINARY:
		fsb := dt.(*arrow.FixedSizeBinaryType)
		return fmt.Sprintf("binary(%d)", fsb.ByteWidth)
	case arrow.BOOL:
		return "boolean"
	case arrow.TIME32, arrow.TIME64:
		t := dt.(arrow.TemporalWithUnit)
		prec := int(t.TimeUnit()) * 3
		return fmt.Sprintf("time(%d)", prec)
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		prec := int(ts.Unit) * 3
		if ts.TimeZone == "" {
			return fmt.Sprintf("timestamp_ntz(%d)", prec)
		}
		return fmt.Sprintf("timestamp_ltz(%d)", prec)
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return "variant"
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return "array"
	case arrow.STRUCT, arrow.MAP:
		return "object"
	}

	return ""
}

// initIngest creates the target table for ingestion.
//
// typeOverrides maps field names to Snowflake types for extension columns
// that should be created with native types instead of their Arrow storage
// type: "geography"/"geometry" for geoarrow columns (BINARY/TEXT storage)
// and "uuid" for arrow.uuid columns (fixed_size_binary[16] storage).
func (st *statement) initIngest(ctx context.Context, typeOverrides map[string]string) error {
	var (
		createBldr strings.Builder
	)

	createBldr.WriteString("CREATE TABLE ")
	if st.ingestMode == adbc.OptionValueIngestModeCreateAppend {
		createBldr.WriteString(" IF NOT EXISTS ")
	}
	createBldr.WriteString(st.qualifiedTableName())
	createBldr.WriteString(" (")

	var schema *arrow.Schema
	if st.bound != nil {
		schema = st.bound.Schema()
	} else {
		schema = st.streamBind.Schema()
	}

	for i, f := range schema.Fields() {
		if i != 0 {
			createBldr.WriteString(", ")
		}

		createBldr.WriteString(quoteIdentifier(f.Name))
		createBldr.WriteString(" ")

		// Use the type override if provided. Extension column detection
		// happens in buildCopyQuery, which checks both arrow.EXTENSION types
		// and ARROW:extension:name field metadata — the latter is needed for
		// data arriving over the C Data Interface, where extension types are
		// not registered. The override map ensures the CREATE TABLE uses
		// GEOGRAPHY/GEOMETRY/UUID for those columns.
		var ty string
		if override, ok := typeOverrides[f.Name]; ok {
			ty = override
		} else {
			ty = toSnowflakeType(f.Type)
		}
		if ty == "" {
			return adbc.Error{
				Msg:  fmt.Sprintf("unimplemented type conversion for field %s, arrow type: %s", f.Name, f.Type),
				Code: adbc.StatusNotImplemented,
			}
		}

		createBldr.WriteString(ty)
		if !f.Nullable {
			createBldr.WriteString(" NOT NULL")
		}
	}

	createBldr.WriteString(")")

	switch st.ingestMode {
	case adbc.OptionValueIngestModeAppend:
		// Do nothing
	case adbc.OptionValueIngestModeReplace:
		replaceQuery := "DROP TABLE IF EXISTS " + st.qualifiedTableName()
		_, err := st.cnxn.cn.ExecContext(ctx, replaceQuery, nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}

		fallthrough
	case adbc.OptionValueIngestModeCreate:
		fallthrough
	case adbc.OptionValueIngestModeCreateAppend:
		fallthrough
	default:
		// create the table!
		createQuery := createBldr.String()
		_, err := st.cnxn.cn.ExecContext(ctx, createQuery, nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
	}

	return nil
}

func (st *statement) executeIngest(ctx context.Context) (int64, error) {
	if st.streamBind == nil && st.bound == nil {
		return -1, adbc.Error{
			Msg:  "must call Bind before bulk ingestion",
			Code: adbc.StatusInvalidState,
		}
	}

	// Capture schema before ingest (ingestRecord nils st.bound after completion)
	var schema *arrow.Schema
	if st.bound != nil {
		schema = st.bound.Schema()
	} else {
		schema = st.streamBind.Schema()
	}

	// Build the COPY query. If the schema has geo columns, this is a COPY
	// transform that converts WKB/WKT → GEOGRAPHY/GEOMETRY inline during
	// COPY INTO; otherwise it is the plain copy query.
	copyQ, typeOverrides, err := st.buildCopyQuery(schema)
	if err != nil {
		return -1, err
	}

	err = st.initIngest(ctx, typeOverrides)
	if err != nil {
		return -1, err
	}

	if st.bound != nil {
		return st.ingestRecord(ctx, copyQ)
	}
	return st.ingestStream(ctx, copyQ)
}

// buildCopyQuery returns the COPY query to use for ingestion and a map of
// column name → Snowflake type for table creation overrides. When the
// schema contains geoarrow columns, a COPY transform is returned that
// converts WKB/WKT to GEOGRAPHY/GEOMETRY inline during COPY INTO — Snowflake's
// COPY INTO from Parquet normally cannot load WKB directly into
// GEOGRAPHY/GEOMETRY columns, and a COPY transform works around this by
// applying TO_GEOGRAPHY/TO_GEOMETRY in the SELECT clause of the COPY subquery.
//
// arrow.uuid columns are created as native UUID columns. No COPY transform
// is needed for them on the plain path: Snowflake's Parquet reader converts
// the Parquet UUID logical type (which the writer emits for arrow.uuid's
// fixed_size_binary[16] storage) natively. On the geo transform path, where
// every column is read through a `$1:"col"` VARIANT reference instead, the
// 16 raw bytes are re-encoded as canonical UUID text inline.
//
// Extension column detection covers both arrow.EXTENSION types and
// ARROW:extension:name field metadata so that data arriving over the C Data
// Interface (where extension types are not registered) is also recognized.
func (st *statement) buildCopyQuery(schema *arrow.Schema) (string, map[string]string, error) {
	if schema == nil {
		return copyQuery, nil, nil
	}

	// Detect geo columns: either a registered arrow.ExtensionType or the
	// ARROW:extension:name field metadata (the C Data Interface case).
	type geoCol struct {
		name    string
		extName string
		extMeta string
	}
	var geoCols []geoCol
	uuidCols := make(map[string]bool)

	for _, f := range schema.Fields() {
		var extName, extMeta string
		if f.Type.ID() == arrow.EXTENSION {
			ext := f.Type.(arrow.ExtensionType)
			extName = ext.ExtensionName()
			extMeta = ext.Serialize()
		} else if name, ok := f.Metadata.GetValue("ARROW:extension:name"); ok {
			extName = name
			extMeta, _ = f.Metadata.GetValue("ARROW:extension:metadata")
		}

		switch extName {
		case "geoarrow.wkb", "geoarrow.wkt":
			geoCols = append(geoCols, geoCol{name: f.Name, extName: extName, extMeta: extMeta})
		case "arrow.uuid":
			uuidCols[f.Name] = true
		}
	}

	if len(geoCols) == 0 && len(uuidCols) == 0 {
		return copyQuery, nil, nil
	}

	typeOverrides := make(map[string]string, len(geoCols)+len(uuidCols))
	for name := range uuidCols {
		typeOverrides[name] = "uuid"
	}

	if len(geoCols) == 0 {
		// Only UUID columns: the plain COPY converts the Parquet UUID
		// logical type natively, so just the CREATE TABLE override applies.
		return copyQuery, typeOverrides, nil
	}

	// Build a COPY transform with inline geo conversion. Each geo column's
	// target type is resolved per-column so a non-4326 CRS can promote that
	// column to GEOMETRY while sibling 4326 columns stay GEOGRAPHY.
	var selectCols []string
	for fieldIndex, f := range schema.Fields() {
		quoted := quoteIdentifier(f.Name)
		parqRef := fmt.Sprintf("$1:%s", quoted)

		// Check if this field is a geo column.
		var gc *geoCol
		for i := range geoCols {
			if geoCols[i].name == f.Name {
				gc = &geoCols[i]
				break
			}
		}

		if gc == nil {
			if uuidCols[f.Name] {
				// UUID column: the VARIANT reference yields the 16 raw
				// storage bytes; re-encode them as canonical UUID text,
				// which Snowflake casts to the UUID column on load.
				selectCols = append(selectCols, fmt.Sprintf(
					`REGEXP_REPLACE(HEX_ENCODE(%s::BINARY, 0), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\\1-\\2-\\3-\\4-\\5') AS %s`,
					parqRef, quoted))
				continue
			}
			// Non-geo column: reference directly from Parquet, Snowflake auto-casts to target type.
			selectCols = append(selectCols, fmt.Sprintf("%s AS %s", parqRef, quoted))
			continue
		}

		// Geo column: apply conversion function.
		isWKB := strings.Contains(gc.extName, "wkb")
		geoType, err := st.ingestOptions.resolveGeoType(fieldIndex, f.Name, gc.extMeta)
		if err != nil {
			return "", nil, err
		}

		typeOverrides[gc.name] = geoType
		var expr string
		if geoType == "geography" {
			if isWKB {
				expr = fmt.Sprintf("TO_GEOGRAPHY(%s::BINARY, true) AS %s", parqRef, quoted)
			} else {
				expr = fmt.Sprintf("TRY_TO_GEOGRAPHY(%s::VARCHAR) AS %s", parqRef, quoted)
			}
		} else {
			srid, _ := extractSRIDFromMeta(gc.extMeta)
			if srid != 0 {
				if isWKB {
					expr = fmt.Sprintf("ST_SETSRID(TO_GEOMETRY(%s::BINARY), %d) AS %s", parqRef, srid, quoted)
				} else {
					expr = fmt.Sprintf("ST_SETSRID(TO_GEOMETRY(%s::VARCHAR), %d) AS %s", parqRef, srid, quoted)
				}
			} else {
				if isWKB {
					expr = fmt.Sprintf("TO_GEOMETRY(%s::BINARY) AS %s", parqRef, quoted)
				} else {
					expr = fmt.Sprintf("TO_GEOMETRY(%s::VARCHAR) AS %s", parqRef, quoted)
				}
			}
		}
		selectCols = append(selectCols, expr)
	}

	transformQ := fmt.Sprintf(
		"COPY INTO IDENTIFIER(?) FROM (SELECT %s FROM @%s)",
		strings.Join(selectCols, ", "),
		bindStageName,
	)
	return transformQ, typeOverrides, nil
}

// resolveGeoType picks the Snowflake target type for a single geoarrow column.
// When the user has set ingest_geo_type explicitly, that value is honored for
// every column (current behavior). Otherwise the column's CRS metadata decides:
// any non-EPSG:4326 SRID promotes the column to GEOMETRY so the SRID survives
// the round trip; missing CRS, EPSG:4326, or unparsable CRS stays GEOGRAPHY.
func (opts *ingestOptions) resolveGeoType(fieldIndex int, fieldName string, extMeta string) (string, error) {
	if opts.geoTypeExplicit {
		return opts.geoType, nil
	}
	srid, edges := extractSRIDFromMeta(extMeta)
	if srid == 4326 && edges == "spherical" {
		return "geography", nil
	} else if edges == "spherical" {
		// Snowflake GEOGRAPHY is always SRID 4326, so if the user
		// specified spherical edges but a different SRID, we should
		// error/ask them to explicitly set the geo type
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[snowflake] field #%d (%s) is a GeoArrow array with spherical edges but an SRID of %d; Snowflake GEOGRAPHY is always SRID 4326, so explicitly set %s to choose whether to ingest this as GEOGRAPHY or GEOMETRY", fieldIndex+1, quoteIdentifier(fieldName), srid, OptionStatementIngestGeoType),
			Code: adbc.StatusInvalidData,
		}
	}
	return "geometry", nil
}

// extractSRIDFromMeta extracts the SRID and edges from GeoArrow extension
// metadata string.  The metadata is a JSON string that may contain a "crs"
// field.
//
// Supported formats:
//   - PROJJSON: {"crs": {"id": {"authority": "EPSG", "code": 4326}}}
//   - Simple string: "EPSG:4326" (as CRS value)
//
// Returns 0 if no SRID can be determined.
func extractSRIDFromMeta(metadata string) (int, string) {
	if metadata == "" {
		return 0, ""
	}

	type projID struct {
		Authority string `json:"authority"`
		Code      int    `json:"code"`
	}
	type projCRS struct {
		ID projID `json:"id"`
	}

	type projIDString struct {
		Authority string `json:"authority"`
		Code      string `json:"code"`
	}
	type projCRSString struct {
		ID projIDString `json:"id"`
	}

	type geoarrowMeta struct {
		CRS   json.RawMessage `json:"crs"`
		Edges string          `json:"edges"`
	}

	var meta geoarrowMeta
	if err := json.Unmarshal([]byte(metadata), &meta); err != nil {
		return 0, ""
	}

	if len(meta.CRS) == 0 {
		return 0, meta.Edges
	}

	// CRS can be a string like "EPSG:4326" or a PROJJSON object
	var crsStr string
	if err := json.Unmarshal(meta.CRS, &crsStr); err == nil {
		if strings.HasPrefix(crsStr, "EPSG:") {
			if code, err := strconv.Atoi(crsStr[5:]); err == nil {
				return code, meta.Edges
			}
		} else if crsStr == "OGC:CRS84" {
			return 4326, meta.Edges
		}
		return 0, meta.Edges
	}

	var crs projCRS
	if err := json.Unmarshal(meta.CRS, &crs); err == nil {
		if strings.EqualFold(crs.ID.Authority, "EPSG") && crs.ID.Code != 0 {
			return crs.ID.Code, meta.Edges
		}
	}

	var crsString projCRSString
	if err := json.Unmarshal(meta.CRS, &crsString); err == nil {
		if strings.EqualFold(crsString.ID.Authority, "EPSG") {
			if code, err := strconv.Atoi(crsString.ID.Code); err == nil {
				return code, meta.Edges
			}
		} else if strings.EqualFold(crsString.ID.Authority, "OGC") && strings.EqualFold(crsString.ID.Code, "CRS84") {
			return 4326, meta.Edges
		}
	}

	return 0, meta.Edges
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (reader array.RecordReader, nRows int64, err error) {
	nRows = -1

	var span trace.Span
	ctx, span = driverbase.StartSpan(ctx, "statement.ExecuteQuery", st)
	defer func() {
		span.SetAttributes(semconv.DBResponseReturnedRowsKey.Int64(nRows))
		driverbase.EndSpan(span, err)
	}()

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		nRows, err = st.executeIngest(ctx)
		return
	}

	if st.query == "" {
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return
	}

	// for a bound stream reader we'd need to implement something to
	// concatenate RecordReaders which doesn't exist yet. let's put
	// that off for now.
	if st.streamBind != nil || st.bound != nil {
		bind := snowflakeBindReader{
			doQuery: func(params []driver.NamedValue) (array.RecordReader, error) {
				var loader gosnowflake.ArrowStreamLoader
				loader, err = st.cnxn.cn.QueryArrowStream(ctx, st.query, params...)
				if err != nil {
					err = errToAdbcErr(adbc.StatusInternal, err)
					return nil, err
				}

				reader, err = newRecordReader(ctx, st.alloc, loader, st.queueSize, st.prefetchConcurrency, st.useHighPrecision, st.streamRetryEnabled, st.maxTimestampPrecision, st.cnxn.useGeoArrow())
				return reader, err
			},
			currentBatch: st.bound,
			stream:       st.streamBind,
		}
		st.bound = nil
		st.streamBind = nil

		rdr := concatReader{}
		err = rdr.Init(&bind)
		if err != nil {
			return
		}
		reader = &rdr
		return
	}

	var loader gosnowflake.ArrowStreamLoader
	loader, err = st.cnxn.cn.QueryArrowStream(ctx, st.query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusInternal, err)
		return
	}

	reader, err = newRecordReader(ctx, st.alloc, loader, st.queueSize, st.prefetchConcurrency, st.useHighPrecision, st.streamRetryEnabled, st.maxTimestampPrecision, st.cnxn.useGeoArrow())
	nRows = loader.TotalRows()
	return
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (numRows int64, err error) {
	ctx, span := driverbase.StartSpan(ctx, "statement.ExecuteUpdate", st)
	defer func() {
		span.SetAttributes(semconv.DBResponseReturnedRowsKey.Int64(numRows))
		driverbase.EndSpan(span, err)
	}()

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		numRows, err = st.executeIngest(ctx)
		return numRows, err
	}

	if st.query == "" {
		numRows = -1
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return numRows, err
	}

	if st.streamBind != nil || st.bound != nil {
		numRows = 0
		bind := snowflakeBindReader{
			currentBatch: st.bound,
			stream:       st.streamBind,
		}
		st.bound = nil
		st.streamBind = nil

		defer bind.Release()
		for {
			params, err := bind.NextParams()
			if err == io.EOF {
				break
			} else if err != nil {
				numRows = -1
				return numRows, err
			}
			r, err := st.cnxn.cn.ExecContext(ctx, st.query, params)
			if err != nil {
				err = errToAdbcErr(adbc.StatusInternal, err)
				numRows = -1
				return numRows, err
			}
			n, err := r.RowsAffected()
			if err != nil {
				numRows = -1
			} else if numRows >= 0 {
				numRows += n
			}
		}
		err = nil
		return numRows, err
	}

	r, err := st.cnxn.cn.ExecContext(ctx, st.query, nil)
	if err != nil {
		numRows = -1
		err = errToAdbcErr(adbc.StatusIO, err)
		return numRows, err
	}

	numRows, err = r.RowsAffected()
	if err != nil {
		numRows = -1
		err = nil
	}

	return numRows, err
}

// ExecuteSchema gets the schema of the result set of a query without executing it.
func (st *statement) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	ctx, span := driverbase.StartSpan(ctx, "statement.ExecuteSchema", st)
	defer driverbase.EndSpan(span, err)

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		err = adbc.Error{
			Msg:  "cannot execute schema for ingestion",
			Code: adbc.StatusInvalidState,
		}
		return nil, err
	}

	if st.query == "" {
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return nil, err
	}

	if st.streamBind != nil || st.bound != nil {
		err = adbc.Error{
			Msg:  "executing schema with bound params not yet implemented",
			Code: adbc.StatusNotImplemented,
		}
		return nil, err
	}

	var loader gosnowflake.ArrowStreamLoader
	loader, err = st.cnxn.cn.QueryArrowStream(gosnowflake.WithDescribeOnly(ctx), st.query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusInternal, err)
		return nil, err
	}

	schema, err = rowTypesToArrowSchema(ctx, loader, st.useHighPrecision, st.maxTimestampPrecision)
	return schema, err
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (st *statement) Prepare(_ context.Context) error {
	if st.query == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "cannot prepare statement with no query",
		}
	}
	// snowflake doesn't provide a "Prepare" api, this is a no-op
	return nil
}

// SetSubstraitPlan allows setting a serialized Substrait execution
// plan into the query or for querying Substrait-related metadata.
//
// Drivers are not required to support both SQL and Substrait semantics.
// If they do, it may be via converting between representations internally.
//
// Like SetSqlQuery, after this is called the query can be executed
// using any of the Execute methods. If the query is expected to be
// executed repeatedly, Prepare should be called first on the statement.
func (st *statement) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return adbc.Error{
		Msg:  "Snowflake does not support Substrait plans",
		Code: adbc.StatusNotImplemented,
	}
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (st *statement) Bind(_ context.Context, values arrow.RecordBatch) error {
	if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	} else if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	}

	st.bound = values
	if st.bound != nil {
		st.bound.Retain()
	}
	return nil
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (st *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	} else if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	}

	st.streamBind = stream
	if st.streamBind != nil {
		st.streamBind.Retain()
	}
	return nil
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
//
// This retrieves an Arrow Schema describing the number, names, and
// types of the parameters in a parameterized statement. The fields
// of the schema should be in order of the ordinal position of the
// parameters; named parameters should appear only once.
//
// If the parameter does not have a name, or a name cannot be determined,
// the name of the corresponding field in the schema will be an empty
// string. If the type cannot be determined, the type of the corresponding
// field will be NA (NullType).
//
// This should be called only after calling Prepare.
//
// This should return an error with StatusNotImplemented if the schema
// cannot be determined.
func (st *statement) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	// snowflake's API does not provide any way to determine the schema
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
	}
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
//
// It returns the Schema of the result set, the collection of partition
// descriptors and the number of rows affected, if known. If unknown,
// the number of rows affected will be -1.
//
// If the driver does not support partitioned results, this will return
// an error with a StatusNotImplemented code.
func (st *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	if st.query == "" {
		return nil, adbc.Partitions{}, -1, adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
	}

	// snowflake partitioned results are not currently portable enough to
	// satisfy the requirements of this function. At least not what is
	// returned from the snowflake driver.
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Msg:  "ExecutePartitions not implemented for Snowflake",
		Code: adbc.StatusNotImplemented,
	}
}
