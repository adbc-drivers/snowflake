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
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/snowflakedb/gosnowflake/v2"
	"golang.org/x/sync/errgroup"
)

const (
	defaultStatementQueueSize  = 100
	defaultPrefetchConcurrency = 5
)

//go:embed queries/get_objects_all.sql
var queryGetObjectsAll string

//go:embed queries/get_objects_dbschemas.sql
var queryGetObjectsDbSchemas string

//go:embed queries/get_objects_tables.sql
var queryGetObjectsTables string

//go:embed queries/get_objects_terse_catalogs.sql
var queryGetObjectsTerseCatalogs string

const geographyGeoArrowJson = `{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}`

type snowflakeConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	QueryArrowStream(context.Context, string, ...driver.NamedValue) (gosnowflake.ArrowStreamLoader, error)
}

type connectionImpl struct {
	driverbase.ConnectionImplBase

	cn   snowflakeConn
	db   *databaseImpl
	ctor driver.Connector

	activeTransaction     bool
	disableWildcards      bool
	useHighPrecision      bool
	streamRetryEnabled    bool
	geographyOutputFormat string
	geometryOutputFormat  string
	maxTimestampPrecision MaxTimestampPrecision
}

// useGeoArrow reports whether the EWKB peek/GeoArrow path should be used.
// This is true when at least one of the geo output formats is EWKB.
func (c *connectionImpl) useGeoArrow() bool {
	return c.geographyOutputFormat == "EWKB" || c.geometryOutputFormat == "EWKB"
}

func escapeSingleQuoteForLike(arg string) string {
	if len(arg) == 0 {
		return arg
	}

	idx := strings.IndexByte(arg, '\'')
	if idx == -1 {
		return arg
	}

	var b strings.Builder
	b.Grow(len(arg))

	for {
		before, after, found := strings.Cut(arg, `'`)
		b.WriteString(before)
		if !found {
			return b.String()
		}

		backslashes := 0
		for i := len(before) - 1; i >= 0 && before[i] == '\\'; i-- {
			backslashes++
		}
		// Backslashes escape each other in pairs, so only an odd run escapes the quote.
		if backslashes%2 == 0 {
			b.WriteByte('\\')
		}
		b.WriteByte('\'')
		arg = after
	}
}

const (
	// Snowflake error numbers signalling a SHOW produced no usable result set.
	errShowNoMatch    = 2043 // the SHOW command matched nothing
	errObjectNotFound = 2003 // the scoped object does not exist or is not authorized
)

func isMetadataNotFound(err error) bool {
	var sfErr *gosnowflake.SnowflakeError
	return errors.As(err, &sfErr) && (sfErr.Number == errShowNoMatch || sfErr.Number == errObjectNotFound)
}

// hasEmptyLiteralFilter identifies filters that cannot match an object name.
func hasEmptyLiteralFilter(disableWildcards bool, filters ...*string) bool {
	if disableWildcards {
		for _, filter := range filters {
			if filter != nil && *filter == "" {
				return true
			}
		}
	}
	return false
}

func getQueryID(ctx context.Context, query string, driverConn driver.QueryerContext, emptyQuery string) (string, error) {
	rows, err := driverConn.QueryContext(ctx, query, nil)
	if err != nil {
		// A missing or inaccessible scoped object has no metadata to contribute.
		// Substitute a typed empty source so RESULT_SCAN remains valid.
		if emptyQuery != "" && query != emptyQuery && isMetadataNotFound(err) {
			return getQueryID(ctx, emptyQuery, driverConn, "")
		}
		return "", errToAdbcErr(adbc.StatusUnknown, err)
	}

	return rows.(gosnowflake.SnowflakeRows).GetQueryID(), rows.Close()
}

const (
	objSchemas   = "SCHEMAS"
	objDatabases = "DATABASES"
	objViews     = "VIEWS"
	objTables    = "TABLES"
	objObjects   = "OBJECTS"
)

// metadataPatternArg builds a pattern for ILIKE predicates using ESCAPE '!'.
func metadataPatternArg(name string, pattern *string, disableWildcards bool) sql.NamedArg {
	if pattern == nil {
		return sql.Named(name, "%")
	}
	if disableWildcards {
		return sql.Named(name, strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(*pattern))
	}
	return sql.Named(name, strings.ReplaceAll(*pattern, "!", "!!"))
}

func addLike(query *strings.Builder, pattern *string, disableWildcards bool) {
	if disableWildcards || pattern == nil || len(*pattern) == 0 || *pattern == "%" || *pattern == ".*" {
		return
	}
	fmt.Fprintf(query, " LIKE '%s'", escapeSingleQuoteForLike(*pattern))
}

func addStartsWith(query *strings.Builder, pattern *string, disableWildcards bool) {
	if !disableWildcards || pattern == nil || len(*pattern) == 0 {
		return
	}
	fmt.Fprintf(query, " STARTS WITH '%s'", strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(*pattern))
}

// buildShowTerseQuery keeps filters in the order required by SHOW syntax.
func buildShowTerseQuery(objType string, pattern *string, suffix string, disableWildcards bool) string {
	var query strings.Builder
	query.WriteString("SHOW TERSE /* ADBC:getObjects */ ")
	query.WriteString(objType)
	addLike(&query, pattern, disableWildcards)
	query.WriteString(suffix)
	addStartsWith(&query, pattern, disableWildcards)
	return query.String()
}

// emptyShowTerseQuery exposes only the columns consumed by RESULT_SCAN templates.
func emptyShowTerseQuery(objType string) (string, error) {
	switch objType {
	case objDatabases:
		return `SELECT NULL::VARCHAR AS "name" WHERE FALSE`, nil
	case objSchemas:
		return `SELECT NULL::VARCHAR AS "name", NULL::VARCHAR AS "database_name" WHERE FALSE`, nil
	case objViews, objTables, objObjects:
		return `SELECT NULL::VARCHAR AS "name", NULL::VARCHAR AS "database_name", NULL::VARCHAR AS "schema_name", NULL::VARCHAR AS "kind" WHERE FALSE`, nil
	default:
		return "", fmt.Errorf("unimplemented object type")
	}
}

func showTerseQuery(objType string, catalog, dbSchema, tableName *string, disableWildcards bool) (string, error) {
	var pattern *string
	var suffix string
	switch objType {
	case objDatabases:
		pattern, suffix = catalog, " IN ACCOUNT"
		if hasEmptyLiteralFilter(disableWildcards, catalog) {
			return emptyShowTerseQuery(objType)
		}
	case objSchemas:
		pattern = dbSchema
		if hasEmptyLiteralFilter(disableWildcards, catalog, dbSchema) {
			return emptyShowTerseQuery(objType)
		}
		if catalog == nil || (!disableWildcards && isWildcardStr(*catalog)) {
			suffix = " IN ACCOUNT"
		} else {
			suffix = " IN DATABASE " + quoteIdentifier(*catalog)
		}
	case objViews, objTables, objObjects:
		pattern = tableName
		if hasEmptyLiteralFilter(disableWildcards, catalog, dbSchema, tableName) {
			return emptyShowTerseQuery(objType)
		}
		if catalog == nil || (!disableWildcards && isWildcardStr(*catalog)) {
			suffix = " IN ACCOUNT"
		} else if dbSchema == nil || (!disableWildcards && isWildcardStr(*dbSchema)) {
			suffix = " IN DATABASE " + quoteIdentifier(*catalog)
		} else {
			suffix = " IN SCHEMA " + quoteIdentifier(*catalog) + "." + quoteIdentifier(*dbSchema)
		}
	default:
		return "", fmt.Errorf("unimplemented object type")
	}
	return buildShowTerseQuery(objType, pattern, suffix, disableWildcards), nil
}

func goGetQueryID(ctx context.Context, conn driver.QueryerContext, grp *errgroup.Group, objType string, catalog, dbSchema, tableName *string, disableWildcards bool, outQueryID *string) {
	grp.Go(func() error {
		emptyQuery, err := emptyShowTerseQuery(objType)
		if err != nil {
			return err
		}
		query, err := showTerseQuery(objType, catalog, dbSchema, tableName, disableWildcards)
		if err != nil {
			return err
		}
		*outQueryID, err = getQueryID(ctx, query, conn, emptyQuery)
		return err
	})
}

func isWildcardStr(ident string) bool {
	return strings.ContainsAny(ident, "_%")
}

// scopeIdentifier reports whether ident can be used as a concrete object name to
// narrow the scope of the SHOW COLUMNS enrichment query (IN DATABASE/SCHEMA/
// TABLE). It deliberately treats '_' as a literal character rather than an ADBC
// single-character wildcard: only '%', the match-all sentinels, and nil force a
// broader scope. See showColumnsScope for why treating '_' literally here is
// safe despite the ADBC pattern semantics.
func scopeIdentifier(ident *string) (string, bool) {
	if ident == nil {
		return "", false
	}
	s := *ident
	if s == "" || s == "%" || s == ".*" || strings.Contains(s, "%") {
		return "", false
	}
	return s, true
}

// showColumnsScope returns the narrowest "IN ..." suffix for the SHOW COLUMNS
// query. SHOW COLUMNS cannot be filtered by table, so an IN ACCOUNT scope scans
// every column in the account (80+ seconds on large accounts).
//
// The result feeds get_objects_all.sql ONLY as a best-effort source for BINARY
// xdbc_column_size enrichment; the authoritative column list comes from
// information_schema.columns filtered by ILIKE, so a narrower scope here can
// never drop columns or alter non-BINARY metadata. The sole consequence of
// treating '_' as a literal (see scopeIdentifier) is that a BINARY column
// reached only through '_'-as-wildcard matches, or through a case-insensitive
// pattern that differs from the stored identifier, may report a NULL
// xdbc_column_size, matching the behavior before this enrichment was added.
func showColumnsScope(catalog, dbSchema, tableName *string, disableWildcards bool) string {
	if disableWildcards {
		if catalog == nil || *catalog == "" {
			return " IN ACCOUNT"
		}
		if dbSchema == nil || *dbSchema == "" {
			return " IN DATABASE " + quoteIdentifier(*catalog)
		}
		if tableName == nil || *tableName == "" {
			return " IN SCHEMA " + quoteIdentifier(*catalog) + "." + quoteIdentifier(*dbSchema)
		}
		return " IN TABLE " + quoteIdentifier(*catalog) + "." + quoteIdentifier(*dbSchema) + "." + quoteIdentifier(*tableName)
	}

	cat, ok := scopeIdentifier(catalog)
	if !ok {
		return " IN ACCOUNT"
	}
	sch, ok := scopeIdentifier(dbSchema)
	if !ok {
		return " IN DATABASE " + quoteIdentifier(cat)
	}
	tbl, ok := scopeIdentifier(tableName)
	if !ok {
		return " IN SCHEMA " + quoteIdentifier(cat) + "." + quoteIdentifier(sch)
	}
	return " IN TABLE " + quoteIdentifier(cat) + "." + quoteIdentifier(sch) + "." + quoteIdentifier(tbl)
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (reader array.RecordReader, err error) {
	ctx, span := driverbase.StartSpan(ctx, "connectionImpl.GetObjects", c)
	defer driverbase.EndSpan(span, err)

	var (
		pkQueryID, fkQueryID, uniqueQueryID, terseDbQueryID string
		showSchemaQueryID, tableQueryID, columnsQueryID     string
	)

	conn := c.cn
	var hasViews, hasTables bool
	for _, t := range tableType {
		if strings.EqualFold("VIEW", t) {
			hasViews = true
		} else if strings.EqualFold("TABLE", t) {
			hasTables = true
		}
	}

	// force empty result from SHOW TABLES if tableType list is not empty
	// and does not contain TABLE or VIEW in the list.
	// we need this because we should have non-null db_schema_tables when
	// depth is Tables, Columns or All.
	var badTableType = "tabletypedoesnotexist"
	if len(tableType) > 0 && depth >= adbc.ObjectDepthTables && !hasViews && !hasTables {
		tableName = &badTableType
		tableType = []string{"TABLE"}
	}

	// Optimized path: read SHOW TERSE results directly instead of through
	// RESULT_SCAN SQL templates, reducing Snowflake round-trips from 3-4 to 1-2.
	if reader, err = c.getObjectsDirectPath(ctx, depth, catalog, dbSchema, tableName, tableType, hasViews, hasTables); reader != nil || err != nil {
		return
	}

	gQueryIDs, gQueryIDsCtx := errgroup.WithContext(ctx)
	query := queryGetObjectsAll
	switch depth {
	case adbc.ObjectDepthCatalogs:
		query = queryGetObjectsTerseCatalogs
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, c.disableWildcards, &terseDbQueryID)
	case adbc.ObjectDepthDBSchemas:
		query = queryGetObjectsDbSchemas
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, c.disableWildcards, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, c.disableWildcards, &terseDbQueryID)
	case adbc.ObjectDepthTables:
		query = queryGetObjectsTables
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, c.disableWildcards, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, c.disableWildcards, &terseDbQueryID)

		objType := objObjects
		if len(tableType) == 1 {
			if strings.EqualFold("VIEW", tableType[0]) {
				objType = objViews
			} else if strings.EqualFold("TABLE", tableType[0]) {
				objType = objTables
			}
		}

		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
			catalog, dbSchema, tableName, c.disableWildcards, &tableQueryID)
	default:
		var suffix string
		if catalog == nil || (!c.disableWildcards && isWildcardStr(*catalog)) {
			suffix = " IN ACCOUNT"
		} else {
			escapedCatalog := quoteIdentifier(*catalog)
			if dbSchema == nil || (!c.disableWildcards && isWildcardStr(*dbSchema)) {
				suffix = " IN DATABASE " + escapedCatalog
			} else {
				escapedSchema := quoteIdentifier(*dbSchema)
				if tableName == nil || (!c.disableWildcards && isWildcardStr(*tableName)) {
					suffix = " IN SCHEMA " + escapedCatalog + "." + escapedSchema
				} else {
					escapedTable := quoteIdentifier(*tableName)
					suffix = " IN TABLE " + escapedCatalog + "." + escapedSchema + "." + escapedTable
				}
			}
		}

		emptyPkUkQuery := `SELECT NULL::VARCHAR AS "database_name", NULL::VARCHAR AS "schema_name", NULL::VARCHAR AS "table_name", NULL::VARCHAR AS "constraint_name", NULL::VARCHAR AS "column_name", NULL::NUMBER AS "key_sequence" WHERE FALSE`
		emptyFkQuery := `SELECT NULL::VARCHAR AS "fk_database_name", NULL::VARCHAR AS "fk_schema_name", NULL::VARCHAR AS "fk_table_name", NULL::VARCHAR AS "fk_name", NULL::VARCHAR AS "fk_column_name", NULL::VARCHAR AS "pk_database_name", NULL::VARCHAR AS "pk_schema_name", NULL::VARCHAR AS "pk_table_name", NULL::VARCHAR AS "pk_column_name", NULL::NUMBER AS "key_sequence" WHERE FALSE`
		emptyColumnsQuery := `SELECT NULL::VARCHAR AS "database_name", NULL::VARCHAR AS "schema_name", NULL::VARCHAR AS "table_name", NULL::VARCHAR AS "column_name", NULL::VARCHAR AS "data_type" WHERE FALSE`

		// Detailed constraint info not available in information_schema
		// Need to dispatch SHOW queries and use conn.Raw to extract the queryID for reuse in GetObjects query
		gQueryIDs.Go(func() (err error) {
			query := "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */" + suffix
			if hasEmptyLiteralFilter(c.disableWildcards, catalog, dbSchema, tableName) {
				query = emptyPkUkQuery
			}
			pkQueryID, err = getQueryID(gQueryIDsCtx, query, conn, emptyPkUkQuery)
			return err
		})

		gQueryIDs.Go(func() (err error) {
			query := "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */" + suffix
			if hasEmptyLiteralFilter(c.disableWildcards, catalog, dbSchema, tableName) {
				query = emptyFkQuery
			}
			fkQueryID, err = getQueryID(gQueryIDsCtx, query, conn, emptyFkQuery)
			return err
		})

		gQueryIDs.Go(func() (err error) {
			query := "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */" + suffix
			if hasEmptyLiteralFilter(c.disableWildcards, catalog, dbSchema, tableName) {
				query = emptyPkUkQuery
			}
			uniqueQueryID, err = getQueryID(gQueryIDsCtx, query, conn, emptyPkUkQuery)
			return err
		})

		columnsSuffix := showColumnsScope(catalog, dbSchema, tableName, c.disableWildcards)
		gQueryIDs.Go(func() (err error) {
			query := "SHOW COLUMNS /* ADBC:getObjects */" + columnsSuffix
			if hasEmptyLiteralFilter(c.disableWildcards, catalog, dbSchema, tableName, columnName) {
				query = emptyColumnsQuery
			}
			columnsQueryID, err = getQueryID(gQueryIDsCtx, query, conn, emptyColumnsQuery)
			return err
		})

		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, c.disableWildcards, &terseDbQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, c.disableWildcards, &showSchemaQueryID)

		objType := objObjects
		if len(tableType) == 1 {
			if strings.EqualFold("VIEW", tableType[0]) {
				objType = objViews
			} else if strings.EqualFold("TABLE", tableType[0]) {
				objType = objTables
			}
		}
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
			catalog, dbSchema, tableName, c.disableWildcards, &tableQueryID)
	}

	// Need constraint subqueries to complete before we can query GetObjects
	if err = gQueryIDs.Wait(); err != nil {
		return nil, err
	}

	args := []sql.NamedArg{
		// Optional filter patterns
		metadataPatternArg("CATALOG", catalog, c.disableWildcards),
		metadataPatternArg("DB_SCHEMA", dbSchema, c.disableWildcards),
		metadataPatternArg("TABLE", tableName, c.disableWildcards),
		metadataPatternArg("COLUMN", columnName, c.disableWildcards),

		// QueryIDs for constraint data if depth is tables or deeper
		// or if the depth is catalog and catalog is null
		sql.Named("PK_QUERY_ID", pkQueryID),
		sql.Named("FK_QUERY_ID", fkQueryID),
		sql.Named("UNIQUE_QUERY_ID", uniqueQueryID),
		sql.Named("SHOW_DB_QUERY_ID", terseDbQueryID),
		sql.Named("SHOW_SCHEMA_QUERY_ID", showSchemaQueryID),
		sql.Named("SHOW_TABLE_QUERY_ID", tableQueryID),
		sql.Named("SHOW_COLUMNS_QUERY_ID", columnsQueryID),
	}

	nvargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nvargs[i] = driver.NamedValue{
			Name:    arg.Name,
			Ordinal: i + 1,
			Value:   arg.Value,
		}
	}

	if c.disableWildcards {
		query = strings.ReplaceAll(query, " ILIKE ", " LIKE ")
	}

	var rows driver.Rows
	rows, err = conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		err = errToAdbcErr(adbc.StatusIO, err)
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	catalogCh := make(chan driverbase.GetObjectsInfo, runtime.NumCPU())
	errCh := make(chan error)

	go func() {
		defer close(catalogCh)
		dest := make([]driver.Value, len(rows.Columns()))
		for {
			if err = rows.Next(dest); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			var getObjectsCatalog driverbase.GetObjectsInfo
			if err = getObjectsCatalog.Scan(dest[0]); err != nil {
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			// A few columns need additional processing outside of Snowflake
			for i, sch := range getObjectsCatalog.CatalogDbSchemas {
				for j, tab := range sch.DbSchemaTables {
					for k, col := range tab.TableColumns {
						field := c.toArrowField(col)
						xdbcDataType := driverbase.ToXdbcDataType(field.Type)

						if field.Type != nil {
							getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcDataType = new(int16(field.Type.ID()))
						}
						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcSqlDataType = new(int16(xdbcDataType))
					}
				}
			}

			catalogCh <- getObjectsCatalog
		}
	}()

	reader, err = driverbase.BuildGetObjectsRecordReader(c.Alloc, catalogCh, errCh)
	return reader, err
}

// PrepareDriverInfo implements driverbase.DriverInfoPreparer.
func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false); err != nil {
		return err
	}

	version, err := c.getStringQuery("SELECT CURRENT_VERSION()")
	if err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, version)
}

// ListTableTypes implements driverbase.TableTypeLister.
func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	return c.getStringQuery("SELECT CURRENT_DATABASE()")
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.getStringQuery("SELECT CURRENT_SCHEMA()")
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, value string) error {
	_, err := c.cn.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s;", quoteIdentifier(value)), nil)
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, value string) error {
	_, err := c.cn.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s;", quoteIdentifier(value)), nil)
	return err
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if enabled {
		if c.activeTransaction {
			_, err := c.cn.ExecContext(ctx, "COMMIT", nil)
			if err != nil {
				return errToAdbcErr(adbc.StatusInternal, err)
			}
			c.activeTransaction = false
		}
		_, err := c.cn.ExecContext(ctx, "ALTER SESSION SET AUTOCOMMIT = true", nil)
		return err
	}

	if !c.activeTransaction {
		_, err := c.cn.ExecContext(ctx, "BEGIN", nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
		c.activeTransaction = true
	}
	_, err := c.cn.ExecContext(ctx, "ALTER SESSION SET AUTOCOMMIT = false", nil)
	return err
}

var loc = time.Now().Location()

func (c *connectionImpl) toArrowField(columnInfo driverbase.ColumnInfo) arrow.Field {
	field := arrow.Field{Name: columnInfo.ColumnName, Nullable: driverbase.ValueOrZero(columnInfo.XdbcNullable) != 0}

	switch driverbase.ValueOrZero(columnInfo.XdbcTypeName) {
	case "ARRAY":
		field.Type = arrow.BinaryTypes.String
		field.Metadata = arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name": "arrow.json",
		})
	case "NUMBER":
		if c.useHighPrecision {
			field.Type = &arrow.Decimal128Type{
				Precision: int32(driverbase.ValueOrZero(columnInfo.XdbcColumnSize)),
				Scale:     int32(driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits)),
			}
		} else {
			if driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits) == 0 {
				field.Type = arrow.PrimitiveTypes.Int64
			} else {
				field.Type = arrow.PrimitiveTypes.Float64
			}
		}
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		field.Type = arrow.PrimitiveTypes.Float64
	case "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "UUID":
		field.Type = extensions.NewUUIDType()
	case "BINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "BOOLEAN":
		field.Type = arrow.FixedWidthTypes.Boolean
	case "VARIANT":
		fallthrough
	case "OBJECT":
		// snowflake will return each value as a string
		field.Type = arrow.BinaryTypes.String
	case "DATE":
		field.Type = arrow.FixedWidthTypes.Date32
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		if c.maxTimestampPrecision == Microseconds {
			field.Type = &arrow.TimestampType{Unit: arrow.Microsecond}
		} else {
			field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
		}
	case "TIMESTAMP_LTZ":
		if c.maxTimestampPrecision == Microseconds {
			field.Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}
		} else {
			field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
		}
	case "TIMESTAMP_TZ":
		if c.maxTimestampPrecision == Microseconds {
			field.Type = arrow.FixedWidthTypes.Timestamp_us
		} else {
			field.Type = arrow.FixedWidthTypes.Timestamp_ns
		}
	case "GEOGRAPHY":
		if c.geographyOutputFormat == "GEOJSON" {
			field.Type = arrow.BinaryTypes.String
		} else {
			// With GEOGRAPHY_OUTPUT_FORMAT=EWKB, data arrives as binary WKB.
			// GEOGRAPHY is always WGS84 (SRID 4326).
			field.Type = arrow.BinaryTypes.Binary
			field.Metadata = arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name":     "geoarrow.wkb",
				"ARROW:extension:metadata": geographyGeoArrowJson,
			})
		}
	case "GEOMETRY":
		if c.geometryOutputFormat == "GEOJSON" {
			field.Type = arrow.BinaryTypes.String
		} else {
			// With GEOMETRY_OUTPUT_FORMAT=EWKB, data arrives as binary WKB.
			field.Type = arrow.BinaryTypes.Binary
			field.Metadata = arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name": "geoarrow.wkb",
			})
		}
	case "VECTOR":
		// despite the fact that Snowflake *does* support returning data
		// for VECTOR typed columns as Arrow FixedSizeLists, there's no way
		// currently to retrieve enough metadata to construct the proper type
		// for it
	}

	return field
}

func descToField(name, typ, isnull, primary string, comment sql.NullString, useHighPrecision bool, maxTimestampPrecision MaxTimestampPrecision, geographyOutputFormat, geometryOutputFormat string) (field arrow.Field, err error) {
	field.Name = name
	if isnull == "Y" {
		field.Nullable = true
	}
	keys := []string{"DATA_TYPE", "PRIMARY_KEY"}
	vals := []string{typ, primary}
	if comment.Valid {
		keys = append(keys, "COMMENT")
		vals = append(vals, comment.String)
	}
	field.Metadata = arrow.NewMetadata(keys, vals)

	paren := strings.Index(typ, "(")
	if paren == -1 {
		// types without params
		switch typ {
		case "FLOAT":
			fallthrough
		case "DOUBLE":
			field.Type = arrow.PrimitiveTypes.Float64
		case "DATE":
			field.Type = arrow.FixedWidthTypes.Date32
		// array, object and variant are all represented as strings by
		// snowflake's return
		case "ARRAY":
			field.Type = arrow.BinaryTypes.String
			field.Metadata = arrow.MetadataFrom(map[string]string{
				"ARROW:extension:name": "arrow.json",
			})
		case "OBJECT":
			fallthrough
		case "VARIANT":
			field.Type = arrow.BinaryTypes.String
		case "UUID":
			field.Type = extensions.NewUUIDType()
		case "GEOGRAPHY":
			if geographyOutputFormat == "GEOJSON" {
				field.Type = arrow.BinaryTypes.String
			} else {
				field.Type = arrow.BinaryTypes.Binary
				field.Metadata = arrow.MetadataFrom(map[string]string{
					"ARROW:extension:name":     "geoarrow.wkb",
					"ARROW:extension:metadata": geographyGeoArrowJson,
				})
			}
		case "GEOMETRY":
			if geometryOutputFormat == "GEOJSON" {
				field.Type = arrow.BinaryTypes.String
			} else {
				field.Type = arrow.BinaryTypes.Binary
				field.Metadata = arrow.MetadataFrom(map[string]string{
					"ARROW:extension:name": "geoarrow.wkb",
				})
			}
		case "BOOLEAN":
			field.Type = arrow.FixedWidthTypes.Boolean
		default:
			err = adbc.Error{
				Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
				Code: adbc.StatusNotImplemented,
			}
		}
		return
	}

	prefix := typ[:paren]
	switch prefix {
	case "VARCHAR", "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "BINARY", "VARBINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "NUMBER":
		comma := strings.Index(typ, ",")
		scale, err := strconv.ParseInt(typ[comma+1:len(typ)-1], 10, 32)
		if err != nil {
			return field, adbc.Error{
				Msg:  "[snowflake] could not parse scale from type '" + typ + "'",
				Code: adbc.StatusInvalidData,
			}
		}
		if useHighPrecision {
			paren := strings.Index(typ, "(")
			precision, err := strconv.ParseInt(typ[paren+1:comma], 10, 32)
			if err != nil {
				return field, adbc.Error{
					Msg:  "[snowflake] could not parse precision from type '" + typ + "'",
					Code: adbc.StatusInvalidData,
				}
			}
			field.Type = &arrow.Decimal128Type{
				Precision: int32(precision),
				Scale:     int32(scale),
			}
		} else {
			if scale == 0 {
				field.Type = arrow.PrimitiveTypes.Int64
			} else {
				field.Type = arrow.PrimitiveTypes.Float64
			}
		}
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		tz := ""
		switch prefix {
		case "TIMESTAMP_LTZ":
			tz = loc.String()
		case "TIMESTAMP_TZ":
			tz = "UTC"
		}
		scale, err := strconv.ParseInt(typ[paren+1:len(typ)-1], 10, 32)
		if err != nil {
			return field, adbc.Error{
				Msg:  "[snowflake] could not parse scale from type '" + typ + "'",
				Code: adbc.StatusInvalidData,
			}
		}
		unit := getArrowTimeUnit(scale, maxTimestampPrecision)
		field.Type = &arrow.TimestampType{Unit: unit, TimeZone: tz}
	default:
		err = adbc.Error{
			Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
			Code: adbc.StatusNotImplemented,
		}
	}
	return
}

func (c *connectionImpl) getStringQuery(query string) (value string, err error) {
	result, err := c.cn.QueryContext(context.Background(), query, nil)
	if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}
	defer func() {
		err = errors.Join(err, result.Close())
	}()

	if len(result.Columns()) != 1 {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong number of columns: %s", result.Columns()),
			Code: adbc.StatusInternal,
		}
	}

	var dest [1]driver.Value
	err = result.Next(dest[:])
	if err == io.EOF {
		return "", adbc.Error{
			Msg:  "[Snowflake] Internal query returned no rows",
			Code: adbc.StatusInternal,
		}
	} else if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}

	value, ok := dest[0].(string)
	if !ok {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong type of value: %s", dest[0]),
			Code: adbc.StatusInternal,
		}
	}

	return value, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (sc *arrow.Schema, err error) {
	ctx, span := driverbase.StartSpan(ctx, "connectionImpl.GetTableSchema", c)
	defer driverbase.EndSpan(span, err)

	tblParts := make([]string, 0, 3)
	if catalog != nil {
		tblParts = append(tblParts, quoteIdentifier(*catalog))
	}
	if dbSchema != nil {
		tblParts = append(tblParts, quoteIdentifier(*dbSchema))
	}
	tblParts = append(tblParts, quoteIdentifier(tableName))
	fullyQualifiedTable := strings.Join(tblParts, ".")

	var rows driver.Rows
	rows, err = c.cn.QueryContext(ctx, `DESC TABLE `+fullyQualifiedTable, nil)
	if err != nil {
		err = errToAdbcErr(adbc.StatusIO, err)
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var (
		name, typ, isnull, primary string
		comment                    sql.NullString
		fields                     []arrow.Field
	)

	// columns are:
	// name, type, kind, isnull, primary, unique, def, check, expr, comment, policyName, privDomain
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err = rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil // don't return the io.EOF
				break
			}
			err = errToAdbcErr(adbc.StatusIO, err)
			return nil, err
		}

		name = dest[0].(string)
		typ = dest[1].(string)
		isnull = dest[3].(string)
		primary = dest[5].(string)
		if err = comment.Scan(dest[9]); err != nil {
			err = errToAdbcErr(adbc.StatusIO, err)
			return nil, err
		}

		var f arrow.Field
		f, err = descToField(name, typ, isnull, primary, comment, c.useHighPrecision, c.maxTimestampPrecision, c.geographyOutputFormat, c.geometryOutputFormat)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	sc = arrow.NewSchema(fields, nil)
	return sc, err
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "ROLLBACK", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	stmtBase := driverbase.NewStatementImplBase(c.Base(), c.ErrorHelper)
	stmt := &statement{
		StatementImplBase:     stmtBase,
		alloc:                 c.db.Alloc,
		cnxn:                  c,
		queueSize:             defaultStatementQueueSize,
		prefetchConcurrency:   defaultPrefetchConcurrency,
		useHighPrecision:      c.useHighPrecision,
		streamRetryEnabled:    c.streamRetryEnabled,
		maxTimestampPrecision: c.maxTimestampPrecision,
		ingestOptions:         DefaultIngestOptions(),
	}
	return driverbase.NewStatement(stmt), nil
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close(ctx context.Context) (err error) {
	_, span := driverbase.StartSpan(ctx, "connectionImpl.Close", c)
	defer driverbase.EndSpan(span, err)

	if c.cn == nil {
		err = adbc.Error{Code: adbc.StatusInvalidState}
		return err
	}

	defer func() {
		c.cn = nil
	}()
	return c.cn.Close()
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ReadPartition not yet implemented for snowflake driver",
	}
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionDisableWildcards:
		if c.disableWildcards {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionUseHighPrecision:
		if c.useHighPrecision {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionStreamRetryEnabled:
		if c.streamRetryEnabled {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionGeographyOutputFormat:
		return c.geographyOutputFormat, nil
	case OptionGeometryOutputFormat:
		return c.geometryOutputFormat, nil
	default:
		return c.Base().GetOption(ctx, key)
	}
}

func (c *connectionImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionDisableWildcards:
		switch value {
		case adbc.OptionValueEnabled:
			c.disableWildcards = true
		case adbc.OptionValueDisabled:
			c.disableWildcards = false
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	case OptionUseHighPrecision:
		// statements will inherit the value of the OptionUseHighPrecision
		// from the connection, but the option can be overridden at the
		// statement level if SetOption is called on the statement.
		switch value {
		case adbc.OptionValueEnabled:
			c.useHighPrecision = true
		case adbc.OptionValueDisabled:
			c.useHighPrecision = false
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	case OptionStreamRetryEnabled:
		switch value {
		case adbc.OptionValueEnabled:
			c.streamRetryEnabled = true
		case adbc.OptionValueDisabled:
			c.streamRetryEnabled = false
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	case OptionGeographyOutputFormat:
		if err := validateGeoOutputFormat(key, value); err != nil {
			return err
		}
		_, err := c.cn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = '%s'", value), nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
		c.geographyOutputFormat = value
		return nil
	case OptionGeometryOutputFormat:
		if err := validateGeoOutputFormat(key, value); err != nil {
			return err
		}
		_, err := c.cn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET GEOMETRY_OUTPUT_FORMAT = '%s'", value), nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
		c.geometryOutputFormat = value
		return nil
	default:
		return c.Base().SetOption(ctx, key, value)
	}
}
