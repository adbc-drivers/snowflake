// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/snowflakedb/gosnowflake/v2"
	"golang.org/x/sync/errgroup"
)

const (
	keyColumns        = `"database_name", "schema_name", "table_name", "constraint_name", "column_name", "key_sequence"`
	fkColumns         = `"fk_database_name", "fk_schema_name", "fk_table_name", "fk_name", "fk_column_name", "pk_database_name", "pk_schema_name", "pk_table_name", "pk_column_name", "key_sequence"`
	columnColumns     = `"database_name", "schema_name", "table_name", "column_name", "data_type"`
	emptyPkUkQuery    = `SELECT NULL::VARCHAR AS "database_name", NULL::VARCHAR AS "schema_name", NULL::VARCHAR AS "table_name", NULL::VARCHAR AS "constraint_name", NULL::VARCHAR AS "column_name", NULL::NUMBER AS "key_sequence" WHERE FALSE`
	emptyFkQuery      = `SELECT NULL::VARCHAR AS "fk_database_name", NULL::VARCHAR AS "fk_schema_name", NULL::VARCHAR AS "fk_table_name", NULL::VARCHAR AS "fk_name", NULL::VARCHAR AS "fk_column_name", NULL::VARCHAR AS "pk_database_name", NULL::VARCHAR AS "pk_schema_name", NULL::VARCHAR AS "pk_table_name", NULL::VARCHAR AS "pk_column_name", NULL::NUMBER AS "key_sequence" WHERE FALSE`
	emptyColumnsQuery = `SELECT NULL::VARCHAR AS "database_name", NULL::VARCHAR AS "schema_name", NULL::VARCHAR AS "table_name", NULL::VARCHAR AS "column_name", NULL::VARCHAR AS "data_type" WHERE FALSE`
)

// SHOW LIKE supplies candidates, not the final literal filter. In particular,
// underscores and percent signs still have wildcard meaning to SHOW.
func matchesLiteralName(name string, filter *string) bool {
	return filter == nil || (*filter != "" && strings.EqualFold(name, *filter))
}

type literalObjects struct {
	catalogs []string
	schemas  []schemaEntry
	tables   []tableEntry
}

// execLiteralShow retains stored spellings for subsequent quoted scopes. Using
// STARTS WITH here would exclude case-insensitive matches before filtering.
func (c *connectionImpl) execLiteralShow(ctx context.Context, objType string, filter *string, suffix string) (entries []tableEntry, err error) {
	if filter != nil && *filter == "" {
		return nil, nil
	}
	var query strings.Builder
	fmt.Fprintf(&query, "SHOW TERSE /* ADBC:getObjects */ %s", objType)
	addLike(&query, filter, true)
	query.WriteString(suffix)
	rows, err := c.cn.QueryContext(ctx, query.String(), nil)
	if err != nil {
		var sfErr *gosnowflake.SnowflakeError
		if errors.As(err, &sfErr) && (sfErr.Number == errShowNoMatch || sfErr.Number == errObjectNotFound) {
			return nil, nil
		}
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	entries, err = readTableEntries(rows)
	if err != nil {
		return nil, err
	}
	matched := entries[:0]
	for _, entry := range entries {
		if matchesLiteralName(entry.tableName, filter) {
			matched = append(matched, entry)
		}
	}
	return matched, nil
}

func (c *connectionImpl) resolveLiteralObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName *string, tableType []string) (objects literalObjects, err error) {
	dbs, err := c.execLiteralShow(ctx, objDatabases, catalog, " IN ACCOUNT")
	if err != nil {
		return objects, err
	}
	for _, db := range dbs {
		objects.catalogs = append(objects.catalogs, db.tableName)
	}
	if depth == adbc.ObjectDepthCatalogs || len(dbs) == 0 {
		return objects, nil
	}

	schemaScopes := []string{" IN ACCOUNT"}
	if catalog != nil {
		schemaScopes = nil
		for _, name := range objects.catalogs {
			schemaScopes = append(schemaScopes, " IN DATABASE "+quoteIdentifier(name))
		}
	}
	for _, scope := range schemaScopes {
		entries, err := c.execLiteralShow(ctx, objSchemas, dbSchema, scope)
		if err != nil {
			return objects, err
		}
		for _, entry := range entries {
			if matchesLiteralName(entry.dbName, catalog) {
				objects.schemas = append(objects.schemas, schemaEntry{dbName: entry.dbName, schemaName: entry.tableName})
			}
		}
	}
	if depth == adbc.ObjectDepthDBSchemas || len(objects.schemas) == 0 {
		return objects, nil
	}

	tableScopes := objects.scopes(catalog, dbSchema, nil)
	for _, scope := range tableScopes {
		entries, err := c.execLiteralShow(ctx, showObjType(tableType), tableName, scope)
		if err != nil {
			return objects, err
		}
		for _, entry := range entries {
			if matchesLiteralName(entry.dbName, catalog) && matchesLiteralName(entry.schemaName, dbSchema) {
				objects.tables = append(objects.tables, entry)
			}
		}
	}
	return objects, nil
}

// scopes uses complete stored identifiers rather than the caller's spelling.
// Case-distinct matches need separate scopes, never an arbitrary first match.
func (objects literalObjects) scopes(catalog, dbSchema, tableName *string) []string {
	var scopes []string
	switch {
	case tableName != nil:
		for _, table := range objects.tables {
			scopes = append(scopes, " IN TABLE "+quoteIdentifier(table.dbName)+"."+quoteIdentifier(table.schemaName)+"."+quoteIdentifier(table.tableName))
		}
	case dbSchema != nil:
		for _, schema := range objects.schemas {
			scopes = append(scopes, " IN SCHEMA "+quoteIdentifier(schema.dbName)+"."+quoteIdentifier(schema.schemaName))
		}
	case catalog != nil:
		for _, name := range objects.catalogs {
			scopes = append(scopes, " IN DATABASE "+quoteIdentifier(name))
		}
	default:
		scopes = []string{" IN ACCOUNT"}
	}
	seen := make(map[string]struct{}, len(scopes))
	unique := scopes[:0]
	for _, scope := range scopes {
		if _, exists := seen[scope]; !exists {
			seen[scope] = struct{}{}
			unique = append(unique, scope)
		}
	}
	return unique
}

func (objects literalObjects) info(depth adbc.ObjectDepth) []driverbase.GetObjectsInfo {
	tablesBySchema := make(map[schemaEntry][]driverbase.TableInfo)
	for _, table := range objects.tables {
		key := schemaEntry{dbName: table.dbName, schemaName: table.schemaName}
		tablesBySchema[key] = append(tablesBySchema[key], driverbase.TableInfo{TableName: table.tableName, TableType: table.tableType})
	}
	schemasByCatalog := make(map[string][]driverbase.DBSchemaInfo)
	for _, schema := range objects.schemas {
		info := driverbase.DBSchemaInfo{DbSchemaName: new(schema.schemaName)}
		if depth != adbc.ObjectDepthDBSchemas {
			info.DbSchemaTables = tablesBySchema[schema]
			if info.DbSchemaTables == nil {
				info.DbSchemaTables = []driverbase.TableInfo{}
			}
		}
		schemasByCatalog[schema.dbName] = append(schemasByCatalog[schema.dbName], info)
	}
	infos := make([]driverbase.GetObjectsInfo, 0, len(objects.catalogs))
	for _, catalog := range objects.catalogs {
		info := driverbase.GetObjectsInfo{CatalogName: new(catalog)}
		if depth != adbc.ObjectDepthCatalogs {
			info.CatalogDbSchemas = schemasByCatalog[catalog]
			if info.CatalogDbSchemas == nil {
				info.CatalogDbSchemas = []driverbase.DBSchemaInfo{}
			}
		}
		infos = append(infos, info)
	}
	return infos
}

// Only project fields used by GetObjects. An empty fallback has fewer columns
// than a real SHOW result, so SELECT * is not safe when combining results.
func resultScanUnion(queryIDs []string, columns string) string {
	queries := make([]string, 0, len(queryIDs))
	for _, id := range queryIDs {
		queries = append(queries, "SELECT "+columns+" FROM TABLE(RESULT_SCAN('"+strings.ReplaceAll(id, "'", "''")+"'))")
	}
	return strings.Join(queries, " UNION ALL ")
}

func (c *connectionImpl) getLiteralScopedQueryID(ctx context.Context, command string, scopes []string, columns, emptyQuery string) (string, error) {
	queryIDs := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		id, err := getQueryID(ctx, command+scope, c.cn, emptyQuery)
		if err != nil {
			return "", err
		}
		queryIDs = append(queryIDs, id)
	}
	switch len(queryIDs) {
	case 0:
		return getQueryID(ctx, emptyQuery, c.cn, "")
	case 1:
		return queryIDs[0], nil
	default:
		return getQueryID(ctx, resultScanUnion(queryIDs, columns), c.cn, "")
	}
}

func (c *connectionImpl) getObjectsLiteral(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	objects, err := c.resolveLiteralObjects(ctx, depth, catalog, dbSchema, tableName, tableType)
	if err != nil {
		return nil, err
	}
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas || depth == adbc.ObjectDepthTables || len(objects.schemas) == 0 {
		return buildGetObjectsResult(c.Alloc, objects.info(depth)...)
	}

	queries := []struct {
		name, command, columns, empty string
	}{
		{"PK_QUERY_ID", "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */", keyColumns, emptyPkUkQuery},
		{"FK_QUERY_ID", "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */", fkColumns, emptyFkQuery},
		{"UNIQUE_QUERY_ID", "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */", keyColumns, emptyPkUkQuery},
		{"SHOW_COLUMNS_QUERY_ID", "SHOW COLUMNS /* ADBC:getObjects */", columnColumns, emptyColumnsQuery},
	}
	scopes := objects.scopes(catalog, dbSchema, tableName)
	args := []sql.NamedArg{
		metadataPatternArg("CATALOG", catalog, true),
		metadataPatternArg("DB_SCHEMA", dbSchema, true),
		metadataPatternArg("TABLE", tableName, true),
		metadataPatternArg("COLUMN", columnName, true),
	}
	ids := make([]string, len(queries))
	group, groupCtx := errgroup.WithContext(ctx)
	for i, query := range queries {
		group.Go(func() error {
			var err error
			ids[i], err = c.getLiteralScopedQueryID(groupCtx, query.command, scopes, query.columns, query.empty)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	for i, query := range queries {
		args = append(args, sql.Named(query.name, ids[i]))
	}
	return c.queryObjects(ctx, queryGetObjectsAll, args)
}
