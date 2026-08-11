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
	"database/sql/driver"
	"errors"
	"io"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/snowflakedb/gosnowflake/v2"
	"golang.org/x/sync/errgroup"
)

// This file contains optimized implementations of GetObjects that read SHOW
// TERSE results directly instead of going through RESULT_SCAN SQL templates.
// This reduces the number of Snowflake round-trips for common query patterns.
//
// The key optimization targets ObjectDepthTables with a specific catalog:
//   - Specific catalog + specific schema: 1 query instead of 4
//   - Specific catalog + wildcard schema: 2 parallel queries instead of 4
//
// ObjectDepthDBSchemas with specific catalog: 1 query instead of 3

// getObjectsDirectPath attempts to handle GetObjects more efficiently by
// reading SHOW TERSE results directly. Returns (nil, nil) if no fast path
// applies, signaling the caller to use the existing SQL template path.
func (c *connectionImpl) getObjectsDirectPath(
	ctx context.Context,
	depth adbc.ObjectDepth,
	catalog, dbSchema, tableName *string,
	tableType []string,
	hasViews, hasTables bool,
) (array.RecordReader, error) {
	specificCatalog := catalog != nil && !isWildcardStr(*catalog)

	switch depth {
	case adbc.ObjectDepthDBSchemas:
		if specificCatalog {
			return c.getObjectsDBSchemasDirect(ctx, *catalog, dbSchema)
		}
	case adbc.ObjectDepthTables:
		if len(tableType) > 0 && !hasViews && !hasTables {
			// tableType was specified but doesn't include TABLE or VIEW:
			// return an empty result without any round-trips.
			return buildGetObjectsResult(c.Alloc)
		}
		if specificCatalog {
			specificSchema := dbSchema != nil && !isWildcardStr(*dbSchema)
			return c.getObjectsTablesDirect(ctx, *catalog, dbSchema, tableName, tableType, specificSchema)
		}
	}

	return nil, nil
}

// getObjectsDBSchemasDirect handles ObjectDepthDBSchemas with a specific
// catalog. Executes 1 SHOW query instead of 3 (2 SHOW + 1 RESULT_SCAN SQL).
func (c *connectionImpl) getObjectsDBSchemasDirect(
	ctx context.Context,
	catalog string,
	dbSchema *string,
) (array.RecordReader, error) {
	schemas, err := c.execShowSchemas(ctx, dbSchema, " IN DATABASE "+quoteTblName(catalog))
	if err != nil {
		return nil, err
	}

	dbSchemas := make([]driverbase.DBSchemaInfo, 0, len(schemas))
	for _, s := range schemas {
		sn := s.schemaName
		dbSchemas = append(dbSchemas, driverbase.DBSchemaInfo{
			DbSchemaName: &sn,
		})
	}

	cat := catalog
	return buildGetObjectsResult(c.Alloc, driverbase.GetObjectsInfo{
		CatalogName:      &cat,
		CatalogDbSchemas: dbSchemas,
	})
}

// getObjectsTablesDirect handles ObjectDepthTables with a specific catalog.
//   - With specific schema: 1 SHOW query (instead of 4)
//   - With wildcard/nil schema: 2 parallel SHOW queries (instead of 4)
func (c *connectionImpl) getObjectsTablesDirect(
	ctx context.Context,
	catalog string,
	dbSchema, tableName *string,
	tableType []string,
	specificSchema bool,
) (array.RecordReader, error) {
	objType := showObjType(tableType)
	escapedCatalog := quoteTblName(catalog)

	if specificSchema {
		return c.getObjectsTablesInSchema(ctx, catalog, *dbSchema, tableName, objType, escapedCatalog)
	}
	return c.getObjectsTablesInDatabase(ctx, catalog, dbSchema, tableName, objType, escapedCatalog)
}

// getObjectsTablesInSchema is the most optimized path: specific catalog + schema.
// Executes a single SHOW TERSE query and builds the GetObjects result in Go.
func (c *connectionImpl) getObjectsTablesInSchema(
	ctx context.Context,
	catalog, dbSchema string,
	tableName *string,
	objType, escapedCatalog string,
) (array.RecordReader, error) {
	suffix := " IN SCHEMA " + escapedCatalog + "." + quoteTblName(dbSchema)
	entries, err := c.execShowTables(ctx, objType, tableName, suffix)
	if err != nil {
		return nil, err
	}

	tables := make([]driverbase.TableInfo, 0, len(entries))
	for _, e := range entries {
		tables = append(tables, driverbase.TableInfo{
			TableName: e.tableName,
			TableType: e.tableType,
		})
	}

	cat := catalog
	sch := dbSchema
	return buildGetObjectsResult(c.Alloc, driverbase.GetObjectsInfo{
		CatalogName: &cat,
		CatalogDbSchemas: []driverbase.DBSchemaInfo{{
			DbSchemaName:   &sch,
			DbSchemaTables: tables,
		}},
	})
}

// getObjectsTablesInDatabase handles specific catalog with wildcard/nil schema.
// Executes 2 parallel SHOW TERSE queries and groups results in Go.
func (c *connectionImpl) getObjectsTablesInDatabase(
	ctx context.Context,
	catalog string,
	dbSchema, tableName *string,
	objType, escapedCatalog string,
) (array.RecordReader, error) {
	var (
		schemas      []schemaEntry
		tableEntries []tableEntry
	)

	dbSuffix := " IN DATABASE " + escapedCatalog
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		schemas, err = c.execShowSchemas(gCtx, dbSchema, dbSuffix)
		return err
	})
	g.Go(func() error {
		var err error
		tableEntries, err = c.execShowTables(gCtx, objType, tableName, dbSuffix)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Build set of matching schema names for filtering
	schemaSet := make(map[string]struct{}, len(schemas))
	for _, s := range schemas {
		schemaSet[s.schemaName] = struct{}{}
	}

	// Group tables by schema, only including tables in matching schemas
	tablesBySchema := make(map[string][]driverbase.TableInfo)
	for _, e := range tableEntries {
		if _, ok := schemaSet[e.schemaName]; ok {
			tablesBySchema[e.schemaName] = append(
				tablesBySchema[e.schemaName],
				driverbase.TableInfo{
					TableName: e.tableName,
					TableType: e.tableType,
				},
			)
		}
	}

	// Build DBSchemaInfo list preserving schema order from SHOW
	dbSchemas := make([]driverbase.DBSchemaInfo, 0, len(schemas))
	for _, s := range schemas {
		sn := s.schemaName
		tables := tablesBySchema[sn]
		if tables == nil {
			tables = []driverbase.TableInfo{}
		}
		dbSchemas = append(dbSchemas, driverbase.DBSchemaInfo{
			DbSchemaName:   &sn,
			DbSchemaTables: tables,
		})
	}

	cat := catalog
	return buildGetObjectsResult(c.Alloc, driverbase.GetObjectsInfo{
		CatalogName:      &cat,
		CatalogDbSchemas: dbSchemas,
	})
}

type tableEntry struct {
	dbName     string
	schemaName string
	tableName  string
	tableType  string
}

type schemaEntry struct {
	dbName     string
	schemaName string
}

// showObjType returns the SHOW object type string based on the tableType filter.
func showObjType(tableType []string) string {
	if len(tableType) == 1 {
		if strings.EqualFold("VIEW", tableType[0]) {
			return objViews
		}
		if strings.EqualFold("TABLE", tableType[0]) {
			return objTables
		}
	}
	return objObjects
}

// execShowTables executes a SHOW TERSE command for tables/objects/views
// and reads the results directly into a slice.
func (c *connectionImpl) execShowTables(ctx context.Context, objType string, pattern *string, suffix string) (entries []tableEntry, err error) {
	query := "SHOW TERSE /* ADBC:getObjects */ " + objType
	query = addLike(query, pattern)
	query += suffix

	rows, err := c.cn.QueryContext(ctx, query, nil)
	if err != nil {
		var sfErr *gosnowflake.SnowflakeError
		if errors.As(err, &sfErr) && sfErr.Number == 2043 {
			return nil, nil
		}
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	return readTableEntries(rows)
}

func readTableEntries(rows driver.Rows) ([]tableEntry, error) {
	cols := rows.Columns()
	nameIdx, kindIdx, dbIdx, schIdx := -1, -1, -1, -1
	for i, col := range cols {
		switch col {
		case "name":
			nameIdx = i
		case "kind":
			kindIdx = i
		case "database_name":
			dbIdx = i
		case "schema_name":
			schIdx = i
		}
	}

	dest := make([]driver.Value, len(cols))
	var entries []tableEntry
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		entry := tableEntry{}
		if nameIdx >= 0 {
			entry.tableName = dest[nameIdx].(string)
		}
		if kindIdx >= 0 {
			entry.tableType = dest[kindIdx].(string)
		}
		if dbIdx >= 0 {
			entry.dbName = dest[dbIdx].(string)
		}
		if schIdx >= 0 {
			entry.schemaName = dest[schIdx].(string)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// execShowSchemas executes a SHOW TERSE SCHEMAS command and reads the results directly.
func (c *connectionImpl) execShowSchemas(ctx context.Context, pattern *string, suffix string) (_ []schemaEntry, err error) {
	query := "SHOW TERSE /* ADBC:getObjects */ " + objSchemas
	query = addLike(query, pattern)
	query += suffix

	rows, err := c.cn.QueryContext(ctx, query, nil)
	if err != nil {
		var sfErr *gosnowflake.SnowflakeError
		// error code 2043 is what you get when a `SHOW` command doesn't match
		// anything (e.g. SHOW TERSE DATABASE "nonexistent"). In this case, we
		// want to return an empty set rather than a failure.
		if errors.As(err, &sfErr) && sfErr.Number == 2043 {
			return nil, nil
		}
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// SHOW TERSE is not actually fixed and has changed between versions in the past,
	// so we look up by name to avoid possible breaking if snowflake adds, removes,
	// or reorders columns in the future.
	cols := rows.Columns()
	nameIdx, dbIdx := -1, -1
	for i, col := range cols {
		switch col {
		case "name":
			nameIdx = i
		case "database_name":
			dbIdx = i
		}
	}

	dest := make([]driver.Value, len(cols))
	var entries []schemaEntry
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		entry := schemaEntry{}
		if nameIdx >= 0 {
			entry.schemaName = dest[nameIdx].(string)
		}
		if dbIdx >= 0 {
			entry.dbName = dest[dbIdx].(string)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// buildGetObjectsResult creates a RecordReader from GetObjectsInfo values
// using the standard driverbase builder.
func buildGetObjectsResult(alloc memory.Allocator, infos ...driverbase.GetObjectsInfo) (array.RecordReader, error) {
	ch := make(chan driverbase.GetObjectsInfo, len(infos)+1)
	errCh := make(chan error)
	for _, info := range infos {
		ch <- info
	}
	close(ch)
	return driverbase.BuildGetObjectsRecordReader(alloc, ch, errCh)
}
