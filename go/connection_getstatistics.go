// Copyright (c) 2026 ADBC Drivers Contributors
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
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

//go:embed queries/get_statistics_tables.sql
var queryGetStatisticsTables string

//go:embed queries/get_statistics_storage_metrics.sql
var queryGetStatisticsStorageMetrics string

// Custom Snowflake-specific statistic keys
const (
	SnowflakeStatisticBytesKey  = 1024
	SnowflakeStatisticBytesName = "snowflake.table.bytes"

	SnowflakeStatisticRetentionTimeDaysKey = 1025
	SnowflakeStatisticRetentionTimeName    = "snowflake.table.retention.time_days"

	SnowflakeStatisticActiveBytesKey  = 1026
	SnowflakeStatisticActiveBytesName = "snowflake.table.bytes.active"

	SnowflakeStatisticTimeTravelBytesKey  = 1027
	SnowflakeStatisticTimeTravelBytesName = "snowflake.table.bytes.time_travel"

	SnowflakeStatisticFailsafeBytesKey  = 1028
	SnowflakeStatisticFailsafeBytesName = "snowflake.table.bytes.failsafe"

	SnowflakeStatisticClusteringDepthKey  = 1029
	SnowflakeStatisticClusteringDepthName = "snowflake.table.clustering.depth"
)

const (
	// maxExactTables is the maximum number of tables for which we'll fetch expensive exact statistics.
	// When approximate=false and the candidate count exceeds this limit, we return StatusNotImplemented
	// to avoid performance cliffs on large Snowflake accounts.
	maxExactTables = 1000
)

func (c *connectionImpl) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {
	_, span := driverbase.StartSpan(ctx, "connectionImpl.GetStatisticNames", c)
	defer driverbase.EndSpan(span, nil)

	return driverbase.BuildGetStatisticNamesReader(c.Alloc, []driverbase.StatisticNameKey{
		{Name: SnowflakeStatisticBytesName, Key: SnowflakeStatisticBytesKey},
		{Name: SnowflakeStatisticRetentionTimeName, Key: SnowflakeStatisticRetentionTimeDaysKey},
		{Name: SnowflakeStatisticActiveBytesName, Key: SnowflakeStatisticActiveBytesKey},
		{Name: SnowflakeStatisticTimeTravelBytesName, Key: SnowflakeStatisticTimeTravelBytesKey},
		{Name: SnowflakeStatisticFailsafeBytesName, Key: SnowflakeStatisticFailsafeBytesKey},
		{Name: SnowflakeStatisticClusteringDepthName, Key: SnowflakeStatisticClusteringDepthKey},
	})
}

// GetStatistics returns table statistics from INFORMATION_SCHEMA.TABLES.
// Performance considerations:
//   - Exact mode (approximate=false): Fails with StatusNotImplemented if candidate count exceeds 1,000 tables.
//     Intended for narrow, targeted queries where detailed statistics justify the cost.
//   - Approximate mode (approximate=true): No limit on candidate count. Designed to handle arbitrarily
//     large result sets (e.g., account-wide scans) by returning only cheap base statistics.
//
// The approximate parameter controls query cost:
//   - Base statistics (row_count, bytes, retention_time) are always sourced from cached
//     INFORMATION_SCHEMA.TABLES (~30 min staleness) regardless of the approximate parameter,
//     and are always marked as approximate in the result.
//   - When approximate=true: Skip expensive per-table queries (TABLE_STORAGE_METRICS, SYSTEM$CLUSTERING_INFORMATION)
//   - When approximate=false: Include storage breakdown and clustering depth for matched tables;
//     these enriched statistics are marked as exact (approx=false).
func (c *connectionImpl) GetStatistics(
	ctx context.Context,
	catalog *string,
	dbSchema *string,
	tableName *string,
	approximate bool,
) (rdr array.RecordReader, err error) {
	ctx, span := driverbase.StartSpan(ctx, "connectionImpl.GetStatistics", c)
	defer func() {
		driverbase.EndSpan(span, err)
	}()

	// Build filter parameters (default to '%' for NULL to match all)
	schemaFilter := "%"
	if dbSchema != nil {
		schemaFilter = *dbSchema
	}
	tableFilter := "%"
	if tableName != nil {
		tableFilter = *tableName
	}

	if (catalog != nil && *catalog == "") || (dbSchema != nil && *dbSchema == "") || (tableName != nil && *tableName == "") {
		return driverbase.EmptyGetStatisticsReader()
	}

	// Determine which databases to query:
	var databasesToQuery []string

	if catalog != nil && !strings.ContainsAny(*catalog, "%_") {
		// Catalog is literal, query only that specific database
		databasesToQuery = []string{*catalog}
	} else {
		var query string
		if catalog == nil {
			query = "SHOW DATABASES"
		} else {
			query = fmt.Sprintf("SHOW DATABASES LIKE '%s'", strings.ReplaceAll(*catalog, "'", "''"))
		}

		rows, err := c.cn.QueryContext(ctx, query, nil)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		defer func() {
			err = errors.Join(err, rows.Close())
		}()

		columns := rows.Columns()
		numColumns := len(columns)
		if numColumns < 2 {
			return nil, errToAdbcErr(adbc.StatusInternal, fmt.Errorf("SHOW DATABASES returned %d columns, expected at least 2", numColumns))
		}

		dest := make([]driver.Value, numColumns)
		for {
			if err := rows.Next(dest); err != nil {
				if err == io.EOF {
					break
				}
				return nil, errToAdbcErr(adbc.StatusIO, err)
			}
			if dest[1] != nil {
				if dbName, ok := dest[1].(string); ok {
					databasesToQuery = append(databasesToQuery, dbName)
				}
			}
		}
	}

	if len(databasesToQuery) == 0 {
		return driverbase.EmptyGetStatisticsReader()
	}

	// Query tables from each matching database and group by catalog
	// In exact mode, track total count and fail fast if limit exceeded
	tablesByCatalog := make(map[string][]tableInfo)
	catalogOrder := []string{}
	totalTables := 0

	for _, db := range databasesToQuery {
		quotedDB := quoteIdentifier(db)
		query := fmt.Sprintf(queryGetStatisticsTables, quotedDB)

		nvargs := []driver.NamedValue{
			{Ordinal: 1, Value: schemaFilter},
			{Ordinal: 2, Value: tableFilter},
		}

		rows, err := c.cn.QueryContext(ctx, query, nvargs)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, fmt.Errorf("failed to query database %s: %w", db, err))
		}

		tables, err := readTableRows(rows)
		err = errors.Join(err, rows.Close())

		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, fmt.Errorf("failed to read table rows from database %s: %w", db, err))
		}

		if len(tables) > 0 {
			actualCatalog := tables[0].catalog
			if _, exists := tablesByCatalog[actualCatalog]; !exists {
				catalogOrder = append(catalogOrder, actualCatalog)
			}
			tablesByCatalog[actualCatalog] = append(tablesByCatalog[actualCatalog], tables...)
			totalTables += len(tables)

			// Early bailout in exact mode: fail fast once we exceed the limit
			// Approximate mode has no limit - it's designed to handle arbitrarily large result sets
			if !approximate && totalTables > maxExactTables {
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg: fmt.Sprintf(
						"exact statistics requested for more than %d tables; use approximate=true or narrower catalog/schema/table filters",
						maxExactTables,
					),
				}
			}
		}
	}

	if len(tablesByCatalog) == 0 {
		return driverbase.EmptyGetStatisticsReader()
	}

	// Build statistics for each catalog
	schemaOrderByCatalog := make(map[string][]string)
	statsByCatalog := make(map[string]map[string][]driverbase.Statistic)

	for _, catalogName := range catalogOrder {
		tables := tablesByCatalog[catalogName]

		// Query storage metrics for this specific catalog (expensive, skip if approximate=true)
		var storageMetrics map[string]map[string]storageMetric
		if !approximate {
			var err error
			storageMetrics, err = queryStorageMetrics(ctx, c.cn, catalogName, tables)
			if err != nil {
				// Storage metrics are optional (requires ACCOUNTADMIN)
				storageMetrics = make(map[string]map[string]storageMetric)
			}
		} else {
			storageMetrics = make(map[string]map[string]storageMetric)
		}

		// Build statistics grouped by schema
		statsBySchema := make(map[string][]driverbase.Statistic)
		schemaOrder := []string{}
		seenSchemas := make(map[string]bool)

		for _, ti := range tables {
			// Track schema order
			if !seenSchemas[ti.schema] {
				schemaOrder = append(schemaOrder, ti.schema)
				seenSchemas[ti.schema] = true
			}

			// Build base statistics (always included, always approximate since from cached INFORMATION_SCHEMA)
			stats := []driverbase.Statistic{
				driverbase.NewFloat64Stat(ti.table, nil, adbc.StatisticRowCountKey, float64(ti.rowCount), true),
				driverbase.NewInt64Stat(ti.table, nil, SnowflakeStatisticBytesKey, ti.bytes, true),
				driverbase.NewInt64Stat(ti.table, nil, SnowflakeStatisticRetentionTimeDaysKey, ti.retentionTime, true),
			}

			// Add storage breakdown if available and not in approximate mode
			if !approximate {
				if schemaMetrics, ok := storageMetrics[ti.schema]; ok {
					if sm, ok := schemaMetrics[ti.table]; ok {
						stats = append(stats,
							driverbase.NewInt64Stat(ti.table, nil, SnowflakeStatisticActiveBytesKey, sm.activeBytes, false),
							driverbase.NewInt64Stat(ti.table, nil, SnowflakeStatisticTimeTravelBytesKey, sm.timeTravelBytes, false),
							driverbase.NewInt64Stat(ti.table, nil, SnowflakeStatisticFailsafeBytesKey, sm.failsafeBytes, false),
						)
					}
				}
			}

			// Add clustering depth if clustering key exists (expensive, skip if approximate=true)
			if !approximate && ti.clusteringKey.Valid && ti.clusteringKey.String != "" {
				clusteringDepth, err := getClusteringDepth(ctx, c.cn, ti.catalog, ti.schema, ti.table)
				if err == nil {
					stats = append(stats, driverbase.NewFloat64Stat(ti.table, nil, SnowflakeStatisticClusteringDepthKey, clusteringDepth, false))
				}
			}

			statsBySchema[ti.schema] = append(statsBySchema[ti.schema], stats...)
		}

		schemaOrderByCatalog[catalogName] = schemaOrder
		statsByCatalog[catalogName] = statsBySchema
	}

	// Build Arrow structure from pre-computed statistics
	return driverbase.BuildGetStatisticsReader(c.Alloc, catalogOrder, schemaOrderByCatalog, statsByCatalog)
}

// tableInfo holds basic table information from INFORMATION_SCHEMA.TABLES
type tableInfo struct {
	catalog       string
	schema        string
	table         string
	rowCount      int64
	bytes         int64
	retentionTime int64
	clusteringKey sql.NullString
}

// readTableRows reads table information from query results
func readTableRows(rows driver.Rows) ([]tableInfo, error) {
	var tables []tableInfo
	dest := make([]driver.Value, 7)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		catalog := dest[0].(string)
		schema := dest[1].(string)
		table := dest[2].(string)

		var rowCount, bytes, retentionTime int64
		if v := dest[3]; v != nil {
			rowCount = convertToInt64(v)
		}
		if v := dest[4]; v != nil {
			bytes = convertToInt64(v)
		}
		if v := dest[5]; v != nil {
			retentionTime = convertToInt64(v)
		}

		var clusteringKey sql.NullString
		if dest[6] != nil {
			if s, ok := dest[6].(string); ok {
				clusteringKey = sql.NullString{String: s, Valid: true}
			}
		}

		tables = append(tables, tableInfo{
			catalog:       catalog,
			schema:        schema,
			table:         table,
			rowCount:      rowCount,
			bytes:         bytes,
			retentionTime: retentionTime,
			clusteringKey: clusteringKey,
		})
	}

	return tables, nil
}

// storageMetric holds storage breakdown from TABLE_STORAGE_METRICS
type storageMetric struct {
	activeBytes     int64
	timeTravelBytes int64
	failsafeBytes   int64
}

// queryStorageMetrics queries INFORMATION_SCHEMA.TABLE_STORAGE_METRICS for storage breakdown
// Note: Requires appropriate permissions (ACCOUNTADMIN) and may return empty for other roles
func queryStorageMetrics(ctx context.Context, conn driver.QueryerContext, catalog string, tables []tableInfo) (map[string]map[string]storageMetric, error) {
	if len(tables) == 0 || catalog == "" {
		return make(map[string]map[string]storageMetric), nil
	}

	// Build a list of (schema, table) pairs to filter by
	var pairs []string
	for _, t := range tables {
		pairs = append(pairs, fmt.Sprintf("(%s, %s)",
			quoteLiteral(t.schema),
			quoteLiteral(t.table)))
	}

	// Build the query filtered to only candidate tables
	query := fmt.Sprintf(queryGetStatisticsStorageMetrics, quoteIdentifier(catalog), strings.Join(pairs, ", "))

	nvargs := []driver.NamedValue{}
	rows, err := conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		// TABLE_STORAGE_METRICS may not be accessible (requires ACCOUNTADMIN)
		return nil, err
	}
	defer func() {
		if closer, ok := rows.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	result := make(map[string]map[string]storageMetric)
	dest := make([]driver.Value, 5)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		schema := dest[0].(string)
		table := dest[1].(string)
		activeBytes := convertToInt64(dest[2])
		timeTravelBytes := convertToInt64(dest[3])
		failsafeBytes := convertToInt64(dest[4])

		if result[schema] == nil {
			result[schema] = make(map[string]storageMetric)
		}

		result[schema][table] = storageMetric{
			activeBytes:     activeBytes,
			timeTravelBytes: timeTravelBytes,
			failsafeBytes:   failsafeBytes,
		}
	}

	return result, nil
}

// getClusteringDepth queries SYSTEM$CLUSTERING_INFORMATION to get the average clustering depth
func getClusteringDepth(ctx context.Context, conn driver.QueryerContext, catalog, schema, table string) (float64, error) {
	// Call SYSTEM$CLUSTERING_INFORMATION function
	// When the table has a clustering key, call without second argument
	// The function will use the table's existing clustering key
	fullyQualifiedTable := fmt.Sprintf("%s.%s.%s",
		quoteIdentifier(catalog),
		quoteIdentifier(schema),
		quoteIdentifier(table))

	query := fmt.Sprintf("SELECT SYSTEM$CLUSTERING_INFORMATION(%s)", quoteLiteral(fullyQualifiedTable))

	nvargs := []driver.NamedValue{}
	rows, err := conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closer, ok := rows.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, err
	}

	// Parse JSON result - SYSTEM$CLUSTERING_INFORMATION returns JSON string
	if dest[0] == nil {
		return 0, fmt.Errorf("null result from SYSTEM$CLUSTERING_INFORMATION")
	}

	jsonStr, ok := dest[0].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected type from SYSTEM$CLUSTERING_INFORMATION: %T", dest[0])
	}

	var result struct {
		AverageDepth float64 `json:"average_depth"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return 0, err
	}

	return result.AverageDepth, nil
}

// buildMultiCatalogStatisticsReader constructs the Arrow RecordReader for GetStatistics with multiple catalogs.
// This is a pure Arrow building function with no business logic or database queries, making it suitable
// for extraction to a shared library in the future.
// convertToInt64 converts various numeric types to int64
func convertToInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// quoteLiteral quotes a SQL string literal for use in WHERE clauses
func quoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `'`, `''`)
	return fmt.Sprintf(`'%s'`, escaped)
}
