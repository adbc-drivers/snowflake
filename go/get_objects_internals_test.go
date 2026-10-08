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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShowColumnsScope(t *testing.T) {
	sp := func(s string) *string { return &s }

	tests := []struct {
		name                   string
		catalog, schema, table *string
		want                   string
	}{
		{"concrete names with underscores scope to the table", sp("DB_NAME"), sp("TEST_SCHEMA"), sp("LINEITEM"), ` IN TABLE "DB_NAME"."TEST_SCHEMA"."LINEITEM"`},
		{"underscore is treated as a literal, not a single-char wildcard", sp("FOO_BAR"), sp("SCH"), sp("TBL"), ` IN TABLE "FOO_BAR"."SCH"."TBL"`},
		{"wildcard table scopes to the schema", sp("DB_NAME"), sp("TEST_SCHEMA"), sp("%"), ` IN SCHEMA "DB_NAME"."TEST_SCHEMA"`},
		{"nil schema scopes to the database", sp("DB_NAME"), nil, nil, ` IN DATABASE "DB_NAME"`},
		{"nil catalog falls back to account", nil, nil, nil, " IN ACCOUNT"},
		{"percent catalog falls back to account", sp("%"), sp("S"), sp("T"), " IN ACCOUNT"},
		{"catalog containing percent falls back to account", sp("DB_%"), nil, nil, " IN ACCOUNT"},
		{"dot-star catalog falls back to account", sp(".*"), nil, nil, " IN ACCOUNT"},
		{"empty catalog falls back to account", sp(""), nil, nil, " IN ACCOUNT"},
		{"percent in table scopes to the schema", sp("DB"), sp("SCH"), sp("LINE%"), ` IN SCHEMA "DB"."SCH"`},
		{"embedded quote is escaped", sp(`DB"X`), sp("S"), sp("T"), ` IN TABLE "DB""X"."S"."T"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, showColumnsScope(tt.catalog, tt.schema, tt.table))
		})
	}
}

func TestShowTerseQuery(t *testing.T) {
	tests := []struct {
		query                        string
		objType                      string
		catalog, dbSchema, tableName *string
	}{
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN DATABASE "DB"`,
			objType: objSchemas,
			catalog: new("DB"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ SCHEMAS LIKE 'S%' IN DATABASE "DB"`,
			objType:  objSchemas,
			catalog:  new("DB"),
			dbSchema: new("S%"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ TABLES IN DATABASE "DB"`,
			objType: objTables,
			catalog: new("DB"),
		},
		{
			query:     `SHOW TERSE /* ADBC:getObjects */ TABLES LIKE 'T_%' IN SCHEMA "DB"."SCHEMA"`,
			objType:   objTables,
			catalog:   new("DB"),
			dbSchema:  new("SCHEMA"),
			tableName: new("T_%"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ DATABASES IN ACCOUNT`,
			objType: "DATABASES",
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ DATABASES LIKE 'foobar_catalog' IN ACCOUNT`,
			objType: "DATABASES",
			catalog: new("foobar_catalog"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ DATABASES LIKE 'foobar%catalog' IN ACCOUNT`,
			objType: "DATABASES",
			catalog: new("foobar%catalog"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN ACCOUNT`,
			objType: "SCHEMAS",
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ SCHEMAS LIKE 'foobar_schema' IN ACCOUNT`,
			objType:  "SCHEMAS",
			dbSchema: new("foobar_schema"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN ACCOUNT`,
			objType: "SCHEMAS",
			catalog: new("foobar_catalog"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ SCHEMAS LIKE 'foobar_schema' IN ACCOUNT`,
			objType:  "SCHEMAS",
			catalog:  new("foobar_catalog"),
			dbSchema: new("foobar_schema"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN ACCOUNT`,
			objType: "SCHEMAS",
			catalog: new("foobar%catalog"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ SCHEMAS LIKE 'foobar_schema' IN ACCOUNT`,
			objType:  "SCHEMAS",
			catalog:  new("foobar%catalog"),
			dbSchema: new("foobar_schema"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType: "TABLES",
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType: "TABLES",
			catalog: new("foobar_catalog"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType:  "TABLES",
			catalog:  new("foobar_catalog"),
			dbSchema: new("foobar_schema"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType:  "TABLES",
			catalog:  new("foobar_catalog"),
			dbSchema: new("foobar%schema"),
		},
		{
			query:     `SHOW TERSE /* ADBC:getObjects */ TABLES LIKE 'foobar%table' IN DATABASE "foobarcatalog"`,
			objType:   "TABLES",
			catalog:   new("foobarcatalog"),
			dbSchema:  new("foobar%schema"),
			tableName: new("foobar%table"),
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType: "TABLES",
			catalog: new("foobar%catalog"),
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType:  "TABLES",
			catalog:  new("foobar%catalog"),
			dbSchema: new("foobar_schema"),
		},
	}

	for _, tt := range tests {
		var testName strings.Builder
		testName.WriteString(tt.objType)
		if tt.catalog != nil {
			fmt.Fprintf(&testName, ";catalog=%s", *tt.catalog)
		}
		if tt.dbSchema != nil {
			fmt.Fprintf(&testName, ";schema=%s", *tt.dbSchema)
		}
		if tt.tableName != nil {
			fmt.Fprintf(&testName, ";table=%s", *tt.tableName)
		}
		t.Run(testName.String(), func(t *testing.T) {
			query, err := showTerseQuery(tt.objType, tt.catalog, tt.dbSchema, tt.tableName)
			assert.NoError(t, err)
			require.Equal(t, tt.query, query)
		})
	}
}

func TestAddLike(t *testing.T) {
	for _, tt := range []struct {
		name              string
		pattern           *string
		wildcard, literal string
	}{
		{"nil", nil, "", ""},
		{"empty", new(""), "", ""},
		{"percent", new("%"), "", ""},
		{"underscore", new("_"), " LIKE '_'", " LIKE '_'"},
		{"dot-star", new(".*"), "", " LIKE '.*'"},
		{"quote", new("a'b"), ` LIKE 'a\'b'`, ` LIKE 'a\'b'`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var query strings.Builder
			addLike(&query, tt.pattern, false)
			assert.Equal(t, tt.wildcard, query.String())
			query.Reset()
			addLike(&query, tt.pattern, true)
			assert.Equal(t, tt.literal, query.String())
		})
	}
}

func TestMetadataPatternArg(t *testing.T) {
	tests := []struct {
		name               string
		pattern            *string
		wildcards, literal string
	}{
		{"nil", nil, "%", "%"},
		{"empty", new(""), "", ""},
		{"plain", new("table"), "table", "table"},
		{"percent", new("%"), "%", "!%"},
		{"underscore", new("_"), "_", "!_"},
		{"escape character", new("!"), "!!", "!!"},
		{"mixed", new("a!_%!!b"), "a!!_%!!!!b", "a!!!_!%!!!!b"},
		{"quote and backslash", new(`a'\b`), `a'\b`, `a'\b`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arg := metadataPatternArg("TABLE", tt.pattern, false)
			assert.Equal(t, "TABLE", arg.Name)
			assert.Equal(t, tt.wildcards, arg.Value)
			arg = metadataPatternArg("TABLE", tt.pattern, true)
			assert.Equal(t, "TABLE", arg.Name)
			assert.Equal(t, tt.literal, arg.Value)
		})
	}
}

func TestMatchesLiteralName(t *testing.T) {
	for _, tt := range []struct {
		name   string
		filter *string
		want   bool
	}{
		{"anything", nil, true},
		{"", new(""), false},
		{"table", new(""), false},
		{"TaBlE", new("table"), true},
		{"table_suffix", new("table"), false},
		{"a_%!", new("A_%!"), true},
		{"aXother!", new("a_%!"), false},
		{".*", new(".*"), true},
	} {
		assert.Equal(t, tt.want, matchesLiteralName(tt.name, tt.filter), "%q, %v", tt.name, tt.filter)
	}
}

func TestLiteralObjectScopes(t *testing.T) {
	objects := literalObjects{
		catalogs: []string{"DB_NAME"},
		schemas:  []schemaEntry{{dbName: "DB_NAME", schemaName: "Schema_Name"}},
		tables: []tableEntry{
			{dbName: "DB_NAME", schemaName: "Schema_Name", tableName: "table_%"},
			{dbName: "DB_NAME", schemaName: "Schema_Name", tableName: "TABLE_%"},
			{dbName: "DB_NAME", schemaName: "Schema_Name", tableName: "table_%"},
		},
	}
	assert.Equal(t, []string{" IN ACCOUNT"}, objects.scopes(nil, nil, nil))
	assert.Equal(t, []string{` IN DATABASE "DB_NAME"`}, objects.scopes(new("db_name"), nil, nil))
	assert.Equal(t, []string{` IN SCHEMA "DB_NAME"."Schema_Name"`}, objects.scopes(new("db_name"), new("schema_name"), nil))
	assert.Equal(t, []string{
		` IN TABLE "DB_NAME"."Schema_Name"."table_%"`,
		` IN TABLE "DB_NAME"."Schema_Name"."TABLE_%"`,
	}, objects.scopes(new("db_name"), new("schema_name"), new("TABLE_%")))
	assert.Empty(t, (literalObjects{}).scopes(new("db_name"), new("schema_name"), new("")))
	quoted := literalObjects{
		catalogs: []string{`DB"%`},
		schemas:  []schemaEntry{{dbName: `DB"%`, schemaName: `S"_`}},
		tables:   []tableEntry{{dbName: `DB"%`, schemaName: `S"_`, tableName: `T"%`}},
	}
	assert.Equal(t, []string{` IN DATABASE "DB""%"`}, quoted.scopes(new(`db"%`), nil, nil))
	assert.Equal(t, []string{` IN SCHEMA "DB""%"."S""_"`}, quoted.scopes(new(`db"%`), new(`s"_`), nil))
	assert.Equal(t, []string{` IN TABLE "DB""%"."S""_"."T""%"`}, quoted.scopes(new(`db"%`), new(`s"_`), new(`t"%`)))
}

func TestLiteralObjectsInfo(t *testing.T) {
	objects := literalObjects{
		catalogs: []string{"DB"},
		schemas: []schemaEntry{
			{dbName: "DB", schemaName: "schema"},
			{dbName: "DB", schemaName: "SCHEMA"},
		},
		tables: []tableEntry{
			{dbName: "DB", schemaName: "schema", tableName: "table", tableType: "TABLE"},
			{dbName: "DB", schemaName: "SCHEMA", tableName: "TABLE", tableType: "TABLE"},
		},
	}
	info := objects.info(adbc.ObjectDepthTables)
	require.Len(t, info, 1)
	require.Len(t, info[0].CatalogDbSchemas, 2)
	assert.Equal(t, "table", info[0].CatalogDbSchemas[0].DbSchemaTables[0].TableName)
	assert.Equal(t, "TABLE", info[0].CatalogDbSchemas[1].DbSchemaTables[0].TableName)
	assert.Nil(t, objects.info(adbc.ObjectDepthCatalogs)[0].CatalogDbSchemas)
	assert.Nil(t, objects.info(adbc.ObjectDepthDBSchemas)[0].CatalogDbSchemas[0].DbSchemaTables)
}

func TestResultScanUnion(t *testing.T) {
	assert.Equal(t, `SELECT "name" FROM TABLE(RESULT_SCAN('first')) UNION ALL SELECT "name" FROM TABLE(RESULT_SCAN('second'))`, resultScanUnion([]string{"first", "second"}, `"name"`))
}
