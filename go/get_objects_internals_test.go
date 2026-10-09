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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShowColumnsScope(t *testing.T) {
	sp := func(s string) *string { return &s }

	tests := []struct {
		name                   string
		catalog, schema, table *string
		disableWildcards       bool
		want                   string
	}{
		{"concrete names with underscores scope to the table", sp("DB_NAME"), sp("TEST_SCHEMA"), sp("LINEITEM"), false, ` IN TABLE "DB_NAME"."TEST_SCHEMA"."LINEITEM"`},
		{"underscore is treated as a literal, not a single-char wildcard", sp("FOO_BAR"), sp("SCH"), sp("TBL"), false, ` IN TABLE "FOO_BAR"."SCH"."TBL"`},
		{"wildcard table scopes to the schema", sp("DB_NAME"), sp("TEST_SCHEMA"), sp("%"), false, ` IN SCHEMA "DB_NAME"."TEST_SCHEMA"`},
		{"nil schema scopes to the database", sp("DB_NAME"), nil, nil, false, ` IN DATABASE "DB_NAME"`},
		{"nil catalog falls back to account", nil, nil, nil, false, " IN ACCOUNT"},
		{"percent catalog falls back to account", sp("%"), sp("S"), sp("T"), false, " IN ACCOUNT"},
		{"catalog containing percent falls back to account", sp("DB_%"), nil, nil, false, " IN ACCOUNT"},
		{"dot-star catalog falls back to account", sp(".*"), nil, nil, false, " IN ACCOUNT"},
		{"empty catalog falls back to account", sp(""), nil, nil, false, " IN ACCOUNT"},
		{"percent in table scopes to the schema", sp("DB"), sp("SCH"), sp("LINE%"), false, ` IN SCHEMA "DB"."SCH"`},
		{"embedded quote is escaped", sp(`DB"X`), sp("S"), sp("T"), false, ` IN TABLE "DB""X"."S"."T"`},
		{"disabled wildcards treat percent names literally", sp("%"), sp("%"), sp("%"), true, ` IN TABLE "%"."%"."%"`},
		{"disabled wildcards treat embedded percent and underscore literally", sp("DB_%"), sp("SCHEMA_%"), sp("TABLE_%"), true, ` IN TABLE "DB_%"."SCHEMA_%"."TABLE_%"`},
		{"disabled wildcards treat dot-star literally", sp(".*"), sp("S"), sp("T"), true, ` IN TABLE ".*"."S"."T"`},
		{"disabled wildcards with nil catalog fall back to account", nil, sp("S"), sp("T"), true, " IN ACCOUNT"},
		{"disabled wildcards with empty catalog fall back to account", sp(""), sp("S"), sp("T"), true, " IN ACCOUNT"},
		{"disabled wildcards with nil schema scope to literal database", sp("DB_%"), nil, sp("T"), true, ` IN DATABASE "DB_%"`},
		{"disabled wildcards with empty schema scope to literal database", sp("DB_%"), sp(""), sp("T"), true, ` IN DATABASE "DB_%"`},
		{"disabled wildcards with nil table scope to literal schema", sp("DB_%"), sp("SCHEMA_%"), nil, true, ` IN SCHEMA "DB_%"."SCHEMA_%"`},
		{"disabled wildcards with empty table scope to literal schema", sp("DB_%"), sp("SCHEMA_%"), sp(""), true, ` IN SCHEMA "DB_%"."SCHEMA_%"`},
		{"disabled wildcards escape embedded quotes", sp(`DB"%`), sp(`S"_`), sp(`T"%`), true, ` IN TABLE "DB""%"."S""_"."T""%"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, showColumnsScope(tt.catalog, tt.schema, tt.table, tt.disableWildcards))
		})
	}
}

func TestShowTerseQuery(t *testing.T) {
	tests := []struct {
		query                        string
		objType                      string
		catalog, dbSchema, tableName *string
		disableWildcards             bool
	}{
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
			query:            `SHOW TERSE /* ADBC:getObjects */ DATABASES IN ACCOUNT STARTS WITH 'foobar_catalog'`,
			objType:          "DATABASES",
			catalog:          new("foobar_catalog"),
			disableWildcards: true,
		},
		{
			query:   `SHOW TERSE /* ADBC:getObjects */ DATABASES LIKE 'foobar%catalog' IN ACCOUNT`,
			objType: "DATABASES",
			catalog: new("foobar%catalog"),
		},
		{
			query:            `SHOW TERSE /* ADBC:getObjects */ DATABASES IN ACCOUNT STARTS WITH 'foobar%catalog'`,
			objType:          "DATABASES",
			catalog:          new("foobar%catalog"),
			disableWildcards: true,
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
			query:            `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN DATABASE "foobar_catalog"`,
			objType:          "SCHEMAS",
			catalog:          new("foobar_catalog"),
			disableWildcards: true,
		},
		{
			query:            `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN DATABASE "foobar_catalog" STARTS WITH 'foobar_schema'`,
			objType:          "SCHEMAS",
			catalog:          new("foobar_catalog"),
			dbSchema:         new("foobar_schema"),
			disableWildcards: true,
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
			query:            `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN DATABASE "foobar%catalog"`,
			objType:          "SCHEMAS",
			catalog:          new("foobar%catalog"),
			disableWildcards: true,
		},
		{
			query:            `SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN DATABASE "foobar%catalog" STARTS WITH 'foobar_schema'`,
			objType:          "SCHEMAS",
			catalog:          new("foobar%catalog"),
			dbSchema:         new("foobar_schema"),
			disableWildcards: true,
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
			query:            `SHOW TERSE /* ADBC:getObjects */ TABLES IN DATABASE "foobar_catalog"`,
			objType:          "TABLES",
			catalog:          new("foobar_catalog"),
			disableWildcards: true,
		},
		{
			query:    `SHOW TERSE /* ADBC:getObjects */ TABLES IN ACCOUNT`,
			objType:  "TABLES",
			catalog:  new("foobar_catalog"),
			dbSchema: new("foobar_schema"),
		},
		{
			query:            `SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "foobar_catalog"."foobar_schema"`,
			objType:          "TABLES",
			catalog:          new("foobar_catalog"),
			dbSchema:         new("foobar_schema"),
			disableWildcards: true,
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
			query:            `SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "foobar_catalog"."foobar%schema" STARTS WITH 'foobar%table'`,
			objType:          "TABLES",
			catalog:          new("foobar_catalog"),
			dbSchema:         new("foobar%schema"),
			tableName:        new("foobar%table"),
			disableWildcards: true,
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
		if tt.disableWildcards {
			testName.WriteString(";disableWildcards")
		}
		t.Run(testName.String(), func(t *testing.T) {
			query, err := showTerseQuery(tt.objType, tt.catalog, tt.dbSchema, tt.tableName, tt.disableWildcards)
			assert.NoError(t, err)
			require.Equal(t, tt.query, query)
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

func TestAddStartsWith(t *testing.T) {
	for _, tt := range []struct {
		name    string
		pattern *string
		want    string
	}{
		{"nil", nil, ""},
		{"empty", new(""), ""},
		{"wildcard characters", new("a_%!"), " STARTS WITH 'a_%!'"},
		{"quote and backslash", new(`a\n'b`), ` STARTS WITH 'a\\n''b'`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var query strings.Builder
			addStartsWith(&query, tt.pattern, true)
			assert.Equal(t, tt.want, query.String())
			query.Reset()
			addStartsWith(&query, tt.pattern, false)
			assert.Empty(t, query.String())
		})
	}
}

func TestBuildShowTerseQuery(t *testing.T) {
	for _, tt := range []struct {
		name             string
		pattern          *string
		disableWildcards bool
		want             string
	}{
		{"unfiltered", nil, true, `SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "DB"."S"`},
		{"wildcards", new("T_%"), false, `SHOW TERSE /* ADBC:getObjects */ TABLES LIKE 'T_%' IN SCHEMA "DB"."S"`},
		{"literal", new("T_%"), true, `SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "DB"."S" STARTS WITH 'T_%'`},
		{"quotes and backslashes", new(`T\n'X`), true, `SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "DB"."S" STARTS WITH 'T\\n''X'`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, buildShowTerseQuery(objTables, tt.pattern, ` IN SCHEMA "DB"."S"`, tt.disableWildcards))
		})
	}
}

func TestShowTerseEmptyLiteralFilters(t *testing.T) {
	for _, objType := range []string{objDatabases, objSchemas, objTables, objViews, objObjects} {
		t.Run(objType, func(t *testing.T) {
			filters := []*string{new("DB"), new("S"), new("T")}
			count := 3
			switch objType {
			case objDatabases:
				count = 1
			case objSchemas:
				count = 2
			}
			for i := range filters {
				original := filters[i]
				filters[i] = new("")
				query, err := showTerseQuery(objType, filters[0], filters[1], filters[2], true)
				require.NoError(t, err)
				if i < count {
					assert.True(t, strings.HasPrefix(query, "SELECT NULL::VARCHAR"))
					assert.True(t, strings.HasSuffix(query, "WHERE FALSE"))
				} else {
					assert.True(t, strings.HasPrefix(query, "SHOW TERSE"))
				}
				filters[i] = original
			}
		})
	}
	_, err := showTerseQuery("unsupported", nil, nil, nil, true)
	require.Error(t, err)
	assert.False(t, hasEmptyLiteralFilter(false, new("")))
	assert.False(t, hasEmptyLiteralFilter(true, nil, new("%")))
	assert.True(t, hasEmptyLiteralFilter(true, nil, new("")))
}
