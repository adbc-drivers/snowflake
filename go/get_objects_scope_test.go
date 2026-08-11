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

import "testing"

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
			if got := showColumnsScope(tt.catalog, tt.schema, tt.table); got != tt.want {
				t.Errorf("showColumnsScope() = %q, want %q", got, tt.want)
			}
		})
	}
}
