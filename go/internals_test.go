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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeSingleQuoteForLike(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"no quotes", "table_%", "table_%"},
		{"single quote", `'`, `\'`},
		{"leading quote", `'table`, `\'table`},
		{"trailing quote", `table'`, `table\'`},
		{"embedded quote", `ta'ble`, `ta\'ble`},
		{"consecutive quotes", `ta''ble`, `ta\'\'ble`},
		{"only consecutive quotes", `'''`, `\'\'\'`},
		{"backslash before quote", `ta\'ble`, `ta\'ble`},
		{"quote after backslash-prefixed quote", `ta\''ble`, `ta\'\'ble`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escapeSingleQuoteForLike(tt.input))
		})
	}
}
