// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowflake

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
)

// geoArrowType implements arrow.ExtensionType for testing geoarrow types.
type geoArrowType struct {
	arrow.ExtensionBase
	name string
}

func newGeoArrowType(name string, storage arrow.DataType) *geoArrowType {
	return &geoArrowType{
		ExtensionBase: arrow.ExtensionBase{Storage: storage},
		name:          name,
	}
}

func (g *geoArrowType) ExtensionName() string { return g.name }
func (g *geoArrowType) Serialize() string      { return "" }
func (g *geoArrowType) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	return newGeoArrowType(g.name, storage), nil
}
func (g *geoArrowType) ExtensionEquals(other arrow.ExtensionType) bool {
	return g.ExtensionName() == other.ExtensionName()
}
func (g *geoArrowType) ArrayType() reflect.Type {
	return reflect.TypeOf(array.ExtensionArrayBase{})
}

func TestToSnowflakeTypeGeoArrow(t *testing.T) {
	tests := []struct {
		name     string
		dt       arrow.DataType
		geoType  string
		expected string
	}{
		{
			name:     "geoarrow.wkb defaults to geography",
			dt:       newGeoArrowType("geoarrow.wkb", arrow.BinaryTypes.Binary),
			geoType:  "geography",
			expected: "geography",
		},
		{
			name:     "geoarrow.wkb with geometry option",
			dt:       newGeoArrowType("geoarrow.wkb", arrow.BinaryTypes.Binary),
			geoType:  "geometry",
			expected: "geometry",
		},
		{
			name:     "plain binary stays binary",
			dt:       arrow.BinaryTypes.Binary,
			geoType:  "geography",
			expected: "binary",
		},
		{
			name:     "unknown extension falls through",
			dt:       newGeoArrowType("some.other.ext", arrow.BinaryTypes.Binary),
			geoType:  "geography",
			expected: "binary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSnowflakeType(tt.dt, tt.geoType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractSRIDFromMeta(t *testing.T) {
	tests := []struct {
		name     string
		metadata string
		expected int
	}{
		{"empty", "", 0},
		{"PROJJSON 4326", `{"crs":{"id":{"authority":"EPSG","code":4326}}}`, 4326},
		{"EPSG string", `{"crs":"EPSG:3857"}`, 3857},
		{"no CRS", `{"edges":"planar"}`, 0},
		{"null CRS", `{"crs":null}`, 0},
		{"invalid JSON", `not json`, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractSRIDFromMeta(tt.metadata))
		})
	}
}
