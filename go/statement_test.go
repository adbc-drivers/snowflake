// Copyright (c) 2026 ADBC Drivers Contributors
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

// geoArrowType implements arrow.ExtensionType for testing geoarrow types
// arriving with a registered extension type (e.g. produced in-process by Go).
type geoArrowType struct {
	arrow.ExtensionBase
	name string
	meta string
}

func newGeoArrowType(name string, storage arrow.DataType, meta string) *geoArrowType {
	return &geoArrowType{
		ExtensionBase: arrow.ExtensionBase{Storage: storage},
		name:          name,
		meta:          meta,
	}
}

func (g *geoArrowType) ExtensionName() string { return g.name }
func (g *geoArrowType) Serialize() string     { return g.meta }
func (g *geoArrowType) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	return newGeoArrowType(g.name, storage, data), nil
}
func (g *geoArrowType) ExtensionEquals(other arrow.ExtensionType) bool {
	return g.ExtensionName() == other.ExtensionName()
}
func (g *geoArrowType) ArrayType() reflect.Type {
	return reflect.TypeOf(array.ExtensionArrayBase{})
}

func TestToSnowflakeType(t *testing.T) {
	tests := []struct {
		name     string
		dt       arrow.DataType
		expected string
	}{
		{"binary", arrow.BinaryTypes.Binary, "binary"},
		{"string", arrow.BinaryTypes.String, "text"},
		{"int64", arrow.PrimitiveTypes.Int64, "integer"},
		{"float64", arrow.PrimitiveTypes.Float64, "double"},
		{
			"extension unwraps to storage",
			newGeoArrowType("geoarrow.wkb", arrow.BinaryTypes.Binary, ""),
			"binary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, toSnowflakeType(tt.dt))
		})
	}
}

// TestBuildCopyQueryDetectsGeoViaFieldMetadata exercises the C Data Interface
// scenario: geoarrow columns arrive as plain BINARY/STRING with the
// ARROW:extension:name metadata on the field (the extension type itself isn't
// registered, so f.Type.ID() != arrow.EXTENSION). buildCopyQuery must still
// recognize them, emit a COPY transform with TO_GEOGRAPHY/TO_GEOMETRY, and
// populate the table-creation overrides.
func TestBuildCopyQueryDetectsGeoViaFieldMetadata(t *testing.T) {
	geoMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", ""},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geoMeta},
	}, nil)

	st := &statement{ingestOptions: DefaultIngestOptions()}
	copyQ, overrides := st.buildCopyQuery(schema)

	assert.Equal(t, "geography", overrides["geom"], "geo column must be created as GEOGRAPHY")
	assert.NotEqual(t, copyQuery, copyQ, "must not fall back to plain copyQuery when geo cols are present")
	assert.Contains(t, copyQ, "TO_GEOGRAPHY", "COPY transform must include TO_GEOGRAPHY for WKB geography column")
	assert.Contains(t, copyQ, `"geom"`, "geo column must be referenced with Snowflake-quoted identifier")
}

// TestBuildCopyQueryGeometryWithSRID covers the GEOMETRY path: when the
// extension metadata declares a non-4326 CRS, the column is promoted to
// GEOMETRY and the SRID is applied via ST_SETSRID.
func TestBuildCopyQueryGeometryWithSRID(t *testing.T) {
	geoMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", `{"crs":"EPSG:3857"}`},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geoMeta},
	}, nil)

	st := &statement{ingestOptions: DefaultIngestOptions()}
	copyQ, overrides := st.buildCopyQuery(schema)

	assert.Equal(t, "geometry", overrides["geom"])
	assert.Contains(t, copyQ, "ST_SETSRID(TO_GEOMETRY")
	assert.Contains(t, copyQ, "3857")
}

// TestBuildCopyQueryExplicitGeoTypeOverrides covers the explicit-pin path:
// when the user sets OptionStatementIngestGeoType="geometry", every geoarrow
// column is created as GEOMETRY regardless of its CRS metadata. This was
// previously covered by a toSnowflakeType unit test that is no longer
// applicable after the geoarrow branch was removed from toSnowflakeType.
func TestBuildCopyQueryExplicitGeoTypeOverrides(t *testing.T) {
	geoMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", ""}, // no CRS → would default to geography
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geoMeta},
	}, nil)

	opts := DefaultIngestOptions()
	opts.geoType = "geometry"
	opts.geoTypeExplicit = true
	st := &statement{ingestOptions: opts}
	copyQ, overrides := st.buildCopyQuery(schema)

	assert.Equal(t, "geometry", overrides["geom"], "explicit geoType must override CRS-derived default")
	assert.Contains(t, copyQ, "TO_GEOMETRY")
	assert.NotContains(t, copyQ, "TO_GEOGRAPHY")
}

// TestBuildCopyQueryQuotesExoticColumnNames locks in the Snowflake-style
// identifier quoting for column names containing characters that Go's %q
// would mishandle (embedded double-quote, backslash).
func TestBuildCopyQueryQuotesExoticColumnNames(t *testing.T) {
	geoMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name"},
		[]string{"geoarrow.wkb"},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: `weird"geom`, Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geoMeta},
	}, nil)

	st := &statement{ingestOptions: DefaultIngestOptions()}
	copyQ, _ := st.buildCopyQuery(schema)

	// Snowflake escapes embedded " by doubling: weird"geom → "weird""geom"
	assert.Contains(t, copyQ, `"weird""geom"`, "column with embedded quote must use Snowflake doubled-quote escaping")
	assert.NotContains(t, copyQ, `"weird\"geom"`, "must not use Go-style backslash escaping (Snowflake parser would reject)")
}

// TestBuildCopyQueryNoGeoColumnsReturnsPlainCopy ensures schemas without any
// geoarrow columns fall back to the plain copy query and produce no overrides.
func TestBuildCopyQueryNoGeoColumnsReturnsPlainCopy(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	st := &statement{ingestOptions: DefaultIngestOptions()}
	copyQ, overrides := st.buildCopyQuery(schema)

	assert.Empty(t, overrides)
	assert.Equal(t, copyQuery, copyQ, "non-geo schemas must use the plain copyQuery constant verbatim")
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
