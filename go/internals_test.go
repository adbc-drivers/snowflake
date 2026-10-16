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
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetOptionInternal_MaxRetryCount(t *testing.T) {
	db := &databaseImpl{cfg: &gosnowflake.Config{}}
	ctx := context.Background()

	// unset: 0 means "use the gosnowflake default"
	v, err := db.GetOption(ctx, OptionMaxRetryCount)
	require.NoError(t, err)
	require.Equal(t, "0", v)

	require.NoError(t, db.SetOptionInternal(OptionMaxRetryCount, "50", nil))
	require.Equal(t, 50, db.cfg.MaxRetryCount)
	v, err = db.GetOption(ctx, OptionMaxRetryCount)
	require.NoError(t, err)
	require.Equal(t, "50", v)

	for _, bad := range []string{"-1", "abc", "1.5", ""} {
		err := db.SetOptionInternal(OptionMaxRetryCount, bad, nil)
		require.Error(t, err, "value %q", bad)
		require.Equal(t, 50, db.cfg.MaxRetryCount, "invalid value %q must not change the setting", bad)
	}
}

func TestConnectorConfigDoesNotMutateDatabase(t *testing.T) {
	db := &databaseImpl{
		cfg:                   &gosnowflake.Config{Params: map[string]*string{}},
		geographyOutputFormat: "GEOJSON",
		geometryOutputFormat:  "GEOJSON",
	}

	cfg := db.connectorConfig()
	require.NotNil(t, cfg.Params["GEOGRAPHY_OUTPUT_FORMAT"])
	assert.Equal(t, "GEOJSON", *cfg.Params["GEOGRAPHY_OUTPUT_FORMAT"])
	assert.Equal(t, "GEOJSON", *cfg.Params["GEOMETRY_OUTPUT_FORMAT"])
	assert.Empty(t, db.cfg.Params, "per-connection defaults leaked into the database config")

	// Changing the option after a connection was opened applies to the next one.
	require.NoError(t, db.SetOptionInternal(OptionGeographyOutputFormat, "EWKB", nil))
	cfg = db.connectorConfig()
	assert.Equal(t, "EWKB", *cfg.Params["GEOGRAPHY_OUTPUT_FORMAT"])
}

func TestConnectorConfigKeepsExplicitParams(t *testing.T) {
	db := &databaseImpl{
		cfg:                   &gosnowflake.Config{Params: map[string]*string{}},
		geographyOutputFormat: "GEOJSON",
		geometryOutputFormat:  "GEOJSON",
	}
	require.NoError(t, db.SetOptionInternal("GEOMETRY_OUTPUT_FORMAT", "WKT", nil))

	cfg := db.connectorConfig()
	assert.Equal(t, "WKT", *cfg.Params["GEOMETRY_OUTPUT_FORMAT"])
	assert.Equal(t, "GEOJSON", *cfg.Params["GEOGRAPHY_OUTPUT_FORMAT"])
	assert.Len(t, db.cfg.Params, 1)
}
