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
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type MetadataCollection interface {
	Name() string
	Init() MetadataCollectionQuery
}

type Filter interface {
	MatchesString(value string) bool
}

type FilterExact struct {
	value string
}

func (f *FilterExact) MatchesString(value string) bool {
	return f.value == value
}

type FilterPattern struct {
	// TODO: compile to regexp
	pattern string
}

func (f *FilterPattern) MatchesString(value string) bool {
	panic("not implemented")
}

type MetadataCollectionQuery interface {
	AddFilter(name string, filter Filter) error
}

type MetadataCollectionState struct {
	collection           MetadataCollectionQuery
	supportedCollections map[string]MetadataCollection
}

func (mcs *MetadataCollectionState) AddCollection(collection MetadataCollection) {
	if mcs.supportedCollections == nil {
		mcs.supportedCollections = make(map[string]MetadataCollection)
	}
	mcs.supportedCollections[collection.Name()] = collection
}

func (mcs *MetadataCollectionState) SetOption(name, value string) (bool, error) {
	switch {
	case name == "adbc.metadata.collection":
		collection, ok := mcs.supportedCollections[value]
		if !ok {
			return true, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[snowflake] unknown collection adbc.metadata.collection=%s", value),
			}
		}
		mcs.collection = collection.Init()
		return true, nil
	case strings.HasPrefix(name, "adbc.metadata.filter_pattern."):
		if mcs.collection == nil {
			// TODO: need errorhelper, constant
			return true, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[snowflake] filter %s specified before collection", name),
			}
		} else {
			filterName := strings.TrimPrefix(name, "adbc.metadata.filter_pattern.")
			filter := &FilterPattern{pattern: value}
			return true, mcs.collection.AddFilter(filterName, filter)
		}
	case strings.HasPrefix(name, "adbc.metadata.filter."):
		if mcs.collection == nil {
			return true, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[snowflake] filter %s specified before collection", name),
			}
		} else {
			filterName := strings.TrimPrefix(name, "adbc.metadata.filter.")
			filter := &FilterExact{value: value}
			return true, mcs.collection.AddFilter(filterName, filter)
		}
	}
	return false, nil
}

func (mcs *MetadataCollectionState) Clear() {
	mcs.collection = nil
}

func (mcs *MetadataCollectionState) Take() MetadataCollectionQuery {
	collection := mcs.collection
	mcs.collection = nil
	return collection
}

type snowflakeMetadataCollection interface {
	MetadataCollectionQuery
	Execute(ctx context.Context, mem memory.Allocator, conn driver.QueryerContext) (array.RecordReader, error)
}

type catalogCollection struct{}

func (c *catalogCollection) Name() string                  { return "catalogs" }
func (c *catalogCollection) Init() MetadataCollectionQuery { return &catalogCollectionQuery{} }

type catalogCollectionQuery struct {
	catalog Filter
}

func (cq *catalogCollectionQuery) AddFilter(name string, filter Filter) error {
	if name == "catalog" {
		cq.catalog = filter
	}
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[snowflake] unsupported filter %s for metadata collection 'catalogs'", name),
	}
}
func (cq *catalogCollectionQuery) Execute(ctx context.Context, mem memory.Allocator, conn driver.QueryerContext) (array.RecordReader, error) {
	var query strings.Builder
	query.WriteString("SHOW DATABASES")
	// TODO:
	// switch f := cq.catalog.(type) {
	// case *FilterExact:
	// case *FilterPattern:
	// }

	rows, err := conn.QueryContext(ctx, query.String(), nil)
	if err != nil {
		// TODO: need variant that provides context
		// TODO: fold this into ErrorHelper
		return nil, errToAdbcErr(adbc.StatusUnknown, err)
	}
	// TODO: merge error
	defer rows.Close()
	// TODO: can we get direct Arrow data?
	dest := make([]driver.Value, len(rows.Columns()))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "catalog_remarks", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	catalogName := builder.Field(0).(*array.StringBuilder)
	catalogRemarks := builder.Field(1).(*array.StringBuilder)
	for {
		if err := rows.Next(dest); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, errToAdbcErr(adbc.StatusUnknown, err)
		}
		catalogName.Append(dest[1].(string))
		comment := dest[6]
		if comment == nil {
			catalogRemarks.AppendNull()
		} else {
			catalogRemarks.Append(comment.(string))
		}
	}

	batch := builder.NewRecordBatch()
	return array.NewRecordReader(schema, []arrow.RecordBatch{batch})
}
