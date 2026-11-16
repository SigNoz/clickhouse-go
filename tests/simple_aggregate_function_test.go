// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tests

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleAggregateFunction(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 21, 1, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_simple_aggregate_function (
			  Col1 UInt64
			, Col2 SimpleAggregateFunction(sum, Double)
			, Col3 SimpleAggregateFunction(sumMap, Tuple(Array(Int16), Array(UInt64)))
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_simple_aggregate_function")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_simple_aggregate_function")
		require.NoError(t, err)
		var (
			col1Data = uint64(42)
			col2Data = float64(256.1)
			col3Data = []any{
				[]int16{1, 2, 3, 4, 5},
				[]uint64{1, 2, 3, 4, 5},
			}
		)
		require.NoError(t, batch.Append(col1Data, col2Data, col3Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var result struct {
			Col1 uint64
			Col2 float64
			Col3 []any
		}
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_simple_aggregate_function").ScanStruct(&result))
		assert.Equal(t, col1Data, result.Col1)
		assert.Equal(t, col2Data, result.Col2)
		assert.Equal(t, col3Data, result.Col3)
	})
}

func TestSimpleAggregateFunctionLowCardinality(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		const ddl = `
		CREATE TABLE test_simple_aggregate_function_lc (
			  Col1 UInt64
			, Col2 SimpleAggregateFunction(anyLast, LowCardinality(String))
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_simple_aggregate_function_lc")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_simple_aggregate_function_lc")
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint64(1), "seconds"))
		require.NoError(t, batch.Append(uint64(2), "bytes"))
		require.NoError(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT Col1, Col2 FROM test_simple_aggregate_function_lc ORDER BY Col1")
		require.NoError(t, err)
		var got []string
		for rows.Next() {
			var (
				col1 uint64
				col2 string
			)
			require.NoError(t, rows.Scan(&col1, &col2))
			got = append(got, col2)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"seconds", "bytes"}, got)
	})
}
