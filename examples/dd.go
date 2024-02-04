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

package main

import (
	"context"
	"fmt"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	opts := clickhouse.Options{
		Addr: []string{"localhost:9000"},
	}
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		fmt.Println(err)
		return
	}

	var dds []chproto.DD

	rows, err := conn.Query(context.Background(), "SELECT sketch FROM 02919_ddsketch_quantile")
	if err != nil {
		fmt.Println(err)
		return
	}

	for rows.Next() {
		var dd chproto.DD
		err = rows.Scan(&dd)
		if err != nil {
			fmt.Println(err)
			return
		}
		dds = append(dds, dd)
	}

	if rows.Err() != nil {
		fmt.Println(rows.Err())
		return
	}

	for _, dd := range dds {
		fmt.Println(dd.Debug())
	}

	err = rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	// insert the same data back to test the insert
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO 02919_ddsketch_quantile")
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, dd := range dds {
		err = batch.Append(dd)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	err = batch.Send()
	if err != nil {
		fmt.Println(err)
		return
	}
}
