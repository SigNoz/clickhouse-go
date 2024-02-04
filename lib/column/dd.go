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

package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"
)

type AggregateFunctionDD struct {
	name     string
	typeName string
	col      proto.AggregateFunctionDD
}

func (col *AggregateFunctionDD) Reset() {
	col.col.Reset()
}

func (col AggregateFunctionDD) Name() string {
	return col.name
}

func (col *AggregateFunctionDD) Rows() int {
	return col.col.Rows()
}

func (col *AggregateFunctionDD) Row(i int, ptr bool) any {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *AggregateFunctionDD) row(i int) proto.DD {
	return col.col.Row(i)
}

func (col *AggregateFunctionDD) Type() Type {
	return Type(col.typeName)
}

func (col *AggregateFunctionDD) ScanType() reflect.Type {
	return scanTypeDD
}

func (col *AggregateFunctionDD) ScanRow(dest any, row int) error {
	switch d := dest.(type) {
	case *proto.DD:
		*d = col.row(row)
	case **proto.DD:
		*d = new(proto.DD)
		**d = col.row(row)
	default:
		return fmt.Errorf("can't scan %T with %T", col, dest)
	}
	return nil
}

func (col *AggregateFunctionDD) Append(v any) (nulls []uint8, err error) {
	switch value := v.(type) {
	case []proto.DD:
		col.col.AppendArr(value)
		return nil, nil
	case []*proto.DD:
		for _, v := range value {
			col.col.Append(*v)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("can't append %T to %T", v, col)
	}
}

func (col *AggregateFunctionDD) AppendRow(v any) error {
	switch value := v.(type) {
	case proto.DD:
		col.col.Append(value)
		return nil
	case *proto.DD:
		col.col.Append(*value)
		return nil
	default:
		return fmt.Errorf("can't append %T to %T", v, col)
	}
}

func (col *AggregateFunctionDD) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *AggregateFunctionDD) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

var _ Interface = (*AggregateFunctionDD)(nil)
