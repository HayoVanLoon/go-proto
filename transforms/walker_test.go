// Copyright 2022 Hayo van Loon. All rights reserved.
// Use of this source code is governed by a licence
// that can be found in the LICENSE file.
package transforms

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"testing"
)

func TestWalkerTimestamp(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected map[string]interface{}
		name     string
	}{
		{
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": int32(2)},
			"happy timestamp",
		},
		{
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"nanos": int32(2)},
			"zero seconds timestamp",
		},
		{
			NewWalker(),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
				},
			},
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method", "request_streaming": true,
					},
				},
			},
			"nested repeated",
		},
		{
			NewWalker(),
			&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": structpb.NewNumberValue(1.2),
					"bar": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
						structpb.NewStringValue("bla"),
						structpb.NewStringValue("bus"),
					}}),
				},
			},
			map[string]interface{}{
				"fields": map[string]interface{}{
					"foo": map[string]interface{}{"number_value": 1.2},
					"bar": map[string]interface{}{
						"list_value": map[string]interface{}{
							"values": []interface{}{
								map[string]interface{}{"string_value": "bla"},
								map[string]interface{}{"string_value": "bus"},
							},
						},
					},
				},
			},
			"map field",
		},
	}
	for _, c := range cases {
		if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
		}
	}
}

func TestWalkerKeepEmpty(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected map[string]interface{}
		name     string
	}{
		{
			NewWalkerKeepEmpty(),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": int32(2)},
			"happy timestamp",
		},
		{
			NewWalkerKeepEmpty(),
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"nanos": int32(2), "seconds": int64(0)},
			"zero seconds timestamp",
		},
	}
	for _, c := range cases {
		if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
		}
	}
}

func TestWalkerDescriptor(t *testing.T) {
	w := NewWalker()
	x := w.ApplyDesc((&timestamppb.Timestamp{}).ProtoReflect().Descriptor())
	print(x)

	cases := []struct {
		walker   Walker
		input    proto.Message
		expected map[string]interface{}
		name     string
	}{
		{
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": nil, "nanos": nil},
			"happy descriptor",
		},
		{
			NewWalkerKeepEmpty(),
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"seconds": nil, "nanos": nil},
			"descriptor should not care about keepEmpty",
		},
	}
	for _, c := range cases {
		if actual := c.walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
		}
	}
}
