// Copyright 2022 Hayo van Loon. All rights reserved.
// Use of this source code is governed by a licence
// that can be found in the LICENSE file.
package transforms

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"strconv"
	"testing"
)

func TestWalker(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": int32(2)},
			"happy timestamp",
		},
		{
			NewWalker(OptionKeepOrder(true)),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			[]KeyValue{{"seconds", int64(1)}, {"nanos", int32(2)}},
			"happy keep order",
		},
		{
			NewWalker(
				OptionAddScalarFunc(protoreflect.Int64Kind, func(_ protoreflect.FieldDescriptor, value *protoreflect.Value) interface{} {
					return value.Int() % 3
				}),
				OptionAddScalarFunc(protoreflect.Int32Kind, func(_ protoreflect.FieldDescriptor, value *protoreflect.Value) interface{} {
					return strconv.Itoa(int(value.Int()))
				}),
			),
			&timestamppb.Timestamp{Seconds: 4, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": "2"},
			"happy scalar override for int64 and int32",
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
					{Name: "bar_method", RequestStreaming: false},
				},
			},
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method", "request_streaming": true,
					},
					map[string]interface{}{
						"name": "bar_method",
					}},
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
				"fields": map[interface{}]interface{}{
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
			NewWalker(OptionKeepEmpty(true)),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": int32(2)},
			"happy timestamp",
		},
		{
			NewWalker(OptionKeepEmpty(true)),
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
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": nil, "nanos": nil},
			"happy descriptor",
		},
		{
			NewWalker(OptionKeepOrder(true)),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			[]KeyValue{{"seconds", nil}, {"nanos", nil}},
			"happy descriptor with keep order",
		},
		{
			NewWalker(OptionKeepEmpty(true)),
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"seconds": nil, "nanos": nil},
			"descriptor should not care about keepEmpty",
		},
		{
			NewWalker(),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method", RequestStreaming: false},
				},
			},
			map[string]interface{}{
				"name":    nil,
				"methods": nil,
				"mixins":  nil,
				"options": nil,
				"source_context": map[string]interface{}{
					"file_name": nil,
				},
				"syntax":  nil,
				"version": nil,
			},
			"nested repeated",
		},
		{
			NewWalker(OptionMaxDepth(1)),
			&structpb.Value{},
			map[string]interface{}{
				"null_value":   nil,
				"string_value": nil,
				"number_value": nil,
				"bool_value":   nil,
				"list_value": map[string]interface{}{
					"values": nil,
				},
				"struct_value": map[string]interface{}{
					"fields": map[interface{}]interface{}{
						"key":   nil,
						"value": nil,
					},
				},
			},
			"oneof",
		},
		{
			NewWalker(OptionMaxDepth(2)),
			&structpb.Struct{},
			map[string]interface{}{
				"fields": map[interface{}]interface{}{
					"key": nil,
					"value": map[string]interface{}{
						"null_value":   nil,
						"string_value": nil,
						"number_value": nil,
						"bool_value":   nil,
						"list_value": map[string]interface{}{
							"values": nil,
						},
						"struct_value": map[string]interface{}{
							"fields": map[interface{}]interface{}{
								"key":   nil,
								"value": nil,
							},
						},
					},
				},
			},
			"map field",
		},
	}
	for _, c := range cases {
		if actual := c.walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
		}
	}
}
