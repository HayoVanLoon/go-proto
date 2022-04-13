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
	"google.golang.org/protobuf/types/known/typepb"
	"reflect"
	"strconv"
	"strings"
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
			NewWalker(),
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"nanos": int32(2)},
			"drop empty scalar",
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
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("expected %v, \ngot      %v", c.expected, actual)
			}
		})
	}
}

func TestOptionKeepOrder(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(OptionKeepOrder(true)),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			[]KeyValue{{"seconds", int64(1)}, {"nanos", int32(2)}},
			"happy keep order",
		},
		{
			NewWalker(OptionKeepOrder(true)),
			&apipb.Method{
				Name: "foo", ResponseStreaming: true, RequestStreaming: true,
				ResponseTypeUrl: "bar", RequestTypeUrl: "bla",
			},
			[]KeyValue{
				{"name", "foo"},
				{"request_type_url", "bla"},
				{"request_streaming", true},
				{"response_type_url", "bar"},
				{"response_streaming", true},
			},
			"happy keep order",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// run many times to decrease chance of random implementations succeeding.
			for i := 0; i < 100; i += 1 {
				if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
					t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
				}
			}
		})
	}
}

func TestOptionKeepOrder_Descriptor(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(OptionKeepOrder(true)),
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			[]KeyValue{{"seconds", nil}, {"nanos", nil}},
			"happy descriptor with keep order",
		},
	}
	// run many times to decrease chance of random implementations succeeding.
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for i := 0; i < 100; i += 1 {
				if actual := c.walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
					t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
				}
			}
		})
	}
}

func TestOptionAddTypeOverride(t *testing.T) {
	inpFunc := func(fd protoreflect.FieldDescriptor, kvs []KeyValue) interface{} {
		// only return its name
		for _, kv := range kvs {
			if kv.Key == "name" {
				return kv.Value
			}
		}
		return nil
	}

	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(OptionAddTypeOverride("google.protobuf.Method", inpFunc)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
			map[string]interface{}{
				"name":    "foo",
				"methods": []interface{}{"foo_method", "bar_method"},
			},
			"message override by type",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func TestOptionAddNameOverride(t *testing.T) {
	inpFunc := func(fd protoreflect.FieldDescriptor, kvs []KeyValue) interface{} {
		// only return its name
		for _, kv := range kvs {
			if kv.Key == "name" {
				return kv.Value
			}
		}
		return nil
	}

	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(
				OptionAddNameOverride("seconds", func(_ protoreflect.FieldDescriptor, value *protoreflect.Value) interface{} {
					return int(value.Int() % 3)
				}),
			),
			&timestamppb.Timestamp{Seconds: 4, Nanos: 2},
			map[string]interface{}{"seconds": 1, "nanos": int32(2)},
			"simple override by name",
		},
		{
			NewWalker(OptionAddNameOverride("methods", inpFunc)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
			map[string]interface{}{
				"name":    "foo",
				"methods": []interface{}{"foo_method", "bar_method"},
			},
			"message override by name",
		},
		{
			NewWalker(OptionAddNameOverride(
				"methods",
				func(_ protoreflect.FieldDescriptor, kvs []KeyValue) interface{} {
					m := make(map[string]interface{})
					for _, kv := range kvs {
						if k := kv.Key; k == "name" {
							switch v := kv.Value.(type) {
							case string:
								m[k] = strings.ToUpper(v)
							}
						} else {
							m[k] = kv.Value
						}
					}
					return m
				},
			)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "FOO_METHOD", "request_streaming": true,
					},
					map[string]interface{}{
						"name": "BAR_METHOD",
					},
				},
			},
			"message with children override by name",
		},
		{
			NewWalker(OptionAddNameOverride(
				"methods.request_streaming",
				func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
					return 42
				},
			)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method", "request_streaming": 42,
					},
					map[string]interface{}{
						"name": "bar_method",
					},
				},
			},
			"scalar override by name",
		},
		{
			NewWalker(OptionAddNameOverride(
				"methods.name",
				func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
					return strings.ToUpper(v.String())
				},
			)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "FOO_METHOD", "request_streaming": true,
					},
					map[string]interface{}{
						"name": "BAR_METHOD",
					},
				},
			},
			"scalar override by name",
		},
		{
			NewWalker(OptionAddNameOverride(
				"fields",
				func(_ protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
					return len(m)
				},
			)),
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
				"fields": 2,
			},
			"override map",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func BenchmarkOptionAddNameOverride(b *testing.B) {
	cases := []struct {
		walker Walker
		input  proto.Message
	}{
		{
			NewWalker(OptionAddNameOverride(
				"methods.name",
				func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
					return strings.ToUpper(v.String())
				},
			)),
			&apipb.Api{
				Name: "foo",
				Methods: []*apipb.Method{
					{Name: "foo_method", RequestStreaming: true},
					{Name: "bar_method"},
				},
			},
		},
		{
			NewWalker(OptionAddNameOverride(
				"fields",
				func(_ protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
					return len(m)
				},
			)),
			&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": structpb.NewNumberValue(1.2),
					"bar": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
						structpb.NewStringValue("bla"),
						structpb.NewStringValue("bus"),
					}}),
				},
			},
		},
	}
	//	b.Run("test", func(b *testing.B) {
	for i := 0; i < b.N; i += 1 {
		for _, c := range cases {
			c.walker.Apply(c.input)
		}
	}
	//	})
}

func TestOptionAddScalarFunc(t *testing.T) {
	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
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
			"type overrides for int64 and int32",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func TestOptionKeepEmpty(t *testing.T) {
	walker := NewWalker(OptionKeepEmpty(true))

	cases := []struct {
		input    proto.Message
		expected map[string]interface{}
		name     string
	}{
		{
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"nanos": int32(2), "seconds": int64(0)},
			"keep empty: empty seconds",
		},
		{
			&timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			map[string]interface{}{"seconds": int64(1), "nanos": int32(2)},
			"keep empty: all filled",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: expected %v, got %v", c.name, c.expected, actual)
			}
		})
	}
}

func TestOptionKeepEmpty_Descriptor(t *testing.T) {
	walker := NewWalker(OptionKeepEmpty(true))

	cases := []struct {
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			&timestamppb.Timestamp{Seconds: 0, Nanos: 2},
			map[string]interface{}{"seconds": nil, "nanos": nil},
			"descriptor should not care about keepEmpty",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func TestWalker_Descriptor(t *testing.T) {
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
		// TODO(hvl): search for (non-recursive) message type with map
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func TestOptionMaxDepth(t *testing.T) {
	apiInput := &apipb.Api{
		Name: "foo",
		Methods: []*apipb.Method{
			{
				Name: "foo_method",
				Options: []*typepb.Option{
					{Name: "foo_opt"},
				},
			},
		},
	}

	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(OptionMaxDepth(defaultMaxRecurse)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method",
						"options": []interface{}{
							map[string]interface{}{"name": "foo_opt"},
						},
					},
				},
			},
			"default depth",
		},
		{
			NewWalker(OptionMaxDepth(2)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method",
						"options": []interface{}{
							map[string]interface{}{"name": "foo_opt"},
						},
					},
				},
			},
			"2",
		},
		{
			NewWalker(OptionMaxDepth(1)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method",
					},
				},
			},
			"1",
		},
		{
			NewWalker(OptionMaxDepth(0)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
			},
			"0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				switch x := c.expected.(type) {
				case map[string]interface{}:
					switch y := actual.(type) {
					case map[string]interface{}:
						t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, x, y)
					}
				}
			}
		})
	}
}

func TestOptionMaxDepth_Descriptor(t *testing.T) {
	cases := []struct {
		depth    int
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			-1,
			&apipb.Api{},
			map[string]interface{}{
				"name":           nil,
				"methods":        nil,
				"mixins":         nil,
				"options":        nil,
				"source_context": map[string]interface{}{"file_name": nil},
				"syntax":         nil,
				"version":        nil,
			},
			"use default with non-recursive message",
		},
		{
			0,
			&apipb.Api{},
			map[string]interface{}{
				"name": nil, "methods": nil, "mixins": nil, "options": nil,
				"source_context": nil, "syntax": nil, "version": nil,
			},
			"use 0 with non-recursive message",
		},
		{
			0,
			&structpb.Value{},
			map[string]interface{}{
				"null_value": nil, "string_value": nil, "number_value": nil,
				"bool_value": nil, "list_value": nil, "struct_value": nil,
			},
			"depth 0, with oneof field",
		},
		{
			1,
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
			"depth 1, with oneof field",
		},
		{
			2,
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
			"depth 2, with oneof field",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			walker := NewWalker(OptionMaxDepth(c.depth))
			if actual := walker.ApplyDesc(c.input.ProtoReflect().Descriptor()); !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, c.expected, actual)
			}
		})
	}
}

func BenchmarkOptionMaxDepth_Descriptor(b *testing.B) {
	cases := []struct {
		walker Walker
		input  proto.Message
	}{
		{
			NewWalker(OptionMaxDepth(-1)),
			&apipb.Api{},
		},
		{
			NewWalker(OptionMaxDepth(0)),
			&apipb.Api{},
		},
		{
			NewWalker(OptionMaxDepth(0)),
			&structpb.Value{},
		},
		{
			NewWalker(OptionMaxDepth(1)),
			&structpb.Value{},
		},
		{
			NewWalker(OptionMaxDepth(2)),
			&structpb.Struct{},
		},
	}
	for i := 0; i < b.N; i += 1 {
		for _, c := range cases {
			c.walker.ApplyDesc(c.input.ProtoReflect().Descriptor())
		}
	}
}

func TestOptionMaxDepthForName(t *testing.T) {
	apiInput := &apipb.Api{
		Name: "foo",
		Methods: []*apipb.Method{
			{
				Name: "foo_method",
				Options: []*typepb.Option{
					{Name: "foo_opt"},
				},
			},
		},
	}

	cases := []struct {
		walker   Walker
		input    proto.Message
		expected interface{}
		name     string
	}{
		{
			NewWalker(OptionMaxDepthForName("methods", 0)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method",
					},
				},
			},
			"depth 0 for name",
		},
		{
			// TODO(hvl): use deeper type; this only tests 'maxDepth > 0', not 'maxDepth < 2'
			NewWalker(OptionMaxDepthForName("methods", 1)),
			apiInput,
			map[string]interface{}{
				"name": "foo",
				"methods": []interface{}{
					map[string]interface{}{
						"name": "foo_method",
						"options": []interface{}{
							map[string]interface{}{"name": "foo_opt"},
						},
					},
				},
			},
			"depth 1 for name",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if actual := c.walker.Apply(c.input); !reflect.DeepEqual(actual, c.expected) {
				switch x := c.expected.(type) {
				case map[string]interface{}:
					switch y := actual.(type) {
					case map[string]interface{}:
						t.Errorf("%s: \nexpected %v, \ngot      %v", c.name, x, y)
					}
				}
			}
		})
	}
}
