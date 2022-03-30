package bigquery

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/HayoVanLoon/go-proto/transforms"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"strings"
	"testing"
)

func TestSchemaConverter(t *testing.T) {
	valueFieldSchema := []*bigquery.FieldSchema{
		{Name: "null_value", Type: "INTEGER"},
		{Name: "number_value", Type: "FLOAT"},
		{Name: "string_value", Type: "STRING"},
		{Name: "bool_value", Type: "BOOLEAN"},
		{Name: "struct_value", Type: "RECORD", Schema: []*bigquery.FieldSchema{
			{Name: "fields", Type: "RECORD", Repeated: true, Schema: []*bigquery.FieldSchema{
				{Name: "key", Type: "STRING"},
			}},
		}},
		{Name: "list_value", Type: "RECORD"},
	}

	cases := []struct {
		message  string
		options  []transforms.Option
		input    proto.Message
		expected []*bigquery.FieldSchema
	}{
		{
			message: "simple timestamp schema",
			input:   &timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			expected: []*bigquery.FieldSchema{
				{Name: "seconds", Type: "INTEGER"},
				{Name: "nanos", Type: "INTEGER"},
			},
		},
		{
			message:  "simple Value schema",
			options:  []transforms.Option{transforms.OptionMaxDepth(1)},
			input:    &structpb.Value{},
			expected: valueFieldSchema,
		},
		{
			message: "simple Struct schema",
			options: []transforms.Option{transforms.OptionMaxDepth(2)},
			input:   &structpb.Struct{},
			expected: []*bigquery.FieldSchema{
				{Name: "fields", Type: "RECORD", Repeated: true, Schema: []*bigquery.FieldSchema{
					{Name: "key", Type: "STRING"},
					{Name: "value", Type: "RECORD", Schema: valueFieldSchema},
				}},
			},
		},
	}

	for _, c := range cases {
		schemaConverter := NewSchemaConverter(c.options...)
		actual := schemaConverter.Apply(c.input.ProtoReflect().Descriptor())
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s, \nexpected %s, \ngot      %v", c.message, pretty(c.expected), pretty(actual))
		}
	}
}

func TestRowConverter(t *testing.T) {
	cases := []struct {
		options  []transforms.Option
		input    proto.Message
		expected interface{}
		message  string
	}{
		{
			input:    &timestamppb.Timestamp{Seconds: 1, Nanos: 2},
			expected: map[string]interface{}{"seconds": int64(1), "nanos": int64(2)},
			message:  "simple timestamp row",
		},
		{
			input: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": structpb.NewNumberValue(1.2),
					"bar": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
						structpb.NewStringValue("bla"),
						structpb.NewStringValue("bus"),
					}}),
				},
			},
			expected: map[string]interface{}{
				"fields": []map[string]interface{}{
					{
						"key": "bar",
						"value": map[string]interface{}{
							"list_value": map[string]interface{}{
								"values": []interface{}{
									map[string]interface{}{"string_value": "bla"},
									map[string]interface{}{"string_value": "bus"},
								},
							},
						},
					},
					{
						"key":   "foo",
						"value": map[string]interface{}{"number_value": 1.2},
					},
				},
			},
			message: "map field"},
		// TODO(hvl): test (stability of) maps with non-string keys
	}

	for _, c := range cases {
		schemaConverter := NewRowConverter()
		actual := schemaConverter.Apply(c.input)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s, \nexpected %v, \ngot      %v", c.message, c.expected, actual)
		}
	}
}

func pretty(v interface{}) string {
	return pretty2(v, "", ", ", false)
}

func pretty2(v interface{}, indent, fieldBreak string, breakOnBlock bool) string {
	sb := strings.Builder{}
	switch x := v.(type) {
	case bigquery.Schema:
		sb.WriteString(pretty2([]*bigquery.FieldSchema(x), indent, fieldBreak, breakOnBlock))
	case []*bigquery.FieldSchema:
		sb.WriteString("{")
		if breakOnBlock && len(x) > 0 {
			sb.WriteString(fieldBreak)
		}
		for i, f := range x {
			if i > 0 {
				sb.WriteString(fmt.Sprintf(fieldBreak))
			}
			sb.WriteString(pretty2(f, indent+indent, fieldBreak, breakOnBlock))
		}
		if breakOnBlock && len(x) > 0 {
			sb.WriteString(fieldBreak)
		}
		sb.WriteString(fmt.Sprintf("%s}", indent))
	case *bigquery.FieldSchema:
		sb.WriteString(fmt.Sprintf("%s%s: ", indent, x.Name))
		if x.Repeated {
			sb.WriteString(fmt.Sprintf("[]%s", x.Type))
		} else {
			sb.WriteString(fmt.Sprintf("%s", x.Type))
		}
		if x.Type == bigquery.RecordFieldType {
			sb.WriteString(pretty2(x.Schema, indent, fieldBreak, breakOnBlock))
		}
	case []interface{}:
		sb.WriteString("[")
		if breakOnBlock && len(x) > 0 {
			sb.WriteString(fieldBreak)
		}
		for i, x2 := range x {
			if i > 0 {
				sb.WriteString(fmt.Sprintf(fieldBreak))
			}
			sb.WriteString(fmt.Sprintf("%v", x2))
		}
		if breakOnBlock && len(x) > 0 {
			sb.WriteString(fieldBreak)
		}
		sb.WriteString("]")
	case map[string]interface{}:
		var ys []string
		for k, v2 := range x {
			ys = append(ys, fmt.Sprintf("%s: %v", k, v2))
		}
		sb.WriteString(fmt.Sprintf("map{%s}", strings.Join(ys, "")))
	case interface{}:
		sb.WriteString(fmt.Sprintf("%s%v%s", indent, x, fieldBreak))
	}
	return sb.String()
}
