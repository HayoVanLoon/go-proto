package bigquery

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/HayoVanLoon/go-proto/transforms"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

var timestampDescriptor = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()

func GetBigQueryType(fd protoreflect.FieldDescriptor) bigquery.FieldType {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return bigquery.BooleanFieldType
	case protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return bigquery.IntegerFieldType
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return bigquery.FloatFieldType
	case protoreflect.StringKind:
		return bigquery.StringFieldType
	case protoreflect.BytesKind:
		return bigquery.BytesFieldType
	default:
		panic(fmt.Sprintf("unsupported type %v", fd.Kind()))
	}
}

type SchemaConverter interface {
	Apply(descriptor protoreflect.MessageDescriptor) []*bigquery.FieldSchema
}

type schemaConverter struct {
	walker transforms.Walker
}

func convertSchemaScalar(fd protoreflect.FieldDescriptor, _ *protoreflect.Value) interface{} {
	name := string(fd.Name())
	typ := GetBigQueryType(fd)
	return &bigquery.FieldSchema{Name: name, Type: typ}
}

func convertSchemaMessage(fd protoreflect.FieldDescriptor, kvs []transforms.KeyValue) interface{} {
	var fs []*bigquery.FieldSchema
	for _, v := range kvs {
		switch x := v.Value.(type) {
		case *bigquery.FieldSchema:
			fs = append(fs, x)
		}
	}
	return &bigquery.FieldSchema{
		Name:   string(fd.Name()),
		Type:   bigquery.RecordFieldType,
		Schema: fs,
	}
}

func convertSchemaMap(fd protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
	// cast and impose order on key and value fields
	casted := map[string]*bigquery.FieldSchema{}
	for _, v := range m {
		switch x := v.(type) {
		case *bigquery.FieldSchema:
			casted[x.Name] = x
		case nil:
		default:
			panic(fmt.Sprintf("expected *bigquery.FieldSchema, got %T", x))
		}
	}
	var fs []*bigquery.FieldSchema
	if v := casted["key"]; v != nil {
		fs = append(fs, v)
	}
	if v := casted["value"]; v != nil {
		fs = append(fs, v)
	}
	return &bigquery.FieldSchema{
		Name:     string(fd.Name()),
		Type:     bigquery.RecordFieldType,
		Repeated: true,
		Schema:   fs,
	}
}

func convertSchemaTimestamp(fd protoreflect.FieldDescriptor, _ []transforms.KeyValue) interface{} {
	return &bigquery.FieldSchema{
		Name: string(fd.Name()),
		Type: bigquery.TimestampFieldType,
	}
}

func NewSchemaConverter(options ...transforms.Option) SchemaConverter {
	basicOptions := []transforms.Option{
		transforms.OptionDefaultScalarFunc(convertSchemaScalar),
		transforms.OptionMessageFunc(convertSchemaMessage),
		transforms.OptionMapFunc(convertSchemaMap),
		transforms.OptionKeepEmpty(true),
		transforms.OptionKeepOrder(true),
		transforms.OptionAddOverride(string(timestampDescriptor.FullName()), convertSchemaTimestamp),
	}
	cs := transforms.NewWalker(append(basicOptions, options...)...)
	return &schemaConverter{walker: cs}
}

func (sc *schemaConverter) Apply(md protoreflect.MessageDescriptor) []*bigquery.FieldSchema {
	out := sc.walker.ApplyDesc(md)
	var fs []*bigquery.FieldSchema
	switch x := out.(type) {
	case []transforms.KeyValue:
		for _, v := range x {
			switch y := v.Value.(type) {
			case *bigquery.FieldSchema:
				fs = append(fs, y)
			}
		}
	default:
		panic(fmt.Sprintf("expected *bigquery.FieldSchema, got %T", x))
	}
	return fs
}

type RowConverter interface {
	Apply(proto.Message) map[string]interface{}
}

type rowConverter struct {
	walker transforms.Walker
}

func (rc *rowConverter) Apply(m proto.Message) map[string]interface{} {
	switch x := rc.walker.Apply(m).(type) {
	case map[string]interface{}:
		return x
	default:
		panic(fmt.Sprintf("expected map[string]interface{}, got %T", x))
	}
}

func convertRowScalar(fd protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
	if v == nil {
		return nil
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.EnumKind:
		return v.Enum()
	case protoreflect.Int32Kind:
		return v.Int()
	case protoreflect.Sint32Kind:
		return v.Int()
	case protoreflect.Uint32Kind:
		return v.Int()
	case protoreflect.Int64Kind:
		return v.Int()
	case protoreflect.Sint64Kind:
		return v.Int()
	case protoreflect.Uint64Kind:
		return v.Int()
	case protoreflect.Sfixed32Kind:
		return v.Int()
	case protoreflect.Fixed32Kind:
		return v.Int()
	case protoreflect.FloatKind:
		return v.Float()
	case protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Fixed64Kind:
		return v.Int()
	case protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return v.Bytes()
	default:
		return nil
	}
}

func convertRowMapFunc(fd protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
	var keys []interface{}
	for k := range m {
		keys = append(keys, k)
	}
	sortMapKeys(fd, keys)
	var kvs []map[string]interface{}
	for _, k := range keys {
		kvs = append(kvs, map[string]interface{}{
			"key":   k,
			"value": m[k],
		})
	}
	return kvs
}

func convertRowTimestamp(_ protoreflect.FieldDescriptor, kvs []transforms.KeyValue) interface{} {
	if len(kvs) == 0 {
		return nil
	}
	seconds := int64(0)
	nanos := int64(0)
	for _, kv := range kvs {
		if kv.Key == "seconds" {
			switch x := kv.Value.(type) {
			case protoreflect.Value:
				seconds = x.Int()
			}
		} else {
			switch x := kv.Value.(type) {
			case protoreflect.Value:
				nanos = x.Int()
			}
		}
	}
	return time.Unix(seconds, nanos)
}

func NewRowConverter(options ...transforms.Option) RowConverter {
	basicOptions := []transforms.Option{
		transforms.OptionDefaultScalarFunc(convertRowScalar),
		transforms.OptionMapFunc(convertRowMapFunc),
		transforms.OptionAddOverride(string(timestampDescriptor.FullName()), convertRowTimestamp),
	}
	cs := transforms.NewWalker(append(basicOptions, options...)...)
	return &rowConverter{walker: cs}
}
