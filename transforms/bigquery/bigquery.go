package bigquery

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/HayoVanLoon/go-proto/transforms"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sort"
	"time"
)

var ts = func() protoreflect.MessageDescriptor {
	t := timestamppb.Timestamp{}
	return t.ProtoReflect().Descriptor()
}()

func GetBigQueryType(fd protoreflect.FieldDescriptor) bigquery.FieldType {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return bigquery.BooleanFieldType
	case protoreflect.Int32Kind:
	case protoreflect.Int64Kind:
	case protoreflect.Sint32Kind:
	case protoreflect.Sint64Kind:
		return bigquery.StringFieldType
	case protoreflect.StringKind:
		return bigquery.StringFieldType
	case protoreflect.MessageKind:
		return bigquery.RecordFieldType
	}
	panic(fmt.Sprintf("unsupported type %v", fd.Kind()))
}

type SchemaConverter interface {
	Apply(descriptor protoreflect.MessageDescriptor) []*bigquery.FieldSchema
}

type schemaConverter struct {
	walker transforms.Walker
}

func convertSchemaScalar(fd protoreflect.FieldDescriptor, _ *protoreflect.Value) interface{} {
	name := string(fd.Name())
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.BooleanFieldType}
	case protoreflect.EnumKind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	case protoreflect.Int32Kind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	case protoreflect.Int64Kind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	case protoreflect.Sint32Kind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	case protoreflect.Sint64Kind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	case protoreflect.FloatKind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.FloatFieldType}
	case protoreflect.DoubleKind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.FloatFieldType}
	case protoreflect.StringKind:
		return &bigquery.FieldSchema{Name: string(fd.Name()), Type: bigquery.StringFieldType}
	case protoreflect.BytesKind:
		return &bigquery.FieldSchema{Name: name, Type: bigquery.BytesFieldType}
	default:
		return nil
	}
}

func convertSchemaMessage(fd protoreflect.FieldDescriptor, m map[string]interface{}) interface{} {
	// hvl: order by field numbers (most stable characteristic)
	var xs []struct {
		int
		*bigquery.FieldSchema
	}
	fds := fd.Message().Fields()
	for k, v := range m {
		fd := fds.ByName(protoreflect.Name(k))
		switch x := v.(type) {
		case *bigquery.FieldSchema:
			xs = append(xs, struct {
				int
				*bigquery.FieldSchema
			}{int(fd.Number()), x})
		}
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].int < xs[j].int })

	schema := make([]*bigquery.FieldSchema, len(xs))
	for i, v := range xs {
		schema[i] = v.FieldSchema
	}
	return &bigquery.FieldSchema{
		Name:   string(fd.Name()),
		Type:   bigquery.RecordFieldType,
		Schema: schema,
	}
}

func convertSchemaMap(fd protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
	var fs []*bigquery.FieldSchema
	for _, v := range m {
		switch x := v.(type) {
		case *bigquery.FieldSchema:
			fs = append(fs, x)
		default:
			panic(fmt.Sprintf("expected *bigquery.FieldSchema, got %T", x))
		}
	}
	return &bigquery.FieldSchema{
		Name:     string(fd.Name()),
		Type:     bigquery.RecordFieldType,
		Repeated: true,
		Schema:   fs,
	}
}

func NewSchemaConverter(options ...transforms.Option) SchemaConverter {
	options = append(options,
		transforms.OptionDefaultScalarFunc(convertSchemaScalar),
		transforms.OptionMessageFunc(convertSchemaMessage),
		transforms.OptionMapFunc(convertSchemaMap),
		transforms.OptionKeepEmpty(true),
	)
	cs := transforms.NewWalker(options...)
	cs.AddOverride(ts.FullName(), func(fd protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
		return &bigquery.FieldSchema{
			Name: string(fd.Name()),
			Type: bigquery.TimestampFieldType,
		}
	})
	return &schemaConverter{walker: cs}
}

func (sc *schemaConverter) Apply(md protoreflect.MessageDescriptor) []*bigquery.FieldSchema {
	out := sc.walker.ApplyDesc(md)
	var fs []*bigquery.FieldSchema
	switch x := out.(type) {
	case map[string]interface{}:
		for _, v := range x {
			switch y := v.(type) {
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

func convertRowMapFunc(md protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
	var kvs []map[string]interface{}
	for k, v := range m {
		kvs = append(kvs, map[string]interface{}{
			"key":   k,
			"value": v,
		})
	}
	return kvs
}

func NewRowConverter(options ...transforms.Option) RowConverter {
	basicOptions := []transforms.Option{
		transforms.OptionDefaultScalarFunc(convertRowScalar),
		transforms.OptionMapFunc(convertRowMapFunc),
	}
	cs := transforms.NewWalker(append(basicOptions, options...)...)
	cs.AddOverride(ts.FullName(), func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
		if v == nil {
			return nil
		}
		m := v.Message()
		seconds := ts.Fields().ByName("seconds")
		nanos := ts.Fields().ByName("nanos")
		return time.Unix(m.Get(seconds).Int(), m.Get(nanos).Int())
	})
	return &rowConverter{walker: cs}
}
