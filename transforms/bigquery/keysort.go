package bigquery

import (
	"fmt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sort"
)

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	}
	panic(fmt.Sprintf("not an int: %T", v))
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	}
	panic(fmt.Sprintf("not a float: %T", v))
}

func toString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	panic(fmt.Sprintf("not a string: %T", v))
}

func sortMapKeys(fd protoreflect.FieldDescriptor, keys []interface{}) {
	switch fd.MapKey().Kind() {
	case protoreflect.BoolKind:
		sort.Slice(keys, func(i, j int) bool {
			switch x := keys[i].(type) {
			case bool:
				return !x
			}
			panic(fmt.Sprintf("unexpected type %T", keys[i]))
		})
	case protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		sort.Slice(keys, func(i, j int) bool {
			return toInt64(keys[i]) <= toInt64(keys[j])
		})
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		sort.Slice(keys, func(i, j int) bool {
			return toFloat64(keys[i]) <= toFloat64(keys[j])
		})
	case protoreflect.StringKind, protoreflect.BytesKind:
		sort.Slice(keys, func(i, j int) bool {
			return toString(keys[i]) <= toString(keys[j])
		})
	default:
		panic(fmt.Sprintf("unsupported type %v", fd.Kind()))
	}
}
