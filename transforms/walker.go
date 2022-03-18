// Copyright 2022 Hayo van Loon. All rights reserved.
// Use of this source code is governed by a licence
// that can be found in the LICENSE file.

package protowalker

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ConvertFunc func(protoreflect.FieldDescriptor, *protoreflect.Value) interface{}
type MessageFunc func(protoreflect.FieldDescriptor, map[string]interface{}) interface{}
type RepeatedFunc func(protoreflect.FieldDescriptor, []interface{}) interface{}

const defaultMaxRecurse = 99

// A Walker walks over a Protocol Buffers message or message descriptor.
// Without additional configuration, it will return it as a map.
type Walker interface {
	// AddScalarFunc adds a conversion function for a scalar kind. If no
	// conversion function has been specified for a certain kind, the default
	// function specified via SetDefaultFunc will be applied.
	AddScalarFunc(protoreflect.Kind, ConvertFunc)

	// SetDefaultFunc sets the default conversion function. The default function
	// is an identity function.
	SetDefaultFunc(ConvertFunc)

	// SetMessageFunc sets the message type conversion function. The default is
	// an identity function.
	SetMessageFunc(MessageFunc)

	// SetMapFunc sets the map type conversion function. The default is an
	// identity function.
	SetMapFunc(MessageFunc)

	// SetRepeatedFunc sets the repeated field conversion function. The default
	// is an identity function.
	SetRepeatedFunc(RepeatedFunc)

	// AddOverride defines a special treatment for the given message type.
	AddOverride(protoreflect.FullName, ConvertFunc)

	// SetMaxDepth sets a maximum message recursion depth. This mitigates the
	// impact of infinite recursion in recursive messages like protobuf.Struct.
	// Only traversed Message fields add to the recursion depth.
	SetMaxDepth(int)

	// Apply will apply this Walker to a Message.
	Apply(m proto.Message) interface{}

	// ApplyDesc will apply this Walker to a message Descriptor.
	ApplyDesc(d protoreflect.MessageDescriptor) interface{}
}

type walker struct {
	scalarFns map[protoreflect.Kind]ConvertFunc
	defltFn   ConvertFunc
	messageFn MessageFunc
	mapFn     MessageFunc
	repFn     RepeatedFunc
	maxDepth  int
	overrides map[protoreflect.FullName]ConvertFunc
}

// BasicConvertFunc returns an identity function for scalar values. If keepEmpty
// is false, default values (like zero or empty string) will be skipped.
func BasicConvertFunc(keepEmpty bool) ConvertFunc {
	return func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
		if v == nil || (!keepEmpty && IsDefaultScalar(v)) {
			return nil
		}
		return v.Interface()
	}
}

// BasicMessageFunc returns an identity function for map and message values. If
// keepEmpty is false, default values (empty messages and maps) will be skipped.
func BasicMessageFunc(keepEmpty bool) MessageFunc {
	return func(_ protoreflect.FieldDescriptor, m map[string]interface{}) interface{} {
		if !keepEmpty && len(m) == 0 {
			return nil
		}
		return m
	}
}

// BasicRepeatedFunc returns an identity function for repeated values. If
// keepEmpty, empty lists will be skipped.
func BasicRepeatedFunc(keepEmpty bool) RepeatedFunc {
	return func(_ protoreflect.FieldDescriptor, xs []interface{}) interface{} {
		if !keepEmpty && len(xs) == 0 {
			return nil
		}
		return xs
	}
}

func NewWalker() Walker {
	defltFn := BasicConvertFunc(false)
	messageOrMapFn := BasicMessageFunc(false)
	repFn := BasicRepeatedFunc(false)
	return &walker{
		scalarFns: make(map[protoreflect.Kind]ConvertFunc),
		defltFn:   defltFn,
		messageFn: messageOrMapFn,
		mapFn:     messageOrMapFn,
		repFn:     repFn,
		maxDepth:  defaultMaxRecurse,
		overrides: map[protoreflect.FullName]ConvertFunc{},
	}
}

func NewWalkerKeepEmpty() Walker {
	defltFn := BasicConvertFunc(true)
	messageOrMapFn := BasicMessageFunc(true)
	repFn := BasicRepeatedFunc(true)
	return &walker{
		scalarFns: make(map[protoreflect.Kind]ConvertFunc),
		defltFn:   defltFn,
		messageFn: messageOrMapFn,
		mapFn:     messageOrMapFn,
		repFn:     repFn,
		maxDepth:  defaultMaxRecurse,
		overrides: map[protoreflect.FullName]ConvertFunc{},
	}
}

func (w *walker) AddScalarFunc(kind protoreflect.Kind, fn ConvertFunc) {
	if !kind.IsValid() {
		panic("invalid kind")
	}
	if kind == protoreflect.MessageKind || kind == protoreflect.GroupKind {
		panic("must be a scalar kind")
	}
	w.scalarFns[kind] = fn
}

func (w *walker) SetDefaultFunc(fn ConvertFunc) {
	w.defltFn = fn
}

func (w *walker) SetMessageFunc(fn MessageFunc) {
	w.messageFn = fn
}

func (w *walker) SetMapFunc(fn MessageFunc) {
	w.mapFn = fn
}

func (w *walker) SetRepeatedFunc(fn RepeatedFunc) {
	w.repFn = fn
}

func (w *walker) SetMaxDepth(i int) {
	w.maxDepth = i
}

func (w *walker) AddOverride(name protoreflect.FullName, fn ConvertFunc) {
	w.overrides[name] = fn
}

func (w *walker) getScalarFn(kind protoreflect.Kind) ConvertFunc {
	fn := w.scalarFns[kind]
	if fn == nil {
		return w.defltFn
	}
	return fn
}

func (w *walker) Apply(m proto.Message) interface{} {
	mp := m.ProtoReflect()
	v := w.convertMessage(mp.Descriptor(), m.ProtoReflect(), 0)
	return w.messageFn(nil, v)
}

func (w *walker) ApplyDesc(d protoreflect.MessageDescriptor) interface{} {
	v := w.convertMessage(d, nil, 0)
	return w.messageFn(nil, v)
}

func (w *walker) convertMessage(d protoreflect.MessageDescriptor, m protoreflect.Message, depth int) map[string]interface{} {
	// only messages induce a risk of infinite recursion
	if depth > w.maxDepth {
		return nil
	}
	mp := make(map[string]interface{})
	fs := d.Fields()
	for i := 0; i < fs.Len(); i += 1 {
		fd := fs.Get(i)
		if m == nil {
			mp[string(fd.Name())] = w.convertValue(fd, nil, depth+1)
		} else {
			v := m.Get(fd)
			x := w.convertValue(fd, &v, depth+1)
			if x != nil {
				mp[string(fd.Name())] = x
			}
		}
	}
	return mp
}

// convertValue converts a value.
func (w *walker) convertValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	if fd.IsMap() {
		return w.mapFn(fd, w.convertMap(fd, v, depth))
	}
	if fd.Cardinality() == protoreflect.Repeated {
		return w.repFn(fd, w.convertList(fd, v, depth))
	}
	if fd.Kind() == protoreflect.MessageKind {
		//if fd.IsMap() {
		//	return w.mapFn(fd, w.convertMap(fd, v, depth))
		//}
		return w.messageFn(fd, w.convertMessage(fd.Message(), v.Message(), depth))
	}
	return w.convertNonRepeatedValue(fd, v, depth)
}

// convertNonRepeatedValue converts a value that is assumed to not be repeated.
// This is the case for items in a repeated field or map values. No checks are
// (nor can be) performed on this assumption.
func (w *walker) convertNonRepeatedValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	if fd.Kind() == protoreflect.MessageKind {
		d := fd.Message()
		override := w.overrides[d.FullName()]
		if override != nil {
			return override(fd, v)
		}
		if v == nil {
			return w.messageFn(fd, w.convertMessage(d, nil, depth))
		}
		if !v.IsValid() {
			return nil
		}
		m := v.Message()
		return w.messageFn(fd, w.convertMessage(d, m, depth))
	}
	return w.getScalarFn(fd.Kind())(fd, v)
}

func (w *walker) convertList(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) []interface{} {
	if v == nil {
		return nil
	}
	xs := v.List()
	var ys []interface{}
	for i := 0; i < xs.Len(); i += 1 {
		x := xs.Get(i)
		if y := w.convertNonRepeatedValue(fd, &x, depth); y != nil {
			ys = append(ys, y)
		}
	}
	return ys
}

func (w *walker) convertMap(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) map[string]interface{} {
	if v == nil {
		return w.convertMessage(fd.Message(), nil, depth)
	}
	m := make(map[string]interface{})
	mv := fd.MapValue()
	rangeFn := func(k protoreflect.MapKey, iv protoreflect.Value) bool {
		x := w.convertNonRepeatedValue(mv, &iv, depth)
		if x != nil {
			m[k.String()] = x
		}
		return true
	}
	v.Map().Range(rangeFn)
	return m
}

func IsDefaultScalar(v *protoreflect.Value) bool {
	switch x := v.Interface().(type) {
	case bool:
		return !x
	case int32:
		return x == 0
	case int64:
		return x == 0
	case uint32:
		return x == 0
	case uint64:
		return x == 0
	case float32:
		return x == 0
	case float64:
		return x == 0
	case string:
		return x == ""
	case []byte:
		return len(x) == 0
	case protoreflect.EnumNumber:
		return x == 0
	default:
		panic(fmt.Sprintf("unexpected type %T", x))
	}
}
