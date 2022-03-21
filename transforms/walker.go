// Copyright 2022 Hayo van Loon. All rights reserved.
// Use of this source code is governed by a licence
// that can be found in the LICENSE file.

package transforms

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ScalarFunc func(protoreflect.FieldDescriptor, *protoreflect.Value) interface{}
type MessageFunc func(protoreflect.FieldDescriptor, map[string]interface{}) interface{}
type MapFunc func(protoreflect.FieldDescriptor, map[interface{}]interface{}) interface{}
type RepeatedFunc func(protoreflect.FieldDescriptor, []interface{}) interface{}

const defaultMaxRecurse = 99

// A Walker walks over a Protocol Buffers message or message descriptor.
// Without additional configuration, it will return it as a map.
type Walker interface {
	// AddScalarFunc adds a conversion function for a scalar kind. If no
	// conversion function has been specified for a certain kind, the default
	// function specified via SetDefaultFunc will be applied.
	AddScalarFunc(protoreflect.Kind, ScalarFunc)

	// AddOverride defines a special treatment for the given message type.
	AddOverride(protoreflect.FullName, ScalarFunc)

	// Apply will apply this Walker to a Message.
	Apply(m proto.Message) interface{}

	// ApplyDesc will apply this Walker to a message Descriptor.
	ApplyDesc(d protoreflect.MessageDescriptor) interface{}
}

type walker struct {
	scalarFns map[protoreflect.Kind]ScalarFunc
	defltFn   ScalarFunc
	messageFn MessageFunc
	mapFn     MapFunc
	repFn     RepeatedFunc
	keepEmpty bool
	maxDepth  int
	overrides map[protoreflect.FullName]ScalarFunc
}

type Option interface {
	Apply(w *walker)
}

type optionMaxDepth struct {
	Value int
}

func (o *optionMaxDepth) Apply(w *walker) {
	w.maxDepth = o.Value
}

// OptionMaxDepth sets a maximum message recursion depth. This mitigates the
// impact of infinite recursion in recursive messages like protobuf.Struct.
// Only traversed Message fields add to the recursion depth.
func OptionMaxDepth(v int) Option {
	return &optionMaxDepth{Value: v}
}

type optionKeepEmpty struct {
	Value bool
}

func (o *optionKeepEmpty) Apply(w *walker) {
	w.keepEmpty = o.Value
}

func OptionKeepEmpty(v bool) Option {
	return &optionKeepEmpty{Value: v}
}

type optionDefaultScalarFunc struct {
	Value ScalarFunc
}

func (o *optionDefaultScalarFunc) Apply(w *walker) {
	w.defltFn = o.Value
}

// OptionDefaultScalarFunc sets the default scalar conversion function. When
// omitted, the default function is an identity function.
func OptionDefaultScalarFunc(fn ScalarFunc) Option {
	return &optionDefaultScalarFunc{Value: fn}
}

type optionMapFunc struct {
	Value MapFunc
}

func (o *optionMapFunc) Apply(w *walker) {
	w.mapFn = o.Value
}

// OptionMapFunc sets the map type conversion function. The default is an
// identity function.
func OptionMapFunc(fn MapFunc) Option {
	return &optionMapFunc{Value: fn}
}

type optionMessageFunc struct {
	Value MessageFunc
}

func (o *optionMessageFunc) Apply(w *walker) {
	w.messageFn = o.Value
}

// OptionMessageFunc sets the message type conversion function. The default is
// an identity function.
func OptionMessageFunc(fn MessageFunc) Option {
	return &optionMessageFunc{Value: fn}
}

type optionRepeatedFunc struct {
	Value RepeatedFunc
}

func (o *optionRepeatedFunc) Apply(w *walker) {
	w.repFn = o.Value
}

// OptionRepeatedFunc sets the repeated field conversion function. The default
// is an identity function.
func OptionRepeatedFunc(fn RepeatedFunc) Option {
	return &optionRepeatedFunc{Value: fn}
}

func NewWalker(options ...Option) Walker {
	w := &walker{}
	for _, option := range options {
		option.Apply(w)
	}
	if w.defltFn == nil {
		w.defltFn = func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
			if v == nil {
				return nil
			}
			return v.Interface()
		}
	}
	if w.messageFn == nil {
		w.messageFn = func(fd protoreflect.FieldDescriptor, m map[string]interface{}) interface{} {
			return m
		}
	}
	if w.mapFn == nil {
		w.mapFn = func(fd protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} {
			return m
		}
	}
	if w.repFn == nil {
		w.repFn = func(_ protoreflect.FieldDescriptor, xs []interface{}) interface{} {
			return xs
		}
	}
	if w.scalarFns == nil {
		w.scalarFns = make(map[protoreflect.Kind]ScalarFunc)
	}
	if w.maxDepth == 0 {
		w.maxDepth = defaultMaxRecurse
	}
	if w.overrides == nil {
		w.overrides = map[protoreflect.FullName]ScalarFunc{}
	}
	return w
}

func (w *walker) AddScalarFunc(kind protoreflect.Kind, fn ScalarFunc) {
	if !kind.IsValid() {
		panic("invalid kind")
	}
	if kind == protoreflect.MessageKind || kind == protoreflect.GroupKind {
		panic("must be a scalar kind")
	}
	w.scalarFns[kind] = fn
}

func (w *walker) AddOverride(name protoreflect.FullName, fn ScalarFunc) {
	w.overrides[name] = fn
}

func (w *walker) Apply(m proto.Message) interface{} {
	mp := m.ProtoReflect()
	v := w.convertMessage(mp.Descriptor(), m.ProtoReflect(), 0)
	if !w.keepEmpty && v == nil {
		return nil
	}
	return w.convertMessage(mp.Descriptor(), mp, 0)
}

func (w *walker) ApplyDesc(d protoreflect.MessageDescriptor) interface{} {
	return w.convertMessage(d, nil, 0)
}

func (w *walker) convertMessage(md protoreflect.MessageDescriptor, m protoreflect.Message, depth int) map[string]interface{} {
	// only messages induce a risk of infinite recursion
	if depth > w.maxDepth {
		return nil
	}
	fds := md.Fields()
	return w.convertMessageFields(fds, m, depth)
}

func (w *walker) convertMessageFields(fds protoreflect.FieldDescriptors, m protoreflect.Message, depth int) map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < fds.Len(); i += 1 {
		fd := fds.Get(i)
		if m == nil {
			result[string(fd.Name())] = w.convertValue(fd, nil, depth+1)
		} else {
			v := m.Get(fd)
			x := w.convertValue(fd, &v, depth+1)
			if x != nil {
				result[string(fd.Name())] = x
			}
		}
	}
	return result
}

// convertValue converts a value.
func (w *walker) convertValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	if fd.IsMap() {
		return w.applyMapFn(fd, v, depth)
	}
	if fd.Cardinality() == protoreflect.Repeated {
		return w.applyRepFn(fd, v, depth)
	}
	if fd.Kind() == protoreflect.MessageKind {
		if v == nil {
			return w.applyMessageFn(fd, nil, depth)
		}
		return w.applyMessageFn(fd, v.Message(), depth)
	}
	return w.convertNonRepeatedValue(fd, v, depth)
}

// convertNonRepeatedValue converts a value that is assumed to not be repeated.
// This is the case for items in a repeated field or map values. No checks are
// (nor can be) performed on this assumption.
func (w *walker) convertNonRepeatedValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	if fd.Kind() == protoreflect.MessageKind {
		override := w.overrides[fd.Message().FullName()]
		if override != nil {
			return override(fd, v)
		}
		if v == nil {
			return w.applyMessageFn(fd, nil, depth)
		}
		if !v.IsValid() {
			return nil
		}
		return w.applyMessageFn(fd, v.Message(), depth)
	}
	return w.applyScalarFn(fd, v)
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

func (w *walker) convertMap(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) map[interface{}]interface{} {
	vfd := fd.MapValue()
	m := make(map[interface{}]interface{})
	if v == nil {
		kfd := fd.MapKey()
		m[kfd.Name()] = w.convertNonRepeatedValue(kfd, nil, depth)
		m[vfd.Name()] = w.convertNonRepeatedValue(vfd, nil, depth)
	} else {
		rangeFn := func(k protoreflect.MapKey, iv protoreflect.Value) bool {
			x := w.convertNonRepeatedValue(vfd, &iv, depth)
			if x != nil {
				m[k.Interface()] = x
			}
			return true
		}
		v.Map().Range(rangeFn)
	}
	return m
}

func (w *walker) applyScalarFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
	if !w.keepEmpty && (v == nil || IsDefaultScalar(v)) {
		return nil
	}
	fn := w.scalarFns[fd.Kind()]
	if fn == nil {
		return w.defltFn(fd, v)
	}
	return fn(fd, v)
}

func (w walker) applyRepFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	r := w.convertList(fd, v, depth)
	if !w.keepEmpty && len(r) == 0 {
		return nil
	}
	return w.repFn(fd, r)
}

func (w walker) applyMessageFn(fd protoreflect.FieldDescriptor, m protoreflect.Message, depth int) interface{} {
	if depth > w.maxDepth {
		return nil
	}
	fds := fd.Message().Fields()
	v := w.convertMessageFields(fds, m, depth)
	if !w.keepEmpty && len(v) == 0 {
		return nil
	}
	return w.messageFn(fd, v)
}

func (w walker) applyMapFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value, depth int) interface{} {
	m := w.convertMap(fd, v, depth)
	if !w.keepEmpty && len(m) == 0 {
		return nil
	}
	return w.mapFn(fd, m)
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
