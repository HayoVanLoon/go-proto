// Copyright 2022 Hayo van Loon. All rights reserved.
// Use of this source code is governed by a licence
// that can be found in the LICENSE file.

package transforms

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sort"
)

type ScalarFunc func(protoreflect.FieldDescriptor, *protoreflect.Value) interface{}
type MessageFunc func(protoreflect.FieldDescriptor, []KeyValue) interface{}
type MapFunc func(protoreflect.FieldDescriptor, map[interface{}]interface{}) interface{}
type RepeatedFunc func(protoreflect.FieldDescriptor, []interface{}) interface{}

const defaultMaxRecurse = 99

// A Walker walks over a Protocol Buffers message or message descriptor.
// Without additional configuration, it will return it as a map.
type Walker interface {
	// Apply will apply this Walker to a Message.
	Apply(m proto.Message) interface{}

	// ApplyDesc will apply this Walker to a message Descriptor.
	ApplyDesc(d protoreflect.MessageDescriptor) interface{}
}

type KeyValue struct {
	Key   string
	Value interface{}
}

type walker struct {
	scalarFns map[protoreflect.Kind]ScalarFunc
	defltFn   ScalarFunc
	messageFn MessageFunc
	mapFn     MapFunc
	repFn     RepeatedFunc
	keepEmpty bool
	keepOrder bool
	maxDepth  int
	overrides map[string]MessageFunc
}

type Option interface {
	// Type returns the Option's type.
	Type() OptionType

	// Apply applies the Option to the Walker.
	Apply(w *walker)
}

type OptionType int

const (
	OptionTypeKeepEmpty = OptionType(1) + iota
	OptionTypeMaxDepth
	OptionTypeDefaultScalarFunc
	OptionTypeMapFunc
	OptionTypeMessageFunc
	OptionTypeRepeatedFunc
	OptionTypeKeepOrder
	OptionTypeAddOverride
	OptionTypeAddScalarFunc
)

type optionMaxDepth struct {
	value int
}

func (o *optionMaxDepth) Type() OptionType {
	return OptionTypeMaxDepth
}

func (o *optionMaxDepth) Apply(w *walker) {
	w.maxDepth = o.value
}

// OptionMaxDepth sets a maximum message recursion depth. This mitigates the
// impact of infinite recursion in recursive messages like protobuf.Struct.
// Only traversed Message fields add to the recursion depth.
func OptionMaxDepth(v int) Option {
	return &optionMaxDepth{value: v}
}

type optionKeepEmpty struct {
	value bool
}

func (o *optionKeepEmpty) Type() OptionType {
	return OptionTypeKeepEmpty
}

func (o *optionKeepEmpty) Apply(w *walker) {
	w.keepEmpty = o.value
}

func OptionKeepEmpty(v bool) Option {
	return &optionKeepEmpty{value: v}
}

type optionDefaultScalarFunc struct {
	value ScalarFunc
}

func (o *optionDefaultScalarFunc) Type() OptionType {
	return OptionTypeDefaultScalarFunc
}

func (o *optionDefaultScalarFunc) Apply(w *walker) {
	w.defltFn = o.value
}

// OptionDefaultScalarFunc sets the default scalar conversion function. When
// omitted, the default function is an identity function.
func OptionDefaultScalarFunc(fn ScalarFunc) Option {
	return &optionDefaultScalarFunc{value: fn}
}

type optionMapFunc struct {
	value MapFunc
}

func (o *optionMapFunc) Type() OptionType {
	return OptionTypeMapFunc
}

func (o *optionMapFunc) Apply(w *walker) {
	w.mapFn = o.value
}

// OptionMapFunc sets the map type conversion function. The default is an
// identity function.
func OptionMapFunc(fn MapFunc) Option {
	return &optionMapFunc{value: fn}
}

type optionMessageFunc struct {
	value MessageFunc
}

func (o *optionMessageFunc) Type() OptionType {
	return OptionTypeMessageFunc
}

func (o *optionMessageFunc) Apply(w *walker) {
	w.messageFn = o.value
}

// OptionMessageFunc sets the message type conversion function. The default is
// an identity function. The top-level message will not be treated by this
// function; it has no FieldDescriptor.
func OptionMessageFunc(fn MessageFunc) Option {
	return &optionMessageFunc{value: fn}
}

type optionRepeatedFunc struct {
	value RepeatedFunc
}

func (o *optionRepeatedFunc) Type() OptionType {
	return OptionTypeRepeatedFunc
}

func (o *optionRepeatedFunc) Apply(w *walker) {
	w.repFn = o.value
}

// OptionRepeatedFunc sets the repeated field conversion function. The default
// is an identity function.
func OptionRepeatedFunc(fn RepeatedFunc) Option {
	return &optionRepeatedFunc{value: fn}
}

type optionKeepOrder struct {
	value bool
}

func (o *optionKeepOrder) Type() OptionType {
	return OptionTypeKeepOrder
}

func (o *optionKeepOrder) Apply(w *walker) {
	w.keepOrder = o.value
}

// OptionKeepOrder will cause the Walker to return a list of KeyValue structs if
// set to true, rather than an unordered map. This list will be ordered
// according to the fields' protocol buffer numbers.
func OptionKeepOrder(v bool) Option {
	return &optionKeepOrder{value: v}
}

type optionAddOverride struct {
	key   string
	value MessageFunc
}

func (o *optionAddOverride) Type() OptionType {
	return OptionTypeAddOverride
}

func (o *optionAddOverride) Apply(w *walker) {
	w.overrides[o.key] = o.value
}

// OptionAddOverride defines a special treatment for the given message type. The
// type name is expected to be the full name, i.e. 'string'.
//
// You can effectively use multiple instances of this option as long as their
// type name differs.
func OptionAddOverride(type_ string, v MessageFunc) Option {
	return &optionAddOverride{key: type_, value: v}
}

type optionAddScalarFunc struct {
	key   protoreflect.Kind
	value ScalarFunc
}

func (o *optionAddScalarFunc) Type() OptionType {
	return OptionTypeAddScalarFunc
}

func (o *optionAddScalarFunc) Apply(w *walker) {
	w.scalarFns[o.key] = o.value
}

// OptionAddScalarFunc adds a conversion function for a scalar kind. If no
// conversion function has been specified for a certain kind, the default
// function specified via SetDefaultFunc will be applied.
//
// You can effectively use multiple instances of this option as long as their
// protoreflect.Kind differs.
func OptionAddScalarFunc(k protoreflect.Kind, v ScalarFunc) Option {
	return &optionAddScalarFunc{key: k, value: v}
}

// NewWalker spawns a new Walker. The provided options will be processed in
// sequence and later options may overwrite earlier ones.
func NewWalker(options ...Option) Walker {
	w := &walker{
		overrides: map[string]MessageFunc{},
		scalarFns: map[protoreflect.Kind]ScalarFunc{},
	}
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
		w.messageFn = func(fd protoreflect.FieldDescriptor, kvs []KeyValue) interface{} {
			m := make(map[string]interface{})
			for _, kv := range kvs {
				m[kv.Key] = kv.Value
			}
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
	if w.maxDepth <= 0 {
		w.maxDepth = defaultMaxRecurse
	}
	return w
}

func (w *walker) Apply(m proto.Message) interface{} {
	mp := m.ProtoReflect()
	return w.convertMessage(mp.Descriptor(), mp, 0)
}

func (w *walker) ApplyDesc(d protoreflect.MessageDescriptor) interface{} {
	return w.convertMessage(d, nil, 0)
}

func (w *walker) convertMessage(md protoreflect.MessageDescriptor, m protoreflect.Message, depth int) interface{} {
	// only messages induce a risk of infinite recursion
	if depth > w.maxDepth {
		return nil
	}
	fds := md.Fields()
	kvs := w.convertMessageFields(fds, m, depth)
	if w.keepOrder {
		return kvs
	}
	result := make(map[string]interface{})
	for _, kv := range kvs {
		result[kv.Key] = kv.Value
	}
	return result
}

func (w *walker) convertMessageFields(fds protoreflect.FieldDescriptors, m protoreflect.Message, depth int) []KeyValue {
	var result []KeyValue
	ss := make([]protoreflect.FieldDescriptor, fds.Len())
	for i := 0; i < len(ss); i += 1 {
		ss[i] = fds.Get(i)
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Number() < ss[j].Number()
	})
	for _, fd := range ss {
		if m == nil {
			result = append(result, KeyValue{string(fd.Name()), w.convertValue(fd, nil, depth+1)})
		} else {
			v := m.Get(fd)
			x := w.convertValue(fd, &v, depth+1)
			if x != nil {
				result = append(result, KeyValue{string(fd.Name()), x})
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
		m[string(kfd.Name())] = w.convertNonRepeatedValue(kfd, nil, depth)
		m[string(vfd.Name())] = w.convertNonRepeatedValue(vfd, nil, depth)
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
	override := w.overrides[string(fd.Message().FullName())]
	if override != nil {
		return override(fd, v)
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
