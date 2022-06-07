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

type OverrideFunc func(protoreflect.FieldDescriptor, interface{}) interface{}

type ScalarFunc func(protoreflect.FieldDescriptor, *protoreflect.Value) interface{}
type MessageFunc func(protoreflect.FieldDescriptor, []KeyValue) interface{}
type MapFunc func(protoreflect.FieldDescriptor, map[interface{}]interface{}) interface{}
type RepeatedFunc func(protoreflect.FieldDescriptor, []interface{}) interface{}

func FromScalarFunc(f ScalarFunc) OverrideFunc {
	return func(fd protoreflect.FieldDescriptor, v interface{}) interface{} {
		switch x := v.(type) {
		case *protoreflect.Value:
			return f(fd, x)
		}
		panic(fmt.Sprintf("expected type *protoreflect.Value, got %T", v))
	}
}

func FromMapFunc(f MapFunc) OverrideFunc {
	return func(fd protoreflect.FieldDescriptor, v interface{}) interface{} {
		switch x := v.(type) {
		case map[interface{}]interface{}:
			return f(fd, x)
		}
		panic(fmt.Sprintf("expected type map[interface{}]interface{}, got %T", v))
	}
}

func FromMessageFunc(f MessageFunc) OverrideFunc {
	return func(fd protoreflect.FieldDescriptor, v interface{}) interface{} {
		switch x := v.(type) {
		case []KeyValue:
			return f(fd, x)
		}
		panic(fmt.Sprintf("expected type []KeyValue, got %T", v))
	}
}

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
	scalarFns       map[protoreflect.Kind]ScalarFunc
	defltFn         ScalarFunc
	messageFn       MessageFunc
	mapFn           MapFunc
	repFn           RepeatedFunc
	keepEmpty       bool
	keepOrder       bool
	maxDepth        int
	maxDepthForName map[string]int
	typeOverrides   map[string]MessageFunc
	nameOverrides   map[string]OverrideFunc
}

type Option interface {
	// Type returns the Option's type.
	Type() OptionType

	// Apply applies the Option to the Walker.
	Apply(w *walker)
}

type OptionType int

// TODO(hvl): OptionTypeMaxDepthForType
const (
	OptionTypeKeepEmpty = OptionType(1) + iota
	OptionTypeMaxDepth
	OptionTypeMaxDepthForName
	OptionTypeDefaultScalarFunc
	OptionTypeMapFunc
	OptionTypeMessageFunc
	OptionTypeRepeatedFunc
	OptionTypeKeepOrder
	OptionTypeAddOverride
	OptionTypeAddNameOverride
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
// impact of infinite recursion in recursive messages (like protobuf.Struct).
// Only traversed Message fields add to the recursion depth.
//
// A depth of zero will return the root level fields. If depth is set to less
// than zero, the default recursion depth (99) will be used.
func OptionMaxDepth(v int) Option {
	return &optionMaxDepth{value: v}
}

type optionMaxDepthForName struct {
	key   string
	value int
}

func (o *optionMaxDepthForName) Type() OptionType {
	return OptionTypeMaxDepthForName
}

func (o *optionMaxDepthForName) Apply(w *walker) {
	w.maxDepthForName[o.key] = o.value
}

// OptionMaxDepthForName sets a maximum message recursion depth starting from
// the given field. This mitigates the impact of infinite recursion in recursive
// messages (like protobuf.Struct). Only traversed Message fields add to the
// recursion depth.
//
// A depth of zero will return the field. If depth is set to less
// than zero, the default recursion depth (99) will be used.
func OptionMaxDepthForName(name string, v int) Option {
	return &optionMaxDepthForName{key: name, value: v}
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

type optionAddTypeOverride struct {
	key   string
	value MessageFunc
}

func (o *optionAddTypeOverride) Type() OptionType {
	return OptionTypeAddOverride
}

func (o *optionAddTypeOverride) Apply(w *walker) {
	w.typeOverrides[o.key] = o.value
}

// OptionAddTypeOverride defines a special treatment for the given message type.
// The type name is expected to be the full name, i.e. 'acme.products.Anvil'.
//
// You can effectively use multiple instances of this option as long as their
// type name differs - otherwise, earlier ones will be overwritten by later
// ones.
func OptionAddTypeOverride(type_ string, v MessageFunc) Option {
	return &optionAddTypeOverride{key: type_, value: v}
}

type optionAddNameOverride struct {
	key   string
	value OverrideFunc
}

func (o *optionAddNameOverride) Type() OptionType {
	return OptionTypeAddNameOverride
}

func (o *optionAddNameOverride) Apply(w *walker) {
	w.nameOverrides[o.key] = o.value
}

// OptionAddNameOverride defines a special (post-)processing for the given
// field. Except for scalar fields, processing occurs after the normal
// conversion. For scalar fields, it is applied instead of normal conversion.
//
// The name is a chain of field names, separated by dots. Repeated fields and
// their subfields can be referenced (i.e. 'addresses.street'), but there is no
// support for indexing. The same goes for map fields.
//
// An override on a repeated field will be applied to each element. There is no
// support for post-processing the converted list as a whole. Such behaviour can
// be achieved by overriding the containing message.
//
// You can effectively use multiple instances of this option as long as their
// name differs - otherwise, earlier ones will be overwritten by later ones.
//
func OptionAddNameOverride(name string, v interface{}) Option {
	var f OverrideFunc
	switch x := v.(type) {
	case ScalarFunc:
		f = FromScalarFunc(x)
	case func(protoreflect.FieldDescriptor, *protoreflect.Value) interface{}:
		f = FromScalarFunc(x)
	case MessageFunc:
		f = FromMessageFunc(x)
	case func(fd protoreflect.FieldDescriptor, kvs []KeyValue) interface{}:
		f = FromMessageFunc(x)
	case MapFunc:
		f = FromMapFunc(x)
	case func(protoreflect.FieldDescriptor, map[interface{}]interface{}) interface{}:
		f = FromMapFunc(x)
	default:
		panic(fmt.Sprintf("Valid options: ScalarFunc, MessageFunc, MapFunc, got %T", v))
	}
	return &optionAddNameOverride{key: name, value: f}
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
// protoreflect.Kind differs - otherwise, earlier ones will be overwritten by
// later ones.
func OptionAddScalarFunc(k protoreflect.Kind, v ScalarFunc) Option {
	return &optionAddScalarFunc{key: k, value: v}
}

// NewWalker spawns a new Walker. The provided options will be processed in
// sequence and later options may overwrite earlier ones.
func NewWalker(options ...Option) Walker {
	w := &walker{
		defltFn: func(_ protoreflect.FieldDescriptor, v *protoreflect.Value) interface{} {
			if v == nil {
				return nil
			}
			return v.Interface()
		},
		messageFn: func(fd protoreflect.FieldDescriptor, kvs []KeyValue) interface{} {
			m := make(map[string]interface{}, len(kvs))
			for _, kv := range kvs {
				m[kv.Key] = kv.Value
			}
			return m
		},
		mapFn:           func(fd protoreflect.FieldDescriptor, m map[interface{}]interface{}) interface{} { return m },
		repFn:           func(_ protoreflect.FieldDescriptor, xs []interface{}) interface{} { return xs },
		maxDepth:        defaultMaxRecurse,
		maxDepthForName: map[string]int{},
		typeOverrides:   map[string]MessageFunc{},
		nameOverrides:   map[string]OverrideFunc{},
		scalarFns:       map[protoreflect.Kind]ScalarFunc{},
	}
	for _, option := range options {
		option.Apply(w)
	}
	if w.maxDepth < 0 {
		w.maxDepth = defaultMaxRecurse
	}
	return w
}

func (w *walker) Apply(m proto.Message) interface{} {
	mp := m.ProtoReflect()
	return w.convertMessage(mp.Descriptor(), mp, w.maxDepth, "")
}

func (w *walker) ApplyDesc(d protoreflect.MessageDescriptor) interface{} {
	return w.convertMessage(d, nil, w.maxDepth, "")
}

func (w *walker) createName(parent string, name protoreflect.Name) string {
	if parent == "" {
		return string(name)
	}
	return parent + "." + string(name)
}

func (w *walker) convertMessage(md protoreflect.MessageDescriptor, m protoreflect.Message, allowedDepth int, parent string) interface{} {
	fds := md.Fields()
	kvs := w.convertMessageFields(fds, m, allowedDepth, parent)
	if w.keepOrder {
		return kvs
	}
	result := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		result[kv.Key] = kv.Value
	}
	return result
}

func (w *walker) convertMessageFields(fds protoreflect.FieldDescriptors, m protoreflect.Message, allowedDepth int, parent string) []KeyValue {
	xs := make([]protoreflect.FieldDescriptor, fds.Len())
	for i := 0; i < len(xs); i += 1 {
		xs[i] = fds.Get(i)
	}
	sort.Slice(xs, func(i, j int) bool {
		return xs[i].Number() < xs[j].Number()
	})
	var kvs []KeyValue
	for _, fd := range xs {
		if m == nil {
			kvs = append(kvs, KeyValue{string(fd.Name()), w.convertValue(fd, nil, allowedDepth-1, parent)})
		} else {
			v := m.Get(fd)
			x := w.convertValue(fd, &v, allowedDepth-1, parent)
			if x != nil {
				kvs = append(kvs, KeyValue{string(fd.Name()), x})
			}
		}
	}
	return kvs
}

// convertValue converts a value.
func (w *walker) convertValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) interface{} {
	if fd.IsMap() {
		return w.applyMapFn(fd, v, allowedDepth, parent)
	}
	if fd.Cardinality() == protoreflect.Repeated {
		return w.applyRepFn(fd, v, allowedDepth, parent)
	}
	if fd.Kind() == protoreflect.MessageKind {
		if v == nil {
			return w.applyMessageFn(fd, nil, allowedDepth, parent)
		}
		return w.applyMessageFn(fd, v.Message(), allowedDepth, parent)
	}
	return w.convertNonRepeatedValue(fd, v, allowedDepth, parent)
}

// convertNonRepeatedValue converts a value that is assumed to not be repeated.
// This is the case for items in a repeated field or map values. No checks are
// (nor can be) performed on this assumption.
func (w *walker) convertNonRepeatedValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) interface{} {
	if fd.Kind() == protoreflect.MessageKind {
		if v == nil {
			return w.applyMessageFn(fd, nil, allowedDepth, parent)
		}
		if !v.IsValid() {
			return nil
		}
		return w.applyMessageFn(fd, v.Message(), allowedDepth, parent)
	}
	return w.applyScalarFn(fd, v, parent)
}

func (w *walker) convertList(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) []interface{} {
	if v == nil {
		return nil
	}
	xs := v.List()
	var ys []interface{}
	for i := 0; i < xs.Len(); i += 1 {
		x := xs.Get(i)
		if y := w.convertNonRepeatedValue(fd, &x, allowedDepth, parent); y != nil {
			ys = append(ys, y)
		}
	}
	return ys
}

func (w *walker) convertMap(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) map[interface{}]interface{} {
	name := w.createName(parent, fd.Name())
	vfd := fd.MapValue()
	m := make(map[interface{}]interface{})
	if v == nil {
		kfd := fd.MapKey()
		m[string(kfd.Name())] = w.convertNonRepeatedValue(kfd, nil, allowedDepth, name)
		m[string(vfd.Name())] = w.convertNonRepeatedValue(vfd, nil, allowedDepth, name)
	} else {
		rangeFn := func(k protoreflect.MapKey, iv protoreflect.Value) bool {
			x := w.convertNonRepeatedValue(vfd, &iv, allowedDepth, name)
			if x != nil {
				m[k.Interface()] = x
			}
			return true
		}
		v.Map().Range(rangeFn)
	}
	return m
}

func (w *walker) applyScalarFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value, parent string) interface{} {
	if !w.keepEmpty && (v == nil || IsDefaultScalar(v)) {
		return nil
	}
	name := w.createName(parent, fd.Name())
	if override := w.nameOverrides[name]; override != nil {
		return override(fd, v)
	}
	if fn := w.scalarFns[fd.Kind()]; fn != nil {
		return fn(fd, v)
	}
	return w.defltFn(fd, v)
}

func (w walker) applyRepFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) interface{} {
	r := w.convertList(fd, v, allowedDepth, parent)
	if !w.keepEmpty && len(r) == 0 {
		return nil
	}
	return w.repFn(fd, r)
}

func (w walker) applyMessageFn(fd protoreflect.FieldDescriptor, m protoreflect.Message, allowedDepth int, parent string) interface{} {
	// only messages induce a risk of infinite recursion
	if allowedDepth < 0 {
		return nil
	}
	name := w.createName(parent, fd.Name())
	if overrideDepth, ok := w.maxDepthForName[name]; ok {
		allowedDepth = overrideDepth
	}

	fds := fd.Message().Fields()
	kvs := w.convertMessageFields(fds, m, allowedDepth, name)
	if !w.keepEmpty && len(kvs) == 0 {
		return nil
	}

	if override := w.nameOverrides[name]; override != nil {
		return override(fd, kvs)
	}
	if override := w.typeOverrides[string(fd.Message().FullName())]; override != nil {
		return override(fd, kvs)
	}
	return w.messageFn(fd, kvs)
}

func (w walker) applyMapFn(fd protoreflect.FieldDescriptor, v *protoreflect.Value, allowedDepth int, parent string) interface{} {
	m := w.convertMap(fd, v, allowedDepth, parent)
	if !w.keepEmpty && len(m) == 0 {
		return nil
	}
	name := w.createName(parent, fd.Name())
	if override := w.nameOverrides[name]; override != nil {
		return override(fd, m)
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
