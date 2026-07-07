// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package graph // import "go.opentelemetry.io/collector/service/internal/graph"

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"reflect"
	"sort"

	"go.opentelemetry.io/collector/component"
)

// HashComponentConfig computes a stable, order-independent hash of a single
// component config value. See HashComponentConfigs for the rationale.
func HashComponentConfig(cfg component.Config) (uint64, error) {
	h := fnv.New64a()
	if err := hashValue(h, reflect.ValueOf(cfg)); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

// HashComponentConfigs computes a stable, order-independent hash for each
// config in cfgs. Partial receiver reload uses these hashes to detect which
// parts of the configuration changed between reloads instead of retaining an
// independent copy of every component config for later comparison: a typical
// collector configuration is dominated by component configs (particularly
// receivers), so copying all of them on every reload is expensive and grows
// with the number of components, even when only one of them actually
// changed. Component configs may also be mutated by the running component
// that owns them, so a stored reference to the original value cannot be
// compared safely later — hashing captures a value at a point in time
// without holding onto it.
//
// Hashing is done by reflecting over each config value's structure rather
// than by serializing it (e.g. via confmap or encoding/json). Serialization
// is unsafe here for the same reason it is unsafe for equality comparison:
// fields of type configopaque.String render as "[REDACTED]" when marshaled,
// which would hash configs with different secret values identically and
// cause a necessary reload to be silently skipped. Reflection reads raw
// field values without invoking marshal interfaces, so it distinguishes
// configs that differ only in opaque fields.
//
// Unexported struct fields are skipped: reflect cannot read them without
// tripping the "obtained from unexported field" restriction, and real
// component.Config values are decoded from YAML via mapstructure, which
// (like encoding/json) only ever populates exported fields. A config type
// that stored meaningful state in an unexported field would not be
// distinguished by this hash, but no component config in the collector does.
//
// A hash collision would cause a changed component to be treated as
// unchanged. Such collisions are astronomically unlikely for the
// well-structured, low-cardinality data found in component configs, and are
// judged an acceptable trade-off against always copying every component.
func HashComponentConfigs(cfgs map[component.ID]component.Config) (map[component.ID]uint64, error) {
	hashes := make(map[component.ID]uint64, len(cfgs))
	for id, cfg := range cfgs {
		h, err := HashComponentConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to hash config for %q: %w", id, err)
		}
		hashes[id] = h
	}
	return hashes, nil
}

// hashValue writes a structural representation of v to h. Maps are sorted by
// their keys' string representation first, so the result does not depend on
// map iteration order. Every variable-length value (strings, slices, arrays,
// maps) is preceded by its length so that, for example, a struct with string
// fields "a" and "bc" cannot hash the same as one with fields "ab" and "c".
func hashValue(h hash.Hash64, v reflect.Value) error {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return writeUint64(h, 0)
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return writeUint64(h, 0)
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue
			}
			if err := writeString(h, t.Field(i).Name); err != nil {
				return err
			}
			if err := hashValue(h, v.Field(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool {
			return fmt.Sprintf("%v", keys[i].Interface()) < fmt.Sprintf("%v", keys[j].Interface())
		})
		if err := writeUint64(h, uint64(len(keys))); err != nil {
			return err
		}
		for _, k := range keys {
			if err := hashValue(h, k); err != nil {
				return err
			}
			if err := hashValue(h, v.MapIndex(k)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if err := writeUint64(h, uint64(v.Len())); err != nil {
			return err
		}
		for i := 0; i < v.Len(); i++ {
			if err := hashValue(h, v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.String:
		return writeString(h, v.String())
	case reflect.Bool:
		b := uint64(0)
		if v.Bool() {
			b = 1
		}
		return writeUint64(h, b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return writeUint64(h, uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return writeUint64(h, v.Uint())
	case reflect.Float32, reflect.Float64:
		return writeUint64(h, math.Float64bits(v.Float()))
	default:
		// Funcs, channels, and unsafe pointers are not expected in component
		// configs; ignore them rather than fail hashing entirely.
		return nil
	}
	return nil
}

func writeString(h hash.Hash64, s string) error {
	if err := writeUint64(h, uint64(len(s))); err != nil {
		return err
	}
	_, err := h.Write([]byte(s))
	return err
}

func writeUint64(h hash.Hash64, n uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], n)
	_, err := h.Write(buf[:])
	return err
}
