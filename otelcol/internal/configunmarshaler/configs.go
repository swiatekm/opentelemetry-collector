// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configunmarshaler // import "go.opentelemetry.io/collector/otelcol/internal/configunmarshaler"

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/exp/maps"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

type Configs[F component.Factory] struct {
	cfgs map[component.ID]component.Config

	factories map[component.Type]F
}

func NewConfigs[F component.Factory](factories map[component.Type]F) *Configs[F] {
	return &Configs[F]{factories: factories}
}

func (c *Configs[F]) Unmarshal(conf *confmap.Conf) error {
	ids, err := componentIDs(conf)
	if err != nil {
		return err
	}

	// Prepare resulting map.
	c.cfgs = make(map[component.ID]component.Config)
	// Iterate over raw configs and create a config for each.
	for _, id := range ids {
		// Find factory based on component kind and type that we read from config source.
		factory, ok := c.factories[id.Type()]
		if !ok {
			return errorUnknownType(id, maps.Keys(c.factories))
		}

		// Get the configuration from the confmap.Conf to preserve internal representation.
		sub, err := conf.Sub(id.String())
		if err != nil {
			return errorUnmarshalError(id, err)
		}

		// Create the default config for this component.
		cfg := factory.CreateDefaultConfig()

		// Now that the default config struct is created we can Unmarshal into it,
		// and it will apply user-defined config on top of the default.
		if err := sub.Unmarshal(&cfg); err != nil {
			return errorUnmarshalError(id, err)
		}

		c.cfgs[id] = cfg
	}

	return nil
}

func (c *Configs[F]) Configs() map[component.ID]component.Config {
	return c.cfgs
}

// componentIDs returns the set of top-level component IDs present in conf.
//
// This intentionally avoids conf.Unmarshal(&map[component.ID]map[string]any{}):
// that decodes every component's entire config tree just to read off the map
// keys, since the decoded values are never used here (each component's
// config is fetched afterward via conf.Sub to preserve internal
// representation, e.g. env-var expansion metadata, that a generic
// map[string]any decode loses). AllKeys already returns every leaf path in
// conf; the top-level component ID is just its first path segment, which is
// far cheaper to recover than decoding the full subtree per component.
func componentIDs(conf *confmap.Conf) ([]component.ID, error) {
	seenRaw := make(map[string]struct{})
	seenIDs := make(map[component.ID]struct{})
	var ids []component.ID
	for _, key := range conf.AllKeys() {
		top, _, _ := strings.Cut(key, confmap.KeyDelimiter)
		if _, ok := seenRaw[top]; ok {
			continue
		}
		seenRaw[top] = struct{}{}

		var id component.ID
		if err := id.UnmarshalText([]byte(top)); err != nil {
			return nil, fmt.Errorf("invalid component id %q: %w", top, err)
		}
		// Two differently-formatted keys (e.g. differing only in whitespace
		// around the type/name separator) can normalize to the same ID.
		if _, ok := seenIDs[id]; ok {
			return nil, fmt.Errorf("duplicate name %q after trimming whitespace", id)
		}
		seenIDs[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func errorUnknownType(id component.ID, factories []component.Type) error {
	if id.Type().String() == "logging" {
		return errors.New("the logging exporter has been deprecated, use the debug exporter instead")
	}
	return fmt.Errorf("unknown type: %q for id: %q (valid values: %v)", id.Type(), id, factories)
}

func errorUnmarshalError(id component.ID, err error) error {
	return fmt.Errorf("error reading configuration for %q: %w", id, err)
}
