// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestLabelsToResourceAttributesAndKey(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected pcommon.Map
		key      string
	}{
		{
			name:     "Empty labels",
			labels:   map[string]string{},
			expected: pcommon.NewMap(),
			key:      "",
		},
		{
			name: "Single label",
			labels: map[string]string{
				"service": "test-service",
			},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("service.name", "test-service")
				return m
			}(),
			key: "service.name=test-service",
		},
		{
			name: "Multiple labels",
			labels: map[string]string{
				"service": "test-service",
				"pod":     "test-pod",
			},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("service.name", "test-service")
				m.PutStr("k8s.pod.name", "test-pod")
				return m
			}(),
			key: "k8s.pod.name=test-pod:service.name=test-service",
		},
		{
			name: "Unknown label",
			labels: map[string]string{
				"unknown": "value",
			},
			expected: pcommon.NewMap(),
			key:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, key := labelsToResourceAttributesAndKey(tt.labels)
			assert.Equal(t, tt.expected.AsRaw(), got.AsRaw())
			assert.Equal(t, tt.key, key)
		})
	}
}
