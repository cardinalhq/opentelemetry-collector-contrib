// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
)

// The metricsConsumer implements the firehoseConsumer
// to use a metrics consumer and unmarshaler.
type metricsConsumer struct {
	// consumer passes the translated metrics on to the
	// next consumer.
	consumer consumer.Metrics
	// unmarshaler is the configured MetricsUnmarshaler
	// to use when processing the records.
	unmarshaler unmarshaler.MetricsUnmarshaler
}

var _ firehoseConsumer = (*metricsConsumer)(nil)

// newMetricsReceiver creates a new instance of the receiver
// with a metricsConsumer.
func newMetricsReceiver(
	config *MetricsConfig,
	unmarshalers map[string]unmarshaler.MetricsUnmarshaler,
	nextConsumer consumer.Metrics,
) (*metricsConsumer, error) {
	configuredUnmarshaler := unmarshalers[config.RecordType]
	if configuredUnmarshaler == nil {
		return nil, errUnrecognizedRecordType
	}
	mc := &metricsConsumer{
		consumer:    nextConsumer,
		unmarshaler: configuredUnmarshaler,
	}
	return mc, nil
}

// Consume uses the configured unmarshaler to deserialize the records into a
// single pmetric.Metrics. If there are common attributes available, then it will
// attach those to each of the pcommon.Resources. It will send the final result
// to the next consumer.
func (mc *metricsConsumer) Consume(ctx context.Context, records [][]byte, commonAttributes map[string]string) (int, error) {
	md, err := mc.unmarshaler.Unmarshal(records)
	if err != nil {
		return http.StatusBadRequest, err
	}

	if commonAttributes != nil {
		applyCommonAttributes(md, commonAttributes)
	}

	err = mc.consumer.ConsumeMetrics(ctx, md)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func applyCommonAttributes(md pmetric.Metrics, commonAttributes map[string]string) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for k, v := range commonAttributes {
			if _, found := rm.Resource().Attributes().Get(k); !found {
				rm.Resource().Attributes().PutStr(k, v)
			}
		}
	}
}

// replace all non-alphanumeric characters with underscores, other than _ and -.
func sanitizeValue(value string) string {
	s := strings.Map(func(r rune) rune {
		if r == '_' || r == '-' {
			return r
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, value)
	// replace runs of underscores with a single underscore
	s = strings.ReplaceAll(s, "__", "_")
	// trim leading and trailing underscores
	return strings.Trim(s, "_")
}

func (mc *metricsConsumer) RecordType() string {
	return mc.unmarshaler.Type()
}

func (mc *metricsConsumer) TelemetryType() string {
	return "metrics"
}
