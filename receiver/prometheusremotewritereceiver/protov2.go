// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"time"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// translateV2 translates a v2 remote-write request into OTLP metrics.
// For now translateV2 is not implemented and returns an empty metrics.
// nolint
func (prw *prometheusRemoteWriteReceiver) translateV2(_ context.Context, v2r *writev2.Request) (pmetric.Metrics, promremote.WriteResponseStats, error) {
	stats := promremote.WriteResponseStats{}
	if v2r == nil {
		return pmetric.NewMetrics(), stats, errors.New("empty request")
	}

	for idx, sym := range v2r.Symbols {
		prw.settings.Logger.Info("Symbol", zap.Int("index", idx), zap.String("symbol", sym))
	}

	m := pmetric.NewMetrics()

	for _, ts := range v2r.Timeseries {
		labels := derefLabels(ts.LabelsRefs, v2r.Symbols)
		name := labels["__name__"]
		if name == "" {
			prw.settings.Logger.Warn("Missing metric name")
			continue
		}
		delete(labels, "__name__")

		switch ts.Metadata.Type {
		case writev2.Metadata_METRIC_TYPE_COUNTER:
			rm := m.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			m.SetName(name)
			for _, sample := range ts.Samples {
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp)))
				dp.SetDoubleValue(sample.Value)
			}
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_GAUGE:
			rm := m.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			g := m.SetEmptyGauge()
			m.SetName(name)
			for _, sample := range ts.Samples {
				dp := g.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp)))
				dp.SetDoubleValue(sample.Value)
			}
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_SUMMARY:
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
			}
		case writev2.Metadata_METRIC_TYPE_HISTOGRAM:
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
			}
		case writev2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM:
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
			}
		case writev2.Metadata_METRIC_TYPE_UNSPECIFIED:
			// ignore
		default:
			prw.settings.Logger.Warn("Unknown metric type", zap.Any("type", ts.Metadata.Type))
		}
		stats.Exemplars += len(ts.Exemplars)
	}
	return m, stats, nil
}

func safeSymbol(symbols []string, index uint32) string {
	if int(index) < len(symbols) {
		return symbols[index]
	}
	return ""
}

func derefLabels(labelsRefs []uint32, symbols []string) map[string]string {
	labels := map[string]string{}

	// Ensure labelsRefs has an even number of entries
	if len(labelsRefs)%2 != 0 {
		return labels
	}

	for i := 0; i < len(labelsRefs); i += 2 {
		key := safeSymbol(symbols, labelsRefs[i])
		value := safeSymbol(symbols, labelsRefs[i+1])

		if key == "" || value == "" {
			continue
		}

		labels[key] = value
	}
	return labels
}
