// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"strings"
	"time"

	merger "github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics"
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

	ignoredSamples := 0
	ignoredHistograms := 0

	metrics := pmetric.NewMetrics()

	for _, ts := range v2r.Timeseries {
		labels := derefLabels(ts.LabelsRefs, v2r.Symbols)
		name := labels["..name.."]
		if name == "" {
			prw.settings.Logger.Warn("Missing metric name")
			continue
		}
		name = strings.ReplaceAll(name, "_", ".")
		delete(labels, "..name..")

		newMetrics := pmetric.NewMetrics()
		rm := newMetrics.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)

		units := safeSymbol(v2r.Symbols, ts.Metadata.UnitRef)
		if units != "" {
			m.SetUnit(units)
		}

		help := safeSymbol(v2r.Symbols, ts.Metadata.HelpRef)
		if help != "" {
			m.SetDescription(help)
		}

		switch ts.Metadata.Type {
		case writev2.Metadata_METRIC_TYPE_COUNTER, writev2.Metadata_METRIC_TYPE_UNSPECIFIED:
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, sample := range ts.Samples {
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp)))
				dp.SetDoubleValue(sample.Value)
				setAttributes(dp.Attributes(), labels)
			}
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_GAUGE:
			gauge := m.SetEmptyGauge()
			for _, sample := range ts.Samples {
				dp := gauge.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp)))
				dp.SetDoubleValue(sample.Value)
				setAttributes(dp.Attributes(), labels)
			}
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_SUMMARY:
			// TODO Implement summary
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
				ignoredHistograms += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
				ignoredSamples += len(ts.Samples)
			}
		case writev2.Metadata_METRIC_TYPE_HISTOGRAM:
			// TODO Implement histogram
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
				ignoredHistograms += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
				ignoredSamples += len(ts.Samples)
			}
		case writev2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM:
			// TODO Implement gaugehistogram
			if len(ts.Histograms) > 0 {
				stats.Samples += len(ts.Histograms)
				ignoredHistograms += len(ts.Histograms)
			} else {
				stats.Samples += len(ts.Samples)
				ignoredSamples += len(ts.Samples)
			}
		default:
			prw.settings.Logger.Warn("Unknown metric type", zap.Any("type", ts.Metadata.Type))
		}
		stats.Exemplars += len(ts.Exemplars)

		// Merge the new metrics into the existing metrics
		metrics = merger.Merge(metrics, newMetrics)
	}

	if ignoredSamples > 0 || ignoredHistograms > 0 {
		prw.settings.Logger.Warn("Ignoring samples", zap.Int("samples", ignoredSamples), zap.Int("histograms", ignoredHistograms))
	}

	return metrics, stats, nil
}

func setAttributes(attrs pcommon.Map, labels map[string]string) {
	for k, v := range labels {
		attrs.PutStr(k, v)
	}
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
		key := strings.ReplaceAll(safeSymbol(symbols, labelsRefs[i]), "_", ".")
		value := safeSymbol(symbols, labelsRefs[i+1])

		if key == "" || value == "" {
			continue
		}

		labels[key] = value
	}
	return labels
}
