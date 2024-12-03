// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.27.0"
	"go.uber.org/zap"
)

var resourceAttributeNameMap = map[string]string{
	"service":   semconv.AttributeServiceName,
	"job":       semconv.AttributeServiceName,
	"namespace": semconv.AttributeServiceNamespace,
	"pod":       semconv.AttributeK8SPodName,
	"node":      semconv.AttributeK8SNodeName,
}

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
	resourceMetricCache := make(map[string]int)

	for _, ts := range v2r.Timeseries {
		labels := derefLabels(ts.LabelsRefs, v2r.Symbols)
		name := labels["..name.."]

		if name == "" {
			prw.settings.Logger.Warn("Missing metric name")
			continue
		}
		name = strings.ReplaceAll(name, "_", ".")
		delete(labels, "..name..")

		m := getMetric(labels, metrics, resourceMetricCache)
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
				milli := pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp))
				dp.SetStartTimestamp(milli)
				dp.SetTimestamp(milli)
				dp.SetDoubleValue(sample.Value)
				setAttributes(labels, dp.Attributes())
			}
			stats.Samples += len(ts.Samples)
			if strings.Contains(name, "rss") {
				prw.settings.Logger.Info("Created sum metric", zap.String("name", name), zap.Any("type", ts.Metadata.Type), zap.Bool("monotonic", sum.IsMonotonic()), zap.Any("temporality", sum.AggregationTemporality()))
			}
		case writev2.Metadata_METRIC_TYPE_GAUGE:
			gauge := m.SetEmptyGauge()
			for _, sample := range ts.Samples {
				dp := gauge.DataPoints().AppendEmpty()
				milli := pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp))
				dp.SetStartTimestamp(milli)
				dp.SetTimestamp(milli)
				dp.SetDoubleValue(sample.Value)
				setAttributes(labels, dp.Attributes())
			}
			stats.Samples += len(ts.Samples)
			if strings.Contains(name, "rss") {
				prw.settings.Logger.Info("Created gauge metric", zap.String("name", name), zap.Any("type", ts.Metadata.Type))
			}
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
	}

	if ignoredSamples > 0 || ignoredHistograms > 0 {
		prw.settings.Logger.Debug("Ignoring samples", zap.Int("samples", ignoredSamples), zap.Int("histograms", ignoredHistograms))
	}

	return metrics, stats, nil
}

func setAttributes(labels map[string]string, iattr pcommon.Map) {
	for k, v := range labels {
		if _, ok := resourceAttributeNameMap[k]; !ok {
			iattr.PutStr(k, v)
		}
	}
}

func safeSymbol(symbols []string, index uint32) string {
	if int(index) < len(symbols) {
		return symbols[index]
	}
	return ""
}

func derefLabels(labelsRefs []uint32, symbols []string) map[string]string {
	labels := make(map[string]string)

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

func getMetric(
	labels map[string]string,
	metrics pmetric.Metrics,
	resourceMetricCache map[string]int,
) pmetric.Metric {
	var parts []string
	resourceAttributes := pcommon.NewMap()
	for promName, semanticName := range resourceAttributeNameMap {
		if value, ok := labels[promName]; ok {
			resourceAttributes.PutStr(semanticName, value)
			parts = append(parts, fmt.Sprintf("%s=%s", semanticName, value))
		}
	}
	slices.Sort(parts)
	resourceID := strings.Join(parts, ":")

	idx, ok := resourceMetricCache[resourceID]
	if !ok {
		idx = metrics.ResourceMetrics().Len()
		rm := metrics.ResourceMetrics().AppendEmpty()
		resourceAttributes.CopyTo(rm.Resource().Attributes())
		rm.ScopeMetrics().AppendEmpty()
		resourceMetricCache[resourceID] = idx
	}

	return metrics.ResourceMetrics().At(idx).ScopeMetrics().At(0).Metrics().AppendEmpty()
}
