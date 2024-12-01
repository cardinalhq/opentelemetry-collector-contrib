// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	resourceMetricCache := make(map[string]pmetric.ResourceMetrics)
	scopeMetricCache := make(map[string]pmetric.ScopeMetrics)

	for _, ts := range v2r.Timeseries {
		labels := derefLabels(ts.LabelsRefs, v2r.Symbols)
		name := labels["..name.."]
		if name == "" {
			prw.settings.Logger.Warn("Missing metric name")
			continue
		}
		name = strings.ReplaceAll(name, "_", ".")
		delete(labels, "..name..")

		m := getMetric(labels, metrics, resourceMetricCache, scopeMetricCache)
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
				setAttributes(labels, dp.Attributes())
			}
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_GAUGE:
			gauge := m.SetEmptyGauge()
			for _, sample := range ts.Samples {
				dp := gauge.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp)))
				dp.SetDoubleValue(sample.Value)
				setAttributes(labels, dp.Attributes())
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
	resourceMetricCache map[string]pmetric.ResourceMetrics,
	scopeMetricCache map[string]pmetric.ScopeMetrics,
) pmetric.Metric {

	var parts []string
	resourceAttributes := make(map[string]string)
	for promName, semanticName := range resourceAttributeNameMap {
		if value, ok := labels[promName]; ok {
			resourceAttributes[semanticName] = value
			parts = append(parts, fmt.Sprintf("%s=%s", semanticName, value))
		}
	}
	sort.Strings(parts)
	resourceID := strings.Join(parts, ":")

	var rm pmetric.ResourceMetrics
	if cachedRM, ok := resourceMetricCache[resourceID]; ok {
		rm = cachedRM
	} else {
		rm = metrics.ResourceMetrics().AppendEmpty()
		for k, v := range resourceAttributes {
			rm.Resource().Attributes().PutStr(k, v)
		}
		resourceMetricCache[resourceID] = rm
	}

	// We index ScopeMetrics with the resourceID itself because there are no discernible scope attributes
	// that we know of coming on the prometheus remote write request.
	var sm pmetric.ScopeMetrics
	if cachedSM, ok := scopeMetricCache[resourceID]; ok {
		sm = cachedSM
	} else {
		sm = rm.ScopeMetrics().AppendEmpty()
		scopeMetricCache[resourceID] = sm
	}

	return sm.Metrics().AppendEmpty()
}
