// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"strings"
	"time"

	writev1 "github.com/prometheus/prometheus/prompb"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (prw *prometheusRemoteWriteReceiver) translateV1(_ context.Context, v1r *writev1.WriteRequest) (pmetric.Metrics, promremote.WriteResponseStats, error) {
	stats := promremote.WriteResponseStats{}
	metrics := pmetric.NewMetrics()
	if v1r == nil {
		return metrics, stats, errors.New("empty request")
	}

	resourceMetricCache := make(map[string]int)

	// make a list of all metric names we found.  We will go through and
	// attempt to classify each as a gauge, counter, histogram, or summary.
	// If we can't classify it, we'll just leave it as a gauge.
	metricNames := map[string]interface{}{}
	for _, ts := range v1r.Timeseries {
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metricNames[strings.ReplaceAll(label.Value, "_", ".")] = nil
				break
			}
		}
	}

	for metricName := range metricNames {
		// pull off the trailing _count, _sum, _bucket, or _quantile if any.
		if strings.HasSuffix(metricName, ".count") {
			delete(metricNames, metricName)
		}
		if strings.HasSuffix(metricName, ".sum") {
			delete(metricNames, metricName)
		}
		if strings.HasSuffix(metricName, ".bucket") {
			delete(metricNames, metricName)
		}
		if strings.HasSuffix(metricName, ".quantile") {
			delete(metricNames, metricName)
		}
	}

	for _, ts := range v1r.Timeseries {
		labels := map[string]string{}
		for _, label := range ts.Labels {
			name := strings.ReplaceAll(label.Name, "_", ".")
			labels[name] = label.Value
		}

		name := getName(labels)
		if name == "" {
			prw.settings.Logger.Warn("Missing metric name")
			continue
		}

		// If we don't want to keep this metric, drop it here.
		if _, found := metricNames[name]; !found {
			continue
		}

		m := getMetric(labels, metrics, resourceMetricCache)
		m.SetName(name)

		if strings.HasSuffix(name, ".total") {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, sample := range ts.Samples {
				dp := sum.DataPoints().AppendEmpty()
				startTs := pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp - 10000))
				milli := pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp))
				dp.SetStartTimestamp(startTs)
				dp.SetTimestamp(milli)
				dp.SetDoubleValue(sample.Value)
				setAttributes(labels, dp.Attributes())
			}
		} else {
			gauge := m.SetEmptyGauge()
			for _, sample := range ts.Samples {
				dp := gauge.DataPoints().AppendEmpty()
				milli := pcommon.NewTimestampFromTime(time.UnixMilli(sample.Timestamp))
				dp.SetStartTimestamp(milli)
				dp.SetTimestamp(milli)
				dp.SetDoubleValue(sample.Value)
				setAttributes(labels, dp.Attributes())
			}
		}
		stats.Samples += len(ts.Samples)
	}

	return metrics, stats, nil
}
