// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promremote "github.com/prometheus/prometheus/storage/remote"
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

	for _, ts := range v2r.Timeseries {
		labels := derefLabels(ts.LabelsRefs, v2r.Symbols)
		prw.settings.Logger.Info("Timeseries",
			zap.Any("labels", labels),
			zap.Any("samples", ts.Samples),
			zap.Any("exemplars", ts.Exemplars),
			zap.Any("histograms", ts.Histograms))

		switch ts.Metadata.Type {
		case writev2.Metadata_METRIC_TYPE_COUNTER:
			stats.Samples += len(ts.Samples)
		case writev2.Metadata_METRIC_TYPE_GAUGE:
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
		default:
			prw.settings.Logger.Warn("Unknown metric type", zap.Any("type", ts.Metadata.Type))
		}
		stats.Exemplars += len(ts.Exemplars)
	}
	return pmetric.NewMetrics(), stats, nil
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
