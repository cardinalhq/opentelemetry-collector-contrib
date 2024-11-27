// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"

	writev1 "github.com/prometheus/prometheus/prompb"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func (prw *prometheusRemoteWriteReceiver) translateV1(_ context.Context, v1r *writev1.WriteRequest) (pmetric.Metrics, promremote.WriteResponseStats, error) {
	stats := promremote.WriteResponseStats{}
	if v1r == nil {
		return pmetric.NewMetrics(), stats, errors.New("empty request")
	}

	for _, ts := range v1r.Timeseries {
		labels := ts.Labels
		prw.settings.Logger.Info("Timeseries",
			zap.Any("labels", labels),
			zap.Any("samples", ts.Samples))

		stats.Samples += len(ts.Samples)
	}

	for _, md := range v1r.Metadata {
		prw.settings.Logger.Info("Metadata",
			zap.String("metricFamileName", md.MetricFamilyName),
			zap.String("help", md.Help),
			zap.Any("type", md.Type),
			zap.Any("unit", md.Unit))
	}

	return pmetric.NewMetrics(), stats, nil
}
