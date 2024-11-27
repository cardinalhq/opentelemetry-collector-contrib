// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	promconfig "github.com/prometheus/prometheus/config"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promremote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newRemoteWriteReceiver(settings receiver.Settings, cfg *Config, nextConsumer consumer.Metrics) (receiver.Metrics, error) {
	return &prometheusRemoteWriteReceiver{
		settings:     settings,
		nextConsumer: nextConsumer,
		config:       cfg,
		server: &http.Server{
			ReadTimeout: 60 * time.Second,
		},
	}, nil
}

type prometheusRemoteWriteReceiver struct {
	settings     receiver.Settings
	nextConsumer consumer.Metrics

	config *Config
	server *http.Server
}

func (prw *prometheusRemoteWriteReceiver) Start(ctx context.Context, host component.Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", prw.handlePRW)
	var err error

	prw.server, err = prw.config.ToServer(ctx, host, prw.settings.TelemetrySettings, mux)
	if err != nil {
		return fmt.Errorf("failed to create server definition: %w", err)
	}
	listener, err := prw.config.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to create prometheus remote-write listener: %w", err)
	}

	go func() {
		if err := prw.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(fmt.Errorf("error starting prometheus remote-write receiver: %w", err)))
		}
	}()
	return nil
}

func (prw *prometheusRemoteWriteReceiver) Shutdown(ctx context.Context) error {
	if prw.server == nil {
		return nil
	}
	return prw.server.Shutdown(ctx)
}

func (prw *prometheusRemoteWriteReceiver) handlePRW(w http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		prw.settings.Logger.Warn("message received without Content-Type header, rejecting")
		http.Error(w, "Content-Type header is required", http.StatusUnsupportedMediaType)
		return
	}

	protoVersion := req.Header.Get("X-Prometheus-Remote-Write-Version")
	if protoVersion == "" {
		prw.settings.Logger.Warn("message received without X-Prometheus-Remote-Write-Version header, rejecting")
		http.Error(w, "X-Prometheus-Remote-Write-Version header is required", http.StatusUnsupportedMediaType)
		return
	}

	msgType, err := prw.parseProto(contentType, protoVersion)
	if err != nil {
		prw.settings.Logger.Warn("Error decoding remote-write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}
	if msgType == promconfig.RemoteWriteProtoMsgV1 {
		prw.settings.Logger.Warn("possible v1 proto received, sending back 204")
		stats := promremote.WriteResponseStats{}
		stats.SetHeaders(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if msgType != promconfig.RemoteWriteProtoMsgV2 {
		prw.settings.Logger.Warn("message received with unsupported proto version, rejecting", zap.String("contentType", contentType))
		http.Error(w, "Unsupported proto version", http.StatusUnsupportedMediaType)
		return
	}

	// After parsing the content-type header, the next step would be to handle content-encoding.
	// Luckly confighttp's Server has middleware that already decompress the request body for us.

	body, err := io.ReadAll(req.Body)
	if err != nil {
		prw.settings.Logger.Warn("Error decoding remote write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var prw2Req writev2.Request
	if err = proto.Unmarshal(body, &prw2Req); err != nil {
		prw.settings.Logger.Warn("Error decoding remote write request", zapcore.Field{Key: "error", Type: zapcore.ErrorType, Interface: err})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m, stats, err := prw.translateV2(req.Context(), &prw2Req)
	stats.SetHeaders(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // Following instructions at https://prometheus.io/docs/specs/remote_write_spec_2_0/#invalid-samples
		return
	}

	if m.DataPointCount() > 0 {
		prw.nextConsumer.ConsumeMetrics(req.Context(), m)
	}

	w.WriteHeader(http.StatusNoContent)
}

// parseProto parses the content-type header and returns the version of the remote-write protocol.
// We can't expect that senders of remote-write v1 will add the "proto=" parameter since it was not
// a requirement in v1. So, if the parameter is not found, we assume v1.
func (prw *prometheusRemoteWriteReceiver) parseProto(contentType string, protoVersion string) (promconfig.RemoteWriteProtoMsg, error) {
	contentType = strings.TrimSpace(contentType)

	parts := strings.Split(contentType, ";")
	if parts[0] != "application/x-protobuf" {
		return "", fmt.Errorf("expected %q as the first (media) part, got %v content-type", "application/x-protobuf", contentType)
	}

	if strings.HasPrefix(protoVersion, "2.") {
		return promconfig.RemoteWriteProtoMsgV2, nil
	}

	// assume v1.
	return promconfig.RemoteWriteProtoMsgV1, nil
}

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
