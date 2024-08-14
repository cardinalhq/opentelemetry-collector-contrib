// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/localhostgate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogs/cteventstream"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogs/cwlogstream"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwmetricstream"
)

const (
	defaultMetricsRecordType    = cwmetricstream.TypeStr
	defaultLogsRecordType       = cwlogstream.TypeStr
	defaultCloudTrailRecordType = cteventstream.TypeStr
	defaultEndpoint             = "0.0.0.0:4433"
	defaultPort                 = 4433
)

var (
	errUnrecognizedRecordType  = errors.New("unrecognized record type")
	errDuplicatePath           = errors.New("duplicate path")
	availableMetricRecordTypes = map[string]bool{
		cwmetricstream.TypeStr: true,
	}
	availableLogRecordTypes = map[string]bool{
		cwlogstream.TypeStr:   true,
		cteventstream.TypeStr: true,
	}
	receivers = sharedcomponent.NewSharedComponents()
)

// NewFactory creates a receiver factory for awsfirehose. Currently, only
// available in metrics pipelines.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability))
}

// validateMetricRecordType checks the available record types for the
// passed in one and returns an error if not found.
func validateMetricRecordType(recordType string) error {
	if _, ok := availableMetricRecordTypes[recordType]; !ok {
		return errUnrecognizedRecordType
	}
	return nil
}

// validateLogRecordType checks the available record types for the
// passed in one and returns an error if not found.
func validateLogRecordType(recordType string) error {
	if _, ok := availableLogRecordTypes[recordType]; !ok {
		return errUnrecognizedRecordType
	}
	return nil
}

// defaultMetricsUnmarshalers creates a map of the available metrics
// unmarshalers.
func defaultMetricsUnmarshalers(logger *zap.Logger) map[string]unmarshaler.MetricsUnmarshaler {
	cwmsu := cwmetricstream.NewUnmarshaler(logger)
	return map[string]unmarshaler.MetricsUnmarshaler{
		cwmsu.Type(): cwmsu,
	}
}

// defaultMetricsUnmarshalers creates a map of the available metrics
// unmarshalers.
func defaultLogsUnmarshalers(logger *zap.Logger) map[string]unmarshaler.LogsUnmarshaler {
	cwmlogs := cwlogstream.NewUnmarshaler(logger)
	ctevents := cteventstream.NewUnmarshaler(logger)
	return map[string]unmarshaler.LogsUnmarshaler{
		cwmlogs.Type():  cwmlogs,
		ctevents.Type(): ctevents,
	}
}

// createDefaultConfig creates a default config with the endpoint set
// to port 8443 and the record type set to the CloudWatch metric stream.
func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: localhostgate.EndpointForPort(defaultPort),
		},
		Metrics: []MetricsConfig{
			{
				Path:       "/metrics",
				RecordType: defaultMetricsRecordType,
			},
		},
		Logs: []LogsConfig{
			{
				Path:       "/logs",
				RecordType: defaultLogsRecordType,
			},
			{
				Path:       "/cloudtrail",
				RecordType: defaultCloudTrailRecordType,
			},
		},
	}
}

// createMetricsReceiver implements the CreateMetricsReceiver function type.
func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	rcfg := cfg.(*Config)
	fhr, err := getSharedReceiver(set.ID.String(), set, rcfg)
	if err != nil {
		return nil, err
	}
	if err := fhr.setMetricsConsumer(defaultMetricsUnmarshalers(set.Logger), nextConsumer); err != nil {
		return nil, err
	}
	return fhr, nil
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	rcfg := cfg.(*Config)
	fhr, err := getSharedReceiver(set.ID.String(), set, rcfg)
	if err != nil {
		return nil, err
	}
	if err := fhr.setLogsConsumer(defaultLogsUnmarshalers(set.Logger), nextConsumer); err != nil {
		return nil, err
	}
	return fhr, nil
}

func getSharedReceiver(key any, set receiver.Settings, cfg *Config) (*firehoseReceiver, error) {
	var err error
	r := receivers.GetOrAdd(key, func() (rr component.Component) {
		rr, err = newFirehoseReceiver(cfg, set)
		return rr
	})
	if err != nil {
		return nil, err
	}
	return r.Unwrap().(*firehoseReceiver), nil
}
