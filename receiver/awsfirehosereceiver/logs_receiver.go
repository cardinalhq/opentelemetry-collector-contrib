// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
)

// The metricsConsumer implements the firehoseConsumer
// to use a metrics consumer and unmarshaler.
type logsConsumer struct {
	// consumer passes the translated logs on to the
	// next consumer.
	consumer consumer.Logs
	// unmarshaler is the configured LogsUnmarshaler
	// to use when processing the records.
	unmarshaler unmarshaler.LogsUnmarshaler
}

var _ firehoseConsumer = (*logsConsumer)(nil)

// newMetricsReceiver creates a new instance of the receiver
// with a metricsConsumer.
func newLogsReceiver(
	config *LogsConfig,
	unmarshalers map[string]unmarshaler.LogsUnmarshaler,
	nextConsumer consumer.Logs,
) (*logsConsumer, error) {
	configuredUnmarshaler := unmarshalers[config.RecordType]
	if configuredUnmarshaler == nil {
		return nil, errUnrecognizedRecordType
	}
	lc := &logsConsumer{
		consumer:    nextConsumer,
		unmarshaler: configuredUnmarshaler,
	}
	return lc, nil
}

// Consume uses the configured unmarshaler to deserialize the records into a
// single pmetric.Metrics. If there are common attributes available, then it will
// attach those to each of the pcommon.Resources. It will send the final result
// to the next consumer.
func (mc *logsConsumer) Consume(ctx context.Context, records [][]byte, commonAttributes map[string]string) (int, error) {
	md, err := mc.unmarshaler.Unmarshal(records)
	if err != nil {
		return http.StatusBadRequest, err
	}

	if commonAttributes != nil {
		applyCommonLogAttributes(md, commonAttributes)
	}

	err = mc.consumer.ConsumeLogs(ctx, md)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func applyCommonLogAttributes(md plog.Logs, commonAttributes map[string]string) {
	for i := 0; i < md.ResourceLogs().Len(); i++ {
		rm := md.ResourceLogs().At(i)
		for k, v := range commonAttributes {
			if _, found := rm.Resource().Attributes().Get(k); !found {
				rm.Resource().Attributes().PutStr(k, v)
			}
		}
	}
}

func (mc *logsConsumer) RecordType() string {
	return mc.unmarshaler.Type()
}

func (mc *logsConsumer) TelemetryType() string {
	return "logs"
}
