// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cwlogstream // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogstream"

import (
	"encoding/json"
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogs"
)

const (
	TypeStr = "cwlogs"
)

var (
	errInvalidRecords = errors.New("record format invalid")
)

// Unmarshaler for the CloudWatch Log Stream JSON record format.
//
// More details can be found at:
// https://docs.aws.amazon.com/firehose/latest/dev/writing-with-cloudwatch-logs.html
type Unmarshaler struct {
	logger *zap.Logger
}

var _ unmarshaler.LogsUnmarshaler = (*Unmarshaler)(nil)

// NewUnmarshaler creates a new instance of the Unmarshaler.
func NewUnmarshaler(logger *zap.Logger) *Unmarshaler {
	return &Unmarshaler{logger}
}

// Unmarshal deserializes the records into cWLogs and uses the
// resourceLogsBuilder to group them into a single plog.Logs.
// Skips invalid cWLogs received in the record and
func (u Unmarshaler) Unmarshal(records [][]byte) (plog.Logs, error) {
	logs := plog.NewLogs()
	for recordIndex, record := range records {
		if len(record) < 2 {
			u.logger.Error(
				"Invalid record (too short)",
				zap.Int("record_index", recordIndex),
			)
			continue
		}
		if record[0] == 0x1f && record[1] == 0x8b {
			var err error
			record, err = cwlogs.Decompress(record)
			if err != nil {
				u.logger.Error(
					"Unable to inflate input",
					zap.Error(err),
					zap.Int("record_index", recordIndex),
				)
				continue
			}
		}
		var log cwlogs.CWLog
		err := json.Unmarshal(record, &log)
		if err != nil {
			u.logger.Error(
				"Unable to unmarshal input",
				zap.Error(err),
				zap.Int("record_index", recordIndex),
			)
			continue
		}
		if !cwlogs.IsValid(log) {
			u.logger.Error(
				"Invalid log",
				zap.Int("record_index", recordIndex),
			)
			continue
		}

		rl := logs.ResourceLogs().AppendEmpty()
		rattr := rl.Resource().Attributes()
		rattr.PutStr(conventions.AttributeCloudProvider, conventions.AttributeCloudProviderAWS)
		rattr.PutStr(conventions.AttributeCloudAccountID, log.Owner)
		rattr.PutStr("aws.cloudwatch.log_stream_name", log.LogStreamName)
		rattr.PutStr("aws.cloudwatch.log_group_name", log.LogGroupName)
		sl := rl.ScopeLogs().AppendEmpty()

		for _, r := range log.LogEvents {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(r.Message)
			lr.SetSeverityNumber(plog.SeverityNumberUnspecified) // TODO try to find this too
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(r.Timestamp)))
		}
	}

	if logs.ResourceLogs().Len() == 0 {
		return logs, errInvalidRecords
	}

	return logs, nil
}

// Type of the serialized messages.
func (u Unmarshaler) Type() string {
	return TypeStr
}
