package cteventstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlog/compression"
)

const (
	TypeStr = "ctevents"
)

var (
	errInvalidRecords = errors.New("record format invalid")
)

type Unmarshaler struct {
	logger *zap.Logger
}

var _ unmarshaler.LogsUnmarshaler = (*Unmarshaler)(nil)

// NewUnmarshaler creates a new instance of the Unmarshaler.
func NewUnmarshaler(logger *zap.Logger) *Unmarshaler {
	return &Unmarshaler{logger}
}

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
			record, err = compression.Unzip(record)
			if err != nil {
				u.logger.Error(
					"Unable to inflate input",
					zap.Error(err),
					zap.Int("record_index", recordIndex),
				)
				continue
			}
		}

		var log cwlog.CWLog
		err := json.Unmarshal(record, &log)
		if err != nil {
			u.logger.Error(
				"Unable to unmarshal input",
				zap.Error(err),
				zap.Int("record_index", recordIndex),
			)
			continue
		}
		if !cwlog.IsValid(log) {
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
		rattr.PutStr("aws.cloudwatch.log_stream_name", log.LogStream)
		rattr.PutStr("aws.cloudwatch.log_group_name", log.LogGroup)
		sl := rl.ScopeLogs().AppendEmpty()

		for _, r := range log.LogEvents {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(r.Message)
			appendToLogRecord(u.logger, &lr, r.Message)

			lr.SetSeverityNumber(plog.SeverityNumberUnspecified) // TODO try to find this too
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(r.Timestamp)))

		}
	}

	if logs.ResourceLogs().Len() == 0 {
		return logs, errInvalidRecords
	}
	return logs, nil
}

func appendToLogRecord(logger *zap.Logger, lr *plog.LogRecord, message string) {
	event, err := validateEvent([]byte(message))
	if err != nil {
		logger.Error(
			"Failed to validate event",
			zap.Error(err),
		)
	}

	makeLogEventName(lr, event.EventName)
	lr.Attributes().PutStr("aws.cloudtrail.event_type", event.EventType)
	lr.Attributes().PutStr("aws.cloudtrail.event_name", event.EventName)
	lr.Attributes().PutStr("aws.cloudtrail.event_version", event.EventVersion)
	lr.Attributes().PutStr("aws.cloudtrail.event_source", event.EventSource)
}

func makeLogEventName(lr *plog.LogRecord, eventName string) {
	logEventName := "aws.cloudtrail"
	if eventName != "" {
		logEventName = fmt.Sprintf("aws.cloudtrail.%s", strings.ToLower(eventName))
	}

	lr.Attributes().PutStr("event.name", logEventName)
}

func validateEvent(message []byte) (Event, error) {
	var event Event
	err := json.Unmarshal(message, &event)
	if err != nil {
		return Event{}, err
	}

	//TODO: perform further validations and also revisit error
	if event.EventVersion == "" || event.EventType == "" || event.EventSource == "" || event.EventName == "" {
		return Event{}, fmt.Errorf("some of the fields are missing in the event data")
	}

	return event, nil
}

// Type of the serialized messages.
func (u Unmarshaler) Type() string {
	return TypeStr
}
