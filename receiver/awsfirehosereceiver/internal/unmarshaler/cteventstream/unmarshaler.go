package cteventstream

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"
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
			record, err = decompress(record)
			if err != nil {
				u.logger.Error(
					"Unable to inflate input",
					zap.Error(err),
					zap.Int("record_index", recordIndex),
				)
				continue
			}
		}
		var log cWLog
		err := json.Unmarshal(record, &log)
		if err != nil {
			u.logger.Error(
				"Unable to unmarshal input",
				zap.Error(err),
				zap.Int("record_index", recordIndex),
			)
			continue
		}
		if !u.isValid(log) {
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
			appendToLogRecord(&lr, r.Message)

			lr.SetSeverityNumber(plog.SeverityNumberUnspecified) // TODO try to find this too
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.UnixMilli(r.Timestamp)))

		}
	}

	if logs.ResourceLogs().Len() == 0 {
		return logs, errInvalidRecords
	}

	return logs, nil
}

func appendToLogRecord(lr *plog.LogRecord, message string) {
	event, err := validateEvent([]byte(message))
	if err != nil {
		lr.Attributes().PutStr("event.name", "aws.cloudtrail")
	}

	eventType := fmt.Sprintf("aws.cloudtrail.%s", strings.ToLower(event.EventType))
	lr.Attributes().PutStr("event.name", eventType)
	lr.Attributes().PutStr("aws.cloudtrail.event_type", eventType)
	lr.Attributes().PutStr("aws.cloudtrail.event_version", event.EventVersion)
	lr.Attributes().PutStr("aws.cloudtrail.event_source", event.EventSource)

}

func validateEvent(message []byte) (Event, error) {
	var event Event
	err := json.Unmarshal(message, &event)
	if err != nil {
		return Event{}, err
	}

	//TODO: perform further validations
	if event.EventVersion != "" && event.EventType != "" && event.EventSource != "" {
		return event, nil
	}

	return Event{}, nil
}

// decompress handles a gzip'd payload.
func decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	uncompressedData, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return uncompressedData, nil
}

// isValid validates that the cWLog has been unmarshalled correctly.
func (u Unmarshaler) isValid(rec cWLog) bool {
	return rec.LogStreamName != "" &&
		rec.LogGroupName != "" &&
		rec.LogEvents != nil &&
		rec.Owner != ""
}

// Type of the serialized messages.
func (u Unmarshaler) Type() string {
	return TypeStr
}
