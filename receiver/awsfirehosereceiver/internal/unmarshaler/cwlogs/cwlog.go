// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cwlogs // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogstream"

import (
	"bytes"
	"compress/gzip" // The cWLog is the format for the CloudWatch log stream records.
	"io"
)

type CWLog struct {
	LogStreamName       string       `json:"logStream"`
	LogGroupName        string       `json:"logGroup"`
	Owner               string       `json:"owner"`
	SubscriptionFilters []string     `json:"subscriptionFilters"`
	MessageType         string       `json:"messageType"`
	LogEvents           []CWLogEvent `json:"logEvents"`
}

// The cWLogEvent is the format for the CloudWatch log stream log events.
type CWLogEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

// decompress handles a gzip'd payload.
func Decompress(data []byte) ([]byte, error) {
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

// IsValid validates that the cWLog has been unmarshalled correctly.
func IsValid(rec CWLog) bool {
	return rec.LogStreamName != "" &&
		rec.LogGroupName != "" &&
		rec.LogEvents != nil &&
		rec.Owner != ""
}
