// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cwlogstream // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogstream"

// The cWLog is the format for the CloudWatch log stream records.
type cWLog struct {
	LogStreamName       string       `json:"logStream"`
	LogGroupName        string       `json:"logGroup"`
	Owner               string       `json:"owner"`
	SubscriptionFilters []string     `json:"subscriptionFilters"`
	MessageType         string       `json:"messageType"`
	LogEvents           []cWLogEvent `json:"logEvents"`
}

// The cWLogEvent is the format for the CloudWatch log stream log events.
type cWLogEvent struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}
