// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cteventstream // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlogstream"

import "time" // The cWLog is the format for the CloudWatch log stream records.
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

type Event struct {
	EventVersion    string    `json:"eventVersion"`
	EventTime       time.Time `json:"eventTime"`
	EventSource     string    `json:"eventSource"`
	EventName       string    `json:"eventName"`
	SourceIPAddress string    `json:"sourceIPAddress"`
	UserAgent       string    `json:"userAgent"`
	EventID         string    `json:"eventID"`
	EventType       string    `json:"eventType"`
	ManagementEvent bool      `json:"managementEvent"`
	EventCategory   string    `json:"eventCategory"`
}
