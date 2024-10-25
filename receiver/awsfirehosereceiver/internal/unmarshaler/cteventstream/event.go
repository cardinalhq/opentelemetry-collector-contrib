// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cteventstream // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cweventstream"

import "time"

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
