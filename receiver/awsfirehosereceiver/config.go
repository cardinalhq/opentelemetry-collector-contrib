// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"errors"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	// ServerConfig is used to set up the Firehose delivery
	// endpoint. The Firehose delivery stream expects an HTTPS
	// endpoint, so TLSSettings must be used to enable that.
	confighttp.ServerConfig `mapstructure:",squash"`
	// AccessKey is checked against the one received with each request.
	// This can be set when creating or updating the Firehose delivery
	// stream.
	AccessKey configopaque.String `mapstructure:"access_key"`
	Metrics   []MetricsConfig     `mapstructure:"metrics"`
	Logs      []LogsConfig        `mapstructure:"logs"`
}

type MetricsConfig struct {
	// Path is the path to the endpoint that the Firehose delivery
	// stream will send requests to for this record type.
	Path string `mapstructure:"path"`
	// RecordType is the key used to determine which unmarshaler to use
	// when receiving the requests.
	RecordType string `mapstructure:"record_type"`
}

type LogsConfig struct {
	// Path is the path to the endpoint that the Firehose delivery
	// stream will send requests to for this record type.
	Path string `mapstructure:"path"`
	// RecordType is the key used to determine which unmarshaler to use
	// when receiving the requests.
	RecordType string `mapstructure:"record_type"`
}

// Validate checks that the endpoint and record type exist and
// are valid.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("must specify endpoint")
	}

	for _, metric := range c.Metrics {
		if err := metric.Validate(); err != nil {
			return err
		}
	}

	for _, log := range c.Logs {
		if err := log.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks that the path, record type, and name prefixes
// exist and are valid.
func (c *MetricsConfig) Validate() error {
	if c.Path == "" {
		return errors.New("must specify path")
	}
	if c.Path[0] != '/' {
		return errors.New("path must start with /")
	}
	if c.RecordType == "" {
		return errors.New("must specify record type")
	}
	if err := validateMetricRecordType(c.RecordType); err != nil {
		return err
	}

	return nil
}

// Validate checks that the path and record type exist and are valid.
func (c *LogsConfig) Validate() error {
	if c.Path == "" {
		return errors.New("must specify path")
	}
	if c.Path[0] != '/' {
		return errors.New("path must start with /")
	}
	if c.RecordType == "" {
		return errors.New("must specify record type")
	}
	if err := validateLogRecordType(c.RecordType); err != nil {
		return err
	}

	return nil
}
