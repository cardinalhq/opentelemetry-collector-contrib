// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"io"

	"github.com/golang/snappy"
)

func newSnappyReader(r io.ReadCloser) io.ReadCloser {
	return snappyReader{orig: r}
}

type snappyReader struct {
	orig io.ReadCloser
}

func (sr snappyReader) Read(p []byte) (n int, err error) {
	in, err := io.ReadAll(sr.orig)
	if err != nil {
		return 0, err
	}
	decoded, err := snappy.Decode(nil, in)
	if err != nil {
		return 0, err
	}
	copy(p, decoded)
	return len(decoded), nil
}

func (sr snappyReader) Close() error {
	return sr.orig.Close()
}
