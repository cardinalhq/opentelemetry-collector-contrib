// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewritereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusremotewritereceiver"

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	promconfig "github.com/prometheus/prometheus/config"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestHandlePRWContentTypeNegotiation(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	prwReceiver, err := factory.CreateMetrics(context.Background(), receivertest.NewNopSettings(), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, prwReceiver, "metrics receiver creation failed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.NoError(t, prwReceiver.Start(ctx, componenttest.NewNopHost()))

	for _, tc := range []struct {
		name         string
		contentType  string
		version      promconfig.RemoteWriteProtoMsg
		extectedCode int
	}{
		{
			name:         "no content type",
			contentType:  "",
			version:      promconfig.RemoteWriteProtoMsgV2,
			extectedCode: http.StatusUnsupportedMediaType,
		},
		{
			name:         "unsupported content type",
			contentType:  "application/json",
			version:      promconfig.RemoteWriteProtoMsgV2,
			extectedCode: http.StatusUnsupportedMediaType,
		},
		{
			name:         "x-protobuf/no proto parameter",
			contentType:  "application/x-protobuf",
			version:      promconfig.RemoteWriteProtoMsgV2,
			extectedCode: http.StatusNoContent,
		},
		{
			name:         "x-protobuf/v1 proto parameter",
			contentType:  fmt.Sprintf("application/x-protobuf;proto=%s", promconfig.RemoteWriteProtoMsgV1),
			version:      promconfig.RemoteWriteProtoMsgV1,
			extectedCode: http.StatusNoContent,
		},
		{
			name:         "x-protobuf/v2 proto parameter",
			contentType:  fmt.Sprintf("application/x-protobuf;proto=%s", promconfig.RemoteWriteProtoMsgV2),
			version:      promconfig.RemoteWriteProtoMsgV2,
			extectedCode: http.StatusNoContent,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := writev2.Request{}
			pBuf := proto.NewBuffer(nil)
			err := pBuf.Marshal(&body)
			assert.NoError(t, err)

			var compressedBody []byte
			snappy.Encode(compressedBody, pBuf.Bytes())
			req, err := http.NewRequest(http.MethodPost, "http://localhost:9090/api/v1/write", bytes.NewBuffer(compressedBody))
			assert.NoError(t, err)

			req.Header.Set("Content-Type", tc.contentType)
			req.Header.Set("Content-Encoding", "snappy")
			req.Header.Set("X-Prometheus-Remote-Write-Version", string(tc.version))
			resp, err := http.DefaultClient.Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.extectedCode, resp.StatusCode)
			if tc.extectedCode == http.StatusNoContent { // We went until the end
				assert.NotEmpty(t, resp.Header.Get("X-Prometheus-Remote-Write-Samples-Written"))
				assert.NotEmpty(t, resp.Header.Get("X-Prometheus-Remote-Write-Histograms-Written"))
				assert.NotEmpty(t, resp.Header.Get("X-Prometheus-Remote-Write-Exemplars-Written"))
			}
		})
	}
	cancel()
	assert.NoError(t, prwReceiver.Shutdown(ctx), "Must not error shutting down")
}
