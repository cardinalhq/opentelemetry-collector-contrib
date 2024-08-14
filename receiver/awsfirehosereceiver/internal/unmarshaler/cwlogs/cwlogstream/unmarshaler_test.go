// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cwlogstream

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestType(t *testing.T) {
	unmarshaler := NewUnmarshaler(zap.NewNop())
	require.Equal(t, TypeStr, unmarshaler.Type())
}

func TestUnmarshal(t *testing.T) {
	unmarshaler := NewUnmarshaler(zap.NewNop())
	testCases := map[string]struct {
		filename           string
		wantResourceCount  int
		wantLogRecordCount int
		wantErr            error
	}{
		"WithValidRecord": {
			filename:           "valid_record",
			wantResourceCount:  1,
			wantLogRecordCount: 3,
		},
		"WithInvalidRecords": {
			filename: "invalid_record",
			wantErr:  errInvalidRecords,
		},
		"Compressed record": {
			filename:           "compressed_record.gz",
			wantResourceCount:  1,
			wantLogRecordCount: 3,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			record, err := os.ReadFile(filepath.Join(".", "testdata", testCase.filename))
			require.NoError(t, err)

			records := [][]byte{record}

			got, err := unmarshaler.Unmarshal(records)
			if testCase.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, testCase.wantErr, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Equal(t, testCase.wantResourceCount, got.ResourceLogs().Len())
				gotLogRecordCount := 0
				for i := 0; i < got.ResourceLogs().Len(); i++ {
					rm := got.ResourceLogs().At(i)
					require.Equal(t, 1, rm.ScopeLogs().Len())
					ilm := rm.ScopeLogs().At(0)
					gotLogRecordCount += ilm.LogRecords().Len()
				}
				require.Equal(t, testCase.wantLogRecordCount, gotLogRecordCount)
			}
		})
	}
}
