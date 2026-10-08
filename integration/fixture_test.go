//nolint:goconst // Independent fixture cases retain their source identifiers.
package integration

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
)

func TestPartitionSourceFixtures(t *testing.T) {
	for _, test := range []struct {
		name string
		ids  []string
	}{
		{name: "san-francisco.parquet", ids: []string{"poi-2"}},
		{name: "helsinki.parquet", ids: []string{"poi-1"}},
		{name: "empty.parquet", ids: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.DeepEqual(t, tileIDs(t, fixtureFile(t, filepath.Join("testdata", test.name))), test.ids)
		})
	}
}

func TestFixtureServesPartialParquetReads(t *testing.T) {
	for _, test := range []struct {
		name, method, rangeHeader string
		status, bytes             int
	}{
		{name: "range", method: http.MethodGet, rangeHeader: "bytes=0-3", status: http.StatusPartialContent, bytes: 4},
		{name: "head", method: http.MethodHead, status: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &fixtureServer{t: t, mode: fixtureNormal, assetRequests: make(map[string]int), sanFranciscoBytes: fixtureFile(t, filepath.Join("testdata", "san-francisco.parquet"))}
			request := httptest.NewRequest(test.method, "https://example.com/release/test/theme=places/type=place/part-00002-test.zstd.parquet", nil)
			if test.rangeHeader != "" {
				request.Header.Set("Range", test.rangeHeader)
			}
			writer := httptest.NewRecorder()
			fixture.serveHTTP(writer, request)
			assert.Equal(t, writer.Code, test.status)
			assert.Equal(t, writer.Body.Len(), test.bytes)
			assert.Equal(t, fixture.assetTransferBytes, int64(test.bytes))
			assert.Equal(t, fixture.assetRequests[request.URL.Path], 1)
			if test.bytes > 0 {
				assert.Equal(t, writer.Body.String(), "PAR1")
			}
		})
	}
}
