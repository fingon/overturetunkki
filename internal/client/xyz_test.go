package client

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fingon/overturetunkki/internal/tilequery"
	"gotest.tools/v3/assert"
)

func TestRequestSquareUsesVersionAndETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		assert.Equal(t, request.URL.Path, TilePathPrefix+"xyz/10/583/296")
		assert.Equal(t, request.URL.Query().Get("catalog_version"), "version")
		assert.Equal(t, request.Header.Get("If-None-Match"), `"sha256:test"`)
		responseWriter.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	httpClient, err := New(Options{ServerURL: server.URL})
	assert.NilError(t, err)
	response, err := httpClient.RequestSquare(t.Context(), tilequery.Square{Z: 10, X: 583, Y: 296}, "version", `"sha256:test"`)
	assert.NilError(t, err)
	assert.Equal(t, response.StatusCode, http.StatusNotModified)
	assert.NilError(t, CloseResponse(response))
}
