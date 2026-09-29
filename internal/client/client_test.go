package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"gotest.tools/v3/assert"
)

func TestClientRequestBuildsBoundedEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		assert.Equal(t, request.Method, http.MethodGet)
		assert.Equal(t, request.URL.Path, "/service/v1/catalog")
		assert.Equal(t, request.URL.Query().Get("x"), "1")
		assert.Equal(t, request.Header.Get("X-Test"), "value")
		responseWriter.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	httpClient, err := New(Options{ServerURL: server.URL + "/service/"})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	response, err := httpClient.Request(context.Background(), http.MethodGet, "/v1/catalog", url.Values{"x": []string{"1"}}, http.Header{"X-Test": []string{"value"}})
	assert.NilError(t, err)
	if response != nil {
		assert.NilError(t, CloseResponse(response))
	}
}

func TestClientRequestValidatesInputs(t *testing.T) {
	httpClient, err := New(Options{ServerURL: "http://localhost:8080"})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cases := []struct {
		name string
		ctx  context.Context
		path string
	}{
		{name: "nil context", path: "/v1/catalog"},
		{name: "empty path", ctx: context.Background()},
		{name: "relative path", ctx: context.Background(), path: "v1/catalog"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := httpClient.Request(test.ctx, http.MethodGet, test.path, nil, nil)
			assert.Assert(t, err != nil)
		})
	}
}

func TestReadBodyEnforcesLimitAndCloses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
		_, err := responseWriter.Write([]byte("12345"))
		assert.NilError(t, err)
	}))
	defer server.Close()
	httpClient, err := New(Options{ServerURL: server.URL})
	assert.NilError(t, err)
	if err != nil {
		return
	}
	response, err := httpClient.Request(context.Background(), http.MethodGet, "/", nil, nil)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	_, err = ReadBody(response, 4)
	assert.ErrorContains(t, err, "exceeds")
}
