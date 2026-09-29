package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/mstenber/overturetunkki/internal/cache"
	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
)

const (
	contentTypeJSON      = "application/json; charset=utf-8"
	contentTypeParquet   = "application/vnd.apache.parquet"
	cacheControlFresh    = "no-cache, must-revalidate"
	releaseHeader        = "Overture-Release"
	catalogVersionHeader = "Overture-Catalog-Version"
	projectionHeader     = "Overture-Projection-ID"
)

type CatalogObserver interface {
	Refresh(context.Context) (catalog.Snapshot, error)
	Current() (catalog.Generation, error)
}

type TileProvider interface {
	Get(context.Context, catalog.Snapshot, h3.Cell) (Tile, error)
}

type Tile struct {
	Path      string
	SizeBytes int64
	Digest    string
}

type Options struct {
	Observer       CatalogObserver
	Provider       TileProvider
	Fields         []string
	MaxTileBytes   int64
	MaxTileRows    int64
	AttributionURL []string
}

type Server struct {
	observer CatalogObserver
	provider TileProvider
	fields   []string
	options  Options
	requests atomic.Uint64
}

type catalogResponse struct {
	Release                string   `json:"release"`
	CatalogVersion         string   `json:"catalog_version"`
	ProjectionID           string   `json:"projection_id"`
	Fields                 []string `json:"fields"`
	MaxTileBytes           int64    `json:"max_tile_bytes"`
	MaxTileRows            int64    `json:"max_tile_rows"`
	SupportedH3Resolutions []int    `json:"supported_h3_resolutions"`
	Attribution            []string `json:"attribution"`
}

type errorResponse struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	Retryable      bool   `json:"retryable"`
	Release        string `json:"release,omitempty"`
	CatalogVersion string `json:"catalog_version,omitempty"`
}

func New(options Options) (*Server, error) {
	if options.Observer == nil {
		return nil, fmt.Errorf("HTTP server observer is nil")
	}
	if options.Provider == nil {
		return nil, fmt.Errorf("HTTP server tile provider is nil")
	}
	if len(options.Fields) == 0 {
		return nil, fmt.Errorf("HTTP server fields must not be empty")
	}
	if options.MaxTileBytes <= 0 || options.MaxTileRows <= 0 {
		return nil, fmt.Errorf("HTTP server tile limits must be positive")
	}
	fields := append([]string(nil), options.Fields...)
	attribution := append([]string(nil), options.AttributionURL...)
	options.Fields = fields
	options.AttributionURL = attribution
	return &Server{observer: options.Observer, provider: options.Provider, fields: fields, options: options}, nil
}

func (server *Server) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	if server == nil {
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "HTTP server is unavailable"})
		return
	}
	server.requests.Add(1)
	switch {
	case request.URL.Path == "/v1/catalog":
		server.serveCatalog(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/v1/tiles/places/"):
		server.serveTile(responseWriter, request)
	case request.URL.Path == "/livez":
		server.serveLive(responseWriter, request)
	case request.URL.Path == "/readyz":
		server.serveReady(responseWriter, request)
	case request.URL.Path == "/metrics":
		server.serveMetrics(responseWriter, request)
	default:
		writeError(responseWriter, http.StatusNotFound, errorResponse{Code: "not_found", Message: "endpoint not found"})
	}
}

func (server *Server) serveCatalog(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	snapshot, err := server.refresh(request.Context())
	if err != nil {
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is unavailable", Retryable: true})
		return
	}
	server.writeVersionHeaders(responseWriter, snapshot)
	responseWriter.Header().Set("Cache-Control", cacheControlFresh)
	writeJSON(responseWriter, http.StatusOK, catalogResponse{
		Release:                snapshot.Release,
		CatalogVersion:         snapshot.CatalogVersion,
		ProjectionID:           snapshot.ProjectionID,
		Fields:                 append([]string(nil), server.fields...),
		MaxTileBytes:           server.options.MaxTileBytes,
		MaxTileRows:            server.options.MaxTileRows,
		SupportedH3Resolutions: supportedH3Resolutions(),
		Attribution:            append([]string(nil), server.options.AttributionURL...),
	})
}

func (server *Server) serveTile(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	version, err := requiredQueryValue(request, "catalog_version")
	if err != nil {
		writeError(responseWriter, http.StatusBadRequest, errorResponse{Code: "invalid_request", Message: err.Error()})
		return
	}
	cellText := strings.TrimPrefix(request.URL.Path, "/v1/tiles/places/")
	cell, err := worker.ParseCanonicalCell(cellText)
	if err != nil {
		writeError(responseWriter, http.StatusBadRequest, errorResponse{Code: "invalid_request", Message: "H3 cell is invalid"})
		return
	}
	snapshot, err := server.refresh(request.Context())
	if err != nil {
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is unavailable", Retryable: true})
		return
	}
	if version != snapshot.CatalogVersion {
		writeError(responseWriter, http.StatusConflict, errorResponse{Code: "catalog_changed", Message: "catalog version is stale", Release: snapshot.Release, CatalogVersion: snapshot.CatalogVersion})
		return
	}
	tile, err := server.provider.Get(request.Context(), snapshot, cell)
	if err != nil {
		status, response := classifyTileError(err)
		writeError(responseWriter, status, response)
		return
	}
	latest, err := server.refresh(request.Context())
	if err != nil {
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is unavailable", Retryable: true})
		return
	}
	if latest.CatalogVersion != snapshot.CatalogVersion || latest.Release != snapshot.Release {
		writeError(responseWriter, http.StatusConflict, errorResponse{Code: "catalog_changed", Message: "catalog changed before response", Release: latest.Release, CatalogVersion: latest.CatalogVersion})
		return
	}
	server.serveTileFile(responseWriter, request, snapshot, tile)
}

func (server *Server) serveTileFile(responseWriter http.ResponseWriter, request *http.Request, snapshot catalog.Snapshot, tile Tile) {
	if tile.Path == "" || tile.SizeBytes <= 0 || tile.Digest == "" || strings.ContainsAny(tile.Digest, "\"\r\n") {
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile metadata is invalid"})
		return
	}
	file, err := os.Open(tile.Path)
	if err != nil {
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile cannot be opened"})
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Error("close tile response file", "path", tile.Path, "error", closeErr)
		}
	}()
	fileInfo, err := file.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Size() != tile.SizeBytes {
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile metadata does not match file"})
		return
	}
	etag := `"` + tile.Digest + `"`
	server.writeVersionHeaders(responseWriter, snapshot)
	responseWriter.Header().Set("Content-Type", contentTypeParquet)
	responseWriter.Header().Set("Content-Length", strconv.FormatInt(tile.SizeBytes, 10))
	responseWriter.Header().Set("ETag", etag)
	responseWriter.Header().Set("Cache-Control", cacheControlFresh)
	if etagMatches(request.Header.Get("If-None-Match"), etag) {
		responseWriter.WriteHeader(http.StatusNotModified)
		return
	}
	responseWriter.WriteHeader(http.StatusOK)
	if _, err := io.CopyN(responseWriter, file, tile.SizeBytes); err != nil {
		slog.Error("stream tile response", "path", tile.Path, "error", err)
	}
}

func (server *Server) serveLive(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	writeJSON(responseWriter, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) serveReady(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	generation, err := server.observer.Current()
	if err != nil {
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is not ready", Retryable: true})
		return
	}
	writeJSON(responseWriter, http.StatusOK, map[string]any{"status": "ready", "generation": generation.Number, "catalog_version": generation.Snapshot.CatalogVersion})
}

func (server *Server) serveMetrics(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	ready := 0
	if _, err := server.observer.Current(); err == nil {
		ready = 1
	}
	responseWriter.Header().Set("Content-Type", "text/plain; version=0.0.4")
	responseWriter.WriteHeader(http.StatusOK)
	_, err := fmt.Fprintf(responseWriter, "# TYPE overture_http_requests_total counter\noverture_http_requests_total %d\n# TYPE overture_catalog_ready gauge\noverture_catalog_ready %d\n", server.requests.Load(), ready)
	if err != nil {
		slog.Error("write metrics response", "error", err)
	}
}

func (server *Server) refresh(ctx context.Context) (catalog.Snapshot, error) {
	return server.observer.Refresh(ctx)
}

func (server *Server) writeVersionHeaders(responseWriter http.ResponseWriter, snapshot catalog.Snapshot) {
	responseWriter.Header().Set(releaseHeader, snapshot.Release)
	responseWriter.Header().Set(catalogVersionHeader, snapshot.CatalogVersion)
	responseWriter.Header().Set(projectionHeader, snapshot.ProjectionID)
}

func requiredQueryValue(request *http.Request, name string) (string, error) {
	values, ok := request.URL.Query()[name]
	if !ok || len(values) != 1 || values[0] == "" {
		return "", fmt.Errorf("query parameter %q is required exactly once", name)
	}
	return values[0], nil
}

func classifyTileError(err error) (int, errorResponse) {
	switch {
	case errors.Is(err, worker.ErrOutputTooLarge):
		return http.StatusUnprocessableEntity, errorResponse{Code: "tile_too_large", Message: "tile exceeds configured size limits"}
	case errors.Is(err, cache.ErrCapacityUnavailable):
		return http.StatusServiceUnavailable, errorResponse{Code: "capacity_unavailable", Message: "tile capacity is unavailable", Retryable: true}
	case errors.Is(err, worker.ErrTileTimeout), errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, errorResponse{Code: "tile_timeout", Message: "tile build timed out", Retryable: true}
	case errors.Is(err, worker.ErrUpstream):
		return http.StatusServiceUnavailable, errorResponse{Code: "upstream_unavailable", Message: "tile source is unavailable", Retryable: true}
	default:
		return http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile build failed"}
	}
}

func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		if strings.TrimSpace(candidate) == etag {
			return true
		}
	}
	return false
}

func supportedH3Resolutions() []int {
	resolutions := make([]int, h3.MaxResolution+1)
	for resolution := range resolutions {
		resolutions[resolution] = resolution
	}
	return resolutions
}

func writeJSON(responseWriter http.ResponseWriter, status int, value any) {
	responseWriter.Header().Set("Content-Type", contentTypeJSON)
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(value); err != nil {
		slog.Error("write JSON response", "status", status, "error", err)
	}
}

func writeError(responseWriter http.ResponseWriter, status int, response errorResponse) {
	writeJSON(responseWriter, status, response)
}
