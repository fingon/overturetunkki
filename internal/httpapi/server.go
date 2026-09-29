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
	"sync"
	"sync/atomic"
	"time"

	"github.com/mstenber/overturetunkki/internal/cache"
	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/worker"
	"github.com/uber/h3-go/v4"
)

const (
	contentTypeJSON            = "application/json; charset=utf-8"
	contentTypeParquet         = "application/vnd.apache.parquet"
	cacheControlFresh          = "no-cache, must-revalidate"
	releaseHeader              = "Overture-Release"
	catalogVersionHeader       = "Overture-Catalog-Version"
	projectionHeader           = "Overture-Projection-ID"
	requestIDHeader            = "X-Request-ID"
	retryAfterSeconds          = "1"
	defaultTileConcurrency     = 2
	defaultWriteTimeout        = 30 * time.Second
	capacityUnavailableCode    = "capacity_unavailable"
	capacityUnavailableMessage = "tile capacity is unavailable"
	serverShuttingDownCode     = "server_shutting_down"
	serverShuttingDownMessage  = "HTTP server is shutting down"
)

var ErrServerShuttingDown = errors.New(serverShuttingDownMessage)

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
	Observer        CatalogObserver
	Provider        TileProvider
	Fields          []string
	MaxTileBytes    int64
	MaxTileRows     int64
	AttributionURL  []string
	TileConcurrency int
	WriteTimeout    time.Duration
}

type Server struct {
	observer        CatalogObserver
	provider        TileProvider
	fields          []string
	options         Options
	requests        atomic.Uint64
	catalogRequests atomic.Uint64
	tileRequests    atomic.Uint64
	error4xx        atomic.Uint64
	error5xx        atomic.Uint64
	capacityRejects atomic.Uint64
	requestSequence atomic.Uint64
	tileAdmissions  chan struct{}
	stateMu         sync.Mutex
	activeRequests  sync.WaitGroup
	shuttingDown    bool
}

type trackingResponseWriter struct {
	http.ResponseWriter
	status int
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
	Code                string `json:"code"`
	Message             string `json:"message"`
	Retryable           bool   `json:"retryable"`
	Release             string `json:"release,omitempty"`
	CatalogVersion      string `json:"catalog_version,omitempty"`
	Cell                string `json:"cell,omitempty"`
	Resolution          *int   `json:"resolution,omitempty"`
	MaxTileBytes        int64  `json:"max_tile_bytes,omitempty"`
	FailedLimit         string `json:"failed_limit,omitempty"`
	SuggestedResolution *int   `json:"suggested_resolution,omitempty"`
	CanRefine           *bool  `json:"can_refine,omitempty"`
	Guidance            string `json:"guidance,omitempty"`
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
	if options.TileConcurrency < 0 {
		return nil, fmt.Errorf("HTTP server tile concurrency must not be negative")
	}
	if options.WriteTimeout < 0 {
		return nil, fmt.Errorf("HTTP server write timeout must not be negative")
	}
	if options.TileConcurrency == 0 {
		options.TileConcurrency = defaultTileConcurrency
	}
	if options.WriteTimeout == 0 {
		options.WriteTimeout = defaultWriteTimeout
	}
	fields := append([]string(nil), options.Fields...)
	attribution := append([]string(nil), options.AttributionURL...)
	options.Fields = fields
	options.AttributionURL = attribution
	return &Server{
		observer:       options.Observer,
		provider:       options.Provider,
		fields:         fields,
		options:        options,
		tileAdmissions: make(chan struct{}, options.TileConcurrency),
	}, nil
}

func (server *Server) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	if server == nil {
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "HTTP server is unavailable"})
		return
	}
	requestID := server.requestID(request)
	request.Header.Set(requestIDHeader, requestID)
	responseWriter.Header().Set(requestIDHeader, requestID)
	trackedWriter := &trackingResponseWriter{ResponseWriter: responseWriter}
	server.requests.Add(1)
	server.countEndpoint(request.URL.Path)
	if !server.beginRequest() {
		writeError(trackedWriter, http.StatusServiceUnavailable, errorResponse{Code: serverShuttingDownCode, Message: serverShuttingDownMessage, Retryable: true})
		server.finishRequest(trackedWriter, request, requestID)
		return
	}
	defer func() {
		server.finishRequest(trackedWriter, request, requestID)
		server.activeRequests.Done()
	}()
	switch {
	case request.URL.Path == "/v1/catalog":
		server.serveCatalog(trackedWriter, request)
	case strings.HasPrefix(request.URL.Path, "/v1/tiles/places/"):
		server.serveTile(trackedWriter, request)
	case request.URL.Path == "/livez":
		server.serveLive(trackedWriter, request)
	case request.URL.Path == "/readyz":
		server.serveReady(trackedWriter, request)
	case request.URL.Path == "/metrics":
		server.serveMetrics(trackedWriter, request)
	default:
		writeError(trackedWriter, http.StatusNotFound, errorResponse{Code: "not_found", Message: "endpoint not found"})
	}
}

func (writer *trackingResponseWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *trackingResponseWriter) Write(data []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(data)
}

func (writer *trackingResponseWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func (server *Server) requestID(request *http.Request) string {
	candidate := strings.TrimSpace(request.Header.Get(requestIDHeader))
	if validRequestID(candidate) {
		return candidate
	}
	return fmt.Sprintf("req-%d", server.requestSequence.Add(1))
}

func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func (server *Server) countEndpoint(path string) {
	switch {
	case path == "/v1/catalog":
		server.catalogRequests.Add(1)
	case strings.HasPrefix(path, "/v1/tiles/places/"):
		server.tileRequests.Add(1)
	}
}

func (server *Server) beginRequest() bool {
	server.stateMu.Lock()
	defer server.stateMu.Unlock()
	if server.shuttingDown {
		return false
	}
	server.activeRequests.Add(1)
	return true
}

func (server *Server) finishRequest(writer *trackingResponseWriter, request *http.Request, requestID string) {
	status := writer.status
	if status == 0 {
		status = http.StatusOK
	}
	switch {
	case status >= http.StatusBadRequest && status < http.StatusInternalServerError:
		server.error4xx.Add(1)
	case status >= http.StatusInternalServerError:
		server.error5xx.Add(1)
	}
	if status >= http.StatusBadRequest {
		logLevel := slog.LevelWarn
		if status >= http.StatusInternalServerError {
			logLevel = slog.LevelError
		}
		slog.Log(request.Context(), logLevel, "HTTP request failed", "request_id", requestID, "method", request.Method, "path", request.URL.Path, "status", status)
	}
}

func (server *Server) acquireTile() error {
	server.stateMu.Lock()
	defer server.stateMu.Unlock()
	if server.shuttingDown {
		return ErrServerShuttingDown
	}
	select {
	case server.tileAdmissions <- struct{}{}:
		return nil
	default:
		server.capacityRejects.Add(1)
		return cache.ErrCapacityUnavailable
	}
}

func (server *Server) releaseTile() {
	<-server.tileAdmissions
}

func (server *Server) Shutdown(ctx context.Context) error {
	if server == nil {
		return fmt.Errorf("HTTP server is nil")
	}
	if ctx == nil {
		return fmt.Errorf("HTTP shutdown context is nil")
	}
	server.stateMu.Lock()
	server.shuttingDown = true
	server.stateMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.activeRequests.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown HTTP server: %w", ctx.Err())
	}
}

func (server *Server) serveCatalog(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, errorResponse{Code: "invalid_request", Message: "method must be GET"})
		return
	}
	snapshot, err := server.refresh(request.Context())
	if err != nil {
		slog.Error("refresh catalog for HTTP response", "request_id", request.Header.Get(requestIDHeader), "error", err)
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
		slog.Error("refresh catalog for tile request", "request_id", request.Header.Get(requestIDHeader), "error", err)
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is unavailable", Retryable: true})
		return
	}
	if version != snapshot.CatalogVersion {
		writeError(responseWriter, http.StatusConflict, errorResponse{Code: "catalog_changed", Message: "catalog version is stale", Release: snapshot.Release, CatalogVersion: snapshot.CatalogVersion})
		return
	}
	if err := server.acquireTile(); err != nil {
		if errors.Is(err, ErrServerShuttingDown) {
			writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: serverShuttingDownCode, Message: serverShuttingDownMessage, Retryable: true})
			return
		}
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: capacityUnavailableCode, Message: capacityUnavailableMessage, Retryable: true})
		return
	}
	defer server.releaseTile()
	tile, err := server.provider.Get(request.Context(), snapshot, cell)
	if err != nil {
		status, response := classifyTileError(err, cell, server.options.MaxTileBytes)
		if errors.Is(err, cache.ErrCapacityUnavailable) {
			server.capacityRejects.Add(1)
		}
		logLevel := slog.LevelError
		if status < http.StatusInternalServerError {
			logLevel = slog.LevelWarn
		}
		slog.Log(request.Context(), logLevel, "tile request failed", "request_id", request.Header.Get(requestIDHeader), "cell", cell.String(), "status", status, "error", err)
		writeError(responseWriter, status, response)
		return
	}
	latest, err := server.refresh(request.Context())
	if err != nil {
		slog.Error("refresh catalog before tile response", "request_id", request.Header.Get(requestIDHeader), "cell", cell.String(), "error", err)
		writeError(responseWriter, http.StatusServiceUnavailable, errorResponse{Code: "catalog_unavailable", Message: "catalog is unavailable", Retryable: true})
		return
	}
	if latest.CatalogVersion != snapshot.CatalogVersion || latest.Release != snapshot.Release {
		slog.Warn("catalog changed before tile response", "request_id", request.Header.Get(requestIDHeader), "cell", cell.String(), "catalog_version", latest.CatalogVersion, "release", latest.Release)
		writeError(responseWriter, http.StatusConflict, errorResponse{Code: "catalog_changed", Message: "catalog changed before response", Release: latest.Release, CatalogVersion: latest.CatalogVersion})
		return
	}
	server.serveTileFile(responseWriter, request, snapshot, tile)
}

func (server *Server) serveTileFile(responseWriter http.ResponseWriter, request *http.Request, snapshot catalog.Snapshot, tile Tile) {
	if tile.Path == "" || tile.SizeBytes <= 0 || tile.Digest == "" || strings.ContainsAny(tile.Digest, "\"\r\n") {
		slog.Error("tile metadata is invalid", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "size_bytes", tile.SizeBytes)
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile metadata is invalid"})
		return
	}
	file, err := os.Open(tile.Path)
	if err != nil {
		slog.Error("open tile response file", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", err)
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile cannot be opened"})
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Error("close tile response file", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", closeErr)
		}
	}()
	fileInfo, err := file.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Size() != tile.SizeBytes {
		if err != nil {
			slog.Error("stat tile response file", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", err)
		} else {
			slog.Error("tile response file does not match metadata", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "expected_bytes", tile.SizeBytes, "actual_bytes", fileInfo.Size(), "regular", fileInfo.Mode().IsRegular())
		}
		writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile metadata does not match file"})
		return
	}
	responseController := http.NewResponseController(responseWriter)
	deadlineSet := false
	if err := responseController.SetWriteDeadline(time.Now().Add(server.options.WriteTimeout)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			slog.Error("set tile response write deadline", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", err)
			writeError(responseWriter, http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile response deadline cannot be set"})
			return
		}
		slog.Debug("tile response writer does not support write deadlines", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path)
	} else {
		deadlineSet = true
	}
	if deadlineSet {
		defer func() {
			if resetErr := responseController.SetWriteDeadline(time.Time{}); resetErr != nil && !errors.Is(resetErr, http.ErrNotSupported) {
				slog.Error("clear tile response write deadline", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", resetErr)
			}
		}()
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
		slog.Error("stream tile response", "request_id", request.Header.Get(requestIDHeader), "path", tile.Path, "error", err)
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
		slog.Warn("catalog readiness check failed", "request_id", request.Header.Get(requestIDHeader), "error", err)
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
	} else {
		slog.Warn("catalog readiness metric is unavailable", "request_id", request.Header.Get(requestIDHeader), "error", err)
	}
	responseWriter.Header().Set("Content-Type", "text/plain; version=0.0.4")
	responseWriter.WriteHeader(http.StatusOK)
	_, err := fmt.Fprintf(responseWriter, "# TYPE overture_http_requests_total counter\noverture_http_requests_total %d\n# TYPE overture_http_catalog_requests_total counter\noverture_http_catalog_requests_total %d\n# TYPE overture_http_tile_requests_total counter\noverture_http_tile_requests_total %d\n# TYPE overture_http_errors_4xx_total counter\noverture_http_errors_4xx_total %d\n# TYPE overture_http_errors_5xx_total counter\noverture_http_errors_5xx_total %d\n# TYPE overture_http_capacity_rejections_total counter\noverture_http_capacity_rejections_total %d\n# TYPE overture_catalog_ready gauge\noverture_catalog_ready %d\n", server.requests.Load(), server.catalogRequests.Load(), server.tileRequests.Load(), server.error4xx.Load(), server.error5xx.Load(), server.capacityRejects.Load(), ready)
	if err != nil {
		slog.Error("write metrics response", "request_id", request.Header.Get(requestIDHeader), "error", err)
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

func classifyTileError(err error, cell h3.Cell, maxTileBytes int64) (int, errorResponse) {
	switch {
	case errors.Is(err, worker.ErrOutputTooLarge):
		return http.StatusUnprocessableEntity, sizeErrorResponse(cell, maxTileBytes, "compressed_bytes", "tile exceeds the compressed byte limit")
	case errors.Is(err, worker.ErrTooManyRows):
		return http.StatusUnprocessableEntity, sizeErrorResponse(cell, maxTileBytes, "rows", "tile exceeds the row limit")
	case errors.Is(err, cache.ErrCapacityUnavailable):
		return http.StatusServiceUnavailable, errorResponse{Code: capacityUnavailableCode, Message: capacityUnavailableMessage, Retryable: true}
	case errors.Is(err, worker.ErrTileTimeout), errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, errorResponse{Code: "tile_timeout", Message: "tile build timed out", Retryable: true}
	case errors.Is(err, worker.ErrUpstream):
		return http.StatusServiceUnavailable, errorResponse{Code: "upstream_unavailable", Message: "tile source is unavailable", Retryable: true}
	default:
		return http.StatusInternalServerError, errorResponse{Code: "internal_error", Message: "tile build failed"}
	}
}

func sizeErrorResponse(cell h3.Cell, maxTileBytes int64, failedLimit, message string) errorResponse {
	resolution := cell.Resolution()
	canRefine := resolution < h3.MaxResolution
	response := errorResponse{
		Code:         "tile_too_large",
		Message:      message,
		Cell:         cell.String(),
		Resolution:   &resolution,
		MaxTileBytes: maxTileBytes,
		FailedLimit:  failedLimit,
		CanRefine:    &canRefine,
		Guidance:     "cover the viewport or original cell with all intersecting finer cells and deduplicate POI ids",
	}
	if canRefine {
		suggestedResolution := resolution + 1
		response.SuggestedResolution = &suggestedResolution
	}
	return response
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
	if status == http.StatusServiceUnavailable && response.Retryable {
		responseWriter.Header().Set("Retry-After", retryAfterSeconds)
	}
	writeJSON(responseWriter, status, response)
}
