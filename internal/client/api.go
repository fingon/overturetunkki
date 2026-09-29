package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/uber/h3-go/v4"
)

const (
	CatalogPath                 = "/v1/catalog"
	TilePathPrefix              = "/v1/tiles/places/"
	ContentTypeParquet          = "application/vnd.apache.parquet"
	ReleaseHeader               = "Overture-Release"
	CatalogVersionHeader        = "Overture-Catalog-Version"
	ProjectionHeader            = "Overture-Projection-ID"
	RequestIDHeader             = "X-Request-ID"
	RetryAfterHeader            = "Retry-After"
	DefaultResponseBytes  int64 = 1 << 20
	DefaultErrorBodyBytes int64 = 64 << 10
)

type CatalogResponse struct {
	Release                string   `json:"release"`
	CatalogVersion         string   `json:"catalog_version"`
	ProjectionID           string   `json:"projection_id"`
	Fields                 []string `json:"fields"`
	MaxTileBytes           int64    `json:"max_tile_bytes"`
	MaxTileRows            int64    `json:"max_tile_rows"`
	SupportedH3Resolutions []int    `json:"supported_h3_resolutions"`
	Attribution            []string `json:"attribution"`
}

type ErrorResponse struct {
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

type ResponseMeta struct {
	StatusCode int
	Status     string
	Headers    http.Header
}

type HTTPError struct {
	Meta       ResponseMeta
	RequestID  string
	RetryAfter string
	Body       []byte
	API        ErrorResponse
	HasAPI     bool
	Cause      error
}

func (err *HTTPError) Error() string {
	if err == nil {
		return "HTTP request failed"
	}
	message := err.Meta.Status
	if err.HasAPI && err.API.Code != "" {
		message += ": " + err.API.Code
		if err.API.Message != "" {
			message += ": " + err.API.Message
		}
	}
	if err.Cause != nil {
		message += ": " + err.Cause.Error()
	}
	return message
}

func (err *HTTPError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func (client *Client) FetchCatalog(ctx context.Context) (CatalogResponse, ResponseMeta, error) {
	response, err := client.Request(ctx, http.MethodGet, CatalogPath, nil, nil)
	if err != nil {
		return CatalogResponse{}, ResponseMeta{}, err
	}
	meta := responseMeta(response)
	body, err := ReadBody(response, client.responseBytes)
	if err != nil {
		return CatalogResponse{}, meta, fmt.Errorf("read catalog response: %w", err)
	}
	if meta.StatusCode != http.StatusOK {
		return CatalogResponse{}, meta, newHTTPError(meta, body, nil)
	}
	var catalog CatalogResponse
	if err := json.Unmarshal(body, &catalog); err != nil {
		return CatalogResponse{}, meta, fmt.Errorf("decode catalog response: %w", err)
	}
	if err := validateCatalogResponse(catalog); err != nil {
		return CatalogResponse{}, meta, err
	}
	return catalog, meta, nil
}

func (client *Client) RequestTile(ctx context.Context, cell, catalogVersion, ifNoneMatch string) (*http.Response, error) {
	parsedCell, err := ParseCell(cell)
	if err != nil {
		return nil, err
	}
	if catalogVersion == "" {
		return nil, fmt.Errorf("catalog version must not be empty")
	}
	query := url.Values{"catalog_version": []string{catalogVersion}}
	headers := make(http.Header)
	if ifNoneMatch != "" {
		headers.Set("If-None-Match", ifNoneMatch)
	}
	return client.Request(ctx, http.MethodGet, TilePathPrefix+parsedCell.String(), query, headers)
}

func ParseCell(value string) (h3.Cell, error) {
	if value == "" {
		return 0, fmt.Errorf("H3 cell must not be empty")
	}
	cell := h3.CellFromString(value)
	if !cell.IsValid() {
		return 0, fmt.Errorf("H3 cell %q is invalid", value)
	}
	if cell.String() != value {
		return 0, fmt.Errorf("H3 cell %q is not canonical, want %q", value, cell.String())
	}
	return cell, nil
}

func ParseHTTPError(response *http.Response, maxBytes int64) error {
	if response == nil {
		return fmt.Errorf("parse HTTP error: response is nil")
	}
	meta := responseMeta(response)
	body, err := ReadBody(response, maxBytes)
	if err != nil {
		return newHTTPError(meta, nil, err)
	}
	return newHTTPError(meta, body, nil)
}

func responseMeta(response *http.Response) ResponseMeta {
	return ResponseMeta{
		StatusCode: response.StatusCode,
		Status:     response.Status,
		Headers:    response.Header.Clone(),
	}
}

func newHTTPError(meta ResponseMeta, body []byte, cause error) *HTTPError {
	result := &HTTPError{
		Meta:       meta,
		RequestID:  meta.Headers.Get(RequestIDHeader),
		RetryAfter: meta.Headers.Get(RetryAfterHeader),
		Body:       append([]byte(nil), body...),
		Cause:      cause,
	}
	if len(body) != 0 && json.Unmarshal(body, &result.API) == nil && result.API.Code != "" {
		result.HasAPI = true
	}
	return result
}

func validateCatalogResponse(catalog CatalogResponse) error {
	if catalog.Release == "" || catalog.CatalogVersion == "" || catalog.ProjectionID == "" {
		return fmt.Errorf("catalog response is missing release, catalog version, or projection ID")
	}
	if len(catalog.Fields) == 0 {
		return fmt.Errorf("catalog response has no fields")
	}
	if catalog.MaxTileBytes <= 0 || catalog.MaxTileRows <= 0 {
		return fmt.Errorf("catalog response has invalid tile limits")
	}
	return nil
}

func (client *Client) responseLimit() int64 {
	if client == nil || client.responseBytes <= 0 {
		return DefaultResponseBytes
	}
	return client.responseBytes
}

func trimHTTPErrorBody(body []byte) string {
	return strings.TrimSpace(string(body))
}
