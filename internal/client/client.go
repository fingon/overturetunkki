package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Options struct {
	ServerURL      string
	HTTPClient     *http.Client
	ResponseBytes  int64
	ErrorBodyBytes int64
}

type RequestOptions struct {
	Method  string
	Path    string
	Query   url.Values
	Headers http.Header
}

type Client struct {
	baseURL        *url.URL
	httpClient     *http.Client
	responseBytes  int64
	errorBodyBytes int64
}

func New(options Options) (*Client, error) {
	baseURL, err := parseServerURL(options.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	responseBytes := options.ResponseBytes
	if responseBytes == 0 {
		responseBytes = DefaultResponseBytes
	}
	if responseBytes <= 0 {
		return nil, errors.New("create HTTP client: response byte limit must be positive")
	}
	errorBodyBytes := options.ErrorBodyBytes
	if errorBodyBytes == 0 {
		errorBodyBytes = DefaultErrorBodyBytes
	}
	if errorBodyBytes <= 0 {
		return nil, errors.New("create HTTP client: error body byte limit must be positive")
	}
	return &Client{baseURL: baseURL, httpClient: httpClient, responseBytes: responseBytes, errorBodyBytes: errorBodyBytes}, nil
}

func (client *Client) Request(ctx context.Context, options RequestOptions) (*http.Response, error) {
	if client == nil || client.baseURL == nil || client.httpClient == nil {
		return nil, errors.New("request HTTP endpoint: client is nil")
	}
	if ctx == nil {
		return nil, errors.New("request HTTP endpoint: context is nil")
	}
	if options.Method == "" {
		return nil, errors.New("request HTTP endpoint: method is empty")
	}
	if options.Path == "" || !strings.HasPrefix(options.Path, "/") {
		return nil, errors.New("request HTTP endpoint: path must start with slash")
	}
	target := *client.baseURL
	target.Path = strings.TrimRight(client.baseURL.Path, "/") + options.Path
	target.RawQuery = options.Query.Encode()
	request, err := http.NewRequestWithContext(ctx, options.Method, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}
	for name, values := range options.Headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request %s %s: %w", options.Method, options.Path, err)
	}
	return response, nil
}

func CloseResponse(response *http.Response) error {
	if response == nil || response.Body == nil {
		return errors.New("close HTTP response: response body is nil")
	}
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close HTTP response body: %w", err)
	}
	return nil
}

func ReadBody(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, errors.New("read HTTP response: response body is nil")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("read HTTP response: byte limit must be positive, got %d", maxBytes)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		if closeErr != nil {
			return nil, fmt.Errorf("read HTTP response body: %w; close HTTP response body: %w", readErr, closeErr)
		}
		return nil, fmt.Errorf("read HTTP response body: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close HTTP response body: %w", closeErr)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("HTTP response body exceeds %d bytes", maxBytes)
	}
	return body, nil
}

func (client *Client) ErrorBodyLimit() int64 {
	if client == nil || client.errorBodyBytes <= 0 {
		return DefaultErrorBodyBytes
	}
	return client.errorBodyBytes
}
