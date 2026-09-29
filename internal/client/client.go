package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Options struct {
	ServerURL  string
	HTTPClient *http.Client
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
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
	return &Client{baseURL: baseURL, httpClient: httpClient}, nil
}

func (client *Client) Request(ctx context.Context, method, path string, query url.Values, headers http.Header) (*http.Response, error) {
	if client == nil || client.baseURL == nil || client.httpClient == nil {
		return nil, fmt.Errorf("request HTTP endpoint: client is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("request HTTP endpoint: context is nil")
	}
	if method == "" {
		return nil, fmt.Errorf("request HTTP endpoint: method is empty")
	}
	if path == "" || !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("request HTTP endpoint: path must start with slash")
	}
	target := *client.baseURL
	target.Path = strings.TrimRight(client.baseURL.Path, "/") + path
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request %s %s: %w", method, path, err)
	}
	return response, nil
}

func CloseResponse(response *http.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("close HTTP response: response body is nil")
	}
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close HTTP response body: %w", err)
	}
	return nil
}

func ReadBody(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("read HTTP response: response body is nil")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("read HTTP response: byte limit must be positive, got %d", maxBytes)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
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
