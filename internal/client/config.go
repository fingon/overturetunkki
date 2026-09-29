package client

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultServerURL = "http://localhost:8080"
	DefaultTimeout   = 60 * time.Second
)

type Config struct {
	ServerURL string
	Timeout   time.Duration
	Verbose   bool
}

func (config Config) Validate() error {
	if _, err := parseServerURL(config.ServerURL); err != nil {
		return err
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", config.Timeout)
	}
	return nil
}

func (config Config) NormalizedServerURL() (string, error) {
	parsed, err := parseServerURL(config.ServerURL)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed.String(), nil
}

func parseServerURL(rawURL string) (*url.URL, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("server URL must not be empty")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse server URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("server URL scheme %q is not HTTP(S)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("server URL host must not be empty")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("server URL must not contain user info, query, or fragment")
	}
	return parsed, nil
}
