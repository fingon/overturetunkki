package client

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		config  Config
		message string
	}{
		{name: "valid HTTP", config: Config{ServerURL: "http://localhost:8080", Timeout: time.Second}},
		{name: "valid HTTPS path", config: Config{ServerURL: "https://example.com/service/", Timeout: time.Second}},
		{name: "empty URL", config: Config{Timeout: time.Second}, message: "server URL"},
		{name: "wrong scheme", config: Config{ServerURL: "ftp://example.com", Timeout: time.Second}, message: "HTTP(S)"},
		{name: "missing host", config: Config{ServerURL: "http:///service", Timeout: time.Second}, message: "host"},
		{name: "user info", config: Config{ServerURL: "http://user@example.com", Timeout: time.Second}, message: "user info"},
		{name: "query", config: Config{ServerURL: "http://example.com?x=1", Timeout: time.Second}, message: "query"},
		{name: "fragment", config: Config{ServerURL: "http://example.com#fragment", Timeout: time.Second}, message: "fragment"},
		{name: "timeout", config: Config{ServerURL: "http://example.com"}, message: "timeout"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.message == "" {
				assert.NilError(t, err)
				return
			}
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestNormalizedServerURL(t *testing.T) {
	config := Config{ServerURL: "https://example.com/service///", Timeout: time.Second}
	value, err := config.NormalizedServerURL()
	assert.NilError(t, err)
	assert.Equal(t, value, "https://example.com/service")
}
