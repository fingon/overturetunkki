package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/mstenber/overturetunkki/internal/client"
	"github.com/mstenber/overturetunkki/internal/logging"
)

type cliConfig struct {
	ServerURL string        `name:"server-url" env:"OVERTURE_CLIENT_SERVER_URL" default:"http://localhost:8080" help:"HTTP(S) server URL."`
	Timeout   time.Duration `name:"timeout" env:"OVERTURE_CLIENT_TIMEOUT" default:"60s" help:"Command-wide timeout."`
	Verbose   bool          `short:"v" env:"OVERTURE_CLIENT_VERBOSE" help:"Enable debug diagnostics."`

	Catalog catalogCommand `cmd:"" help:"Fetch catalog metadata."`
	Tile    tileCommand    `cmd:"" help:"Fetch one H3 tile."`
}

type catalogCommand struct{}

type tileCommand struct {
	Cell             string `arg:"" help:"Canonical H3 cell index."`
	CatalogVersion   string `name:"catalog-version" help:"Explicit catalog version."`
	IfNoneMatch      string `name:"if-none-match" help:"ETag for conditional tile testing."`
	Output           string `name:"output" help:"Destination Parquet file."`
	Force            bool   `name:"force" help:"Allow replacing an existing destination."`
	MaxDownloadBytes int64  `name:"max-download-bytes" env:"OVERTURE_CLIENT_MAX_DOWNLOAD_BYTES" default:"67108864" help:"Maximum downloaded response bytes."`
}

const (
	exitSuccess          = 0
	exitFailure          = 1
	exitInvalidArguments = 2
	exitCatalogChanged   = 3
	exitTileTooLarge     = 4
	exitTemporaryFailure = 5
)

type argumentError struct {
	cause error
}

func (err *argumentError) Error() string {
	return err.cause.Error()
}

func (err *argumentError) Unwrap() error {
	return err.cause
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		code := exitCode(err)
		slog.Error("overture client failed", "error", diagnostic(err), "exit_code", code)
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return fmt.Errorf("run client context is nil")
	}
	var config cliConfig
	parser, err := kong.New(
		&config,
		kong.Name("overture-client"),
		kong.Description("Inspect and download Overture tile service responses."),
	)
	if err != nil {
		return fmt.Errorf("build client command parser: %w", err)
	}
	parsed, err := parser.Parse(args)
	if err != nil {
		return markArgumentError(fmt.Errorf("parse client command: %w", err))
	}
	clientConfig := client.Config{ServerURL: config.ServerURL, Timeout: config.Timeout, Verbose: config.Verbose}
	if err := clientConfig.Validate(); err != nil {
		return markArgumentError(fmt.Errorf("validate client configuration: %w", err))
	}
	logging.Configure(config.Verbose)
	commandContext, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	if err := commandContext.Err(); err != nil {
		return fmt.Errorf("client command canceled: %w", err)
	}
	serverURL, err := clientConfig.NormalizedServerURL()
	if err != nil {
		return markArgumentError(fmt.Errorf("normalize client configuration: %w", err))
	}
	httpClient, err := client.New(client.Options{ServerURL: serverURL})
	if err != nil {
		return err
	}
	commandName := strings.Fields(parsed.Command())
	if len(commandName) == 0 {
		return markArgumentError(fmt.Errorf("client command is required"))
	}
	switch commandName[0] {
	case "catalog":
		return runCatalog(commandContext, httpClient)
	case "tile":
		return runTile(commandContext, httpClient, config.Tile)
	default:
		return markArgumentError(fmt.Errorf("client command %q is not supported", parsed.Command()))
	}
}

func runCatalog(ctx context.Context, httpClient *client.Client) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("catalog command canceled: %w", err)
	}
	catalogResponse, _, err := httpClient.FetchCatalog(ctx)
	if err != nil {
		return fmt.Errorf("fetch catalog: %w", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(catalogResponse); err != nil {
		return fmt.Errorf("write catalog result: %w", err)
	}
	return nil
}

func runTile(ctx context.Context, httpClient *client.Client, command tileCommand) error {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tile command canceled: %w", err)
	}
	if httpClient == nil {
		return fmt.Errorf("tile command HTTP client is nil")
	}
	cell, err := client.ParseCell(command.Cell)
	if err != nil {
		return markArgumentError(err)
	}
	if command.Output == "" {
		return markArgumentError(fmt.Errorf("tile command output is required"))
	}
	if command.MaxDownloadBytes <= 0 {
		return markArgumentError(fmt.Errorf("tile command max download bytes must be positive"))
	}
	catalogVersion := command.CatalogVersion
	expectedProjectionID := ""
	maxDownloadBytes := command.MaxDownloadBytes
	if catalogVersion == "" {
		catalogResponse, _, err := httpClient.FetchCatalog(ctx)
		if err != nil {
			return fmt.Errorf("discover catalog for tile: %w", err)
		}
		catalogVersion = catalogResponse.CatalogVersion
		expectedProjectionID = catalogResponse.ProjectionID
		if catalogResponse.MaxTileBytes < maxDownloadBytes {
			maxDownloadBytes = catalogResponse.MaxTileBytes
		}
	}
	response, err := httpClient.RequestTile(ctx, cell.String(), catalogVersion, command.IfNoneMatch)
	if err != nil {
		return err
	}
	downloadResult, err := client.DownloadTileResponse(ctx, response, client.DownloadOptions{
		Destination:            command.Output,
		Force:                  command.Force,
		ErrorBodyBytes:         httpClient.ErrorBodyLimit(),
		MaxDownloadBytes:       maxDownloadBytes,
		ExpectedCatalogVersion: catalogVersion,
		ExpectedProjectionID:   expectedProjectionID,
	})
	if err != nil {
		return fmt.Errorf("download tile: %w", err)
	}
	result := struct {
		Cell            string `json:"cell"`
		Release         string `json:"release,omitempty"`
		CatalogVersion  string `json:"catalog_version"`
		ProjectionID    string `json:"projection_id,omitempty"`
		Status          int    `json:"status"`
		ETag            string `json:"etag,omitempty"`
		DownloadedBytes int64  `json:"downloaded_bytes,omitempty"`
		PublishedPath   string `json:"published_path,omitempty"`
		NotModified     bool   `json:"not_modified,omitempty"`
		ElapsedMS       int64  `json:"elapsed_ms"`
	}{
		Cell:            cell.String(),
		Release:         downloadResult.Release,
		CatalogVersion:  catalogVersion,
		ProjectionID:    downloadResult.ProjectionID,
		Status:          downloadResult.StatusCode,
		ETag:            downloadResult.ETag,
		DownloadedBytes: downloadResult.DownloadedBytes,
		PublishedPath:   downloadResult.PublishedPath,
		NotModified:     downloadResult.NotModified,
		ElapsedMS:       time.Since(started).Milliseconds(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return fmt.Errorf("write tile result: %w", err)
	}
	return nil
}

func markArgumentError(err error) error {
	if err == nil {
		return nil
	}
	return &argumentError{cause: err}
}

func exitCode(err error) int {
	if err == nil {
		return exitSuccess
	}
	var argumentErr *argumentError
	if errors.As(err, &argumentErr) {
		return exitInvalidArguments
	}
	var httpErr *client.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Meta.StatusCode {
		case http.StatusBadRequest:
			return exitInvalidArguments
		case http.StatusConflict:
			return exitCatalogChanged
		case http.StatusUnprocessableEntity:
			return exitTileTooLarge
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return exitTemporaryFailure
		}
	}
	return exitFailure
}

func diagnostic(err error) string {
	var httpErr *client.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Diagnostic()
	}
	return err.Error()
}
