package main

import (
	"context"
	"encoding/json"
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
	Cell           string `arg:"" help:"Canonical H3 cell index."`
	CatalogVersion string `name:"catalog-version" help:"Explicit catalog version."`
	IfNoneMatch    string `name:"if-none-match" help:"ETag for conditional tile testing."`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("overture client failed", "error", err)
		os.Exit(1)
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
		return fmt.Errorf("parse client command: %w", err)
	}
	clientConfig := client.Config{ServerURL: config.ServerURL, Timeout: config.Timeout, Verbose: config.Verbose}
	if err := clientConfig.Validate(); err != nil {
		return fmt.Errorf("validate client configuration: %w", err)
	}
	logging.Configure(config.Verbose)
	commandContext, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	if err := commandContext.Err(); err != nil {
		return fmt.Errorf("client command canceled: %w", err)
	}
	serverURL, err := clientConfig.NormalizedServerURL()
	if err != nil {
		return fmt.Errorf("normalize client configuration: %w", err)
	}
	httpClient, err := client.New(client.Options{ServerURL: serverURL})
	if err != nil {
		return err
	}
	commandName := strings.Fields(parsed.Command())
	if len(commandName) == 0 {
		return fmt.Errorf("client command is required")
	}
	switch commandName[0] {
	case "catalog":
		return runCatalog(commandContext, httpClient)
	case "tile":
		return runTile(commandContext, httpClient, config.Tile)
	default:
		return fmt.Errorf("client command %q is not supported", parsed.Command())
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
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tile command canceled: %w", err)
	}
	if httpClient == nil {
		return fmt.Errorf("tile command HTTP client is nil")
	}
	cell, err := client.ParseCell(command.Cell)
	if err != nil {
		return err
	}
	catalogVersion := command.CatalogVersion
	if catalogVersion == "" {
		catalogResponse, _, err := httpClient.FetchCatalog(ctx)
		if err != nil {
			return fmt.Errorf("discover catalog for tile: %w", err)
		}
		catalogVersion = catalogResponse.CatalogVersion
	}
	response, err := httpClient.RequestTile(ctx, cell.String(), catalogVersion, command.IfNoneMatch)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotModified {
		return client.ParseHTTPError(response, client.DefaultErrorBodyBytes)
	}
	if err := client.CloseResponse(response); err != nil {
		return fmt.Errorf("close tile response: %w", err)
	}
	result := struct {
		Cell           string `json:"cell"`
		CatalogVersion string `json:"catalog_version"`
		Status         int    `json:"status"`
		ETag           string `json:"etag,omitempty"`
	}{
		Cell:           cell.String(),
		CatalogVersion: catalogVersion,
		Status:         response.StatusCode,
		ETag:           response.Header.Get("ETag"),
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return fmt.Errorf("write tile result: %w", err)
	}
	return nil
}
