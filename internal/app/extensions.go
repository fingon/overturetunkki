package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	extensionPreparationTimeout = 5 * time.Minute
	extensionRepository         = "https://extensions.duckdb.org"
)

func prepareExtensions(ctx context.Context, cacheDir string) (string, error) {
	return prepareExtensionsFrom(ctx, cacheDir, extensionRepository)
}

func prepareExtensionsFrom(ctx context.Context, cacheDir, repository string) (directory string, err error) {
	ctx, cancel := context.WithTimeout(ctx, extensionPreparationTimeout)
	defer cancel()
	directory = filepath.Join(cacheDir, "extensions")
	slog.DebugContext(ctx, "prepare DuckDB extensions", "directory", directory)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return "", fmt.Errorf("create DuckDB extension directory: %w", err)
	}
	database, err := sql.Open("duckdb", "")
	if err != nil {
		return "", fmt.Errorf("open DuckDB for extension installation: %w", err)
	}
	defer func() { err = errors.Join(err, database.Close()) }()
	connection, err := database.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("connect DuckDB for extension installation: %w", err)
	}
	defer func() { err = errors.Join(err, connection.Close()) }()
	if _, err := connection.ExecContext(ctx, "SET extension_directory = ?", directory); err != nil {
		return "", fmt.Errorf("set DuckDB installation directory: %w", err)
	}
	for _, extension := range []string{duckdbHTTPFSExtension, duckdbSpatialExtension} {
		slog.InfoContext(ctx, "ensure DuckDB extension installed", "extension", extension, "directory", directory)
		query := "INSTALL " + extension + " FROM '" + strings.ReplaceAll(repository, "'", "''") + "'"
		if _, err := connection.ExecContext(ctx, query); err != nil {
			return "", fmt.Errorf("install DuckDB extension %q: %w", extension, err)
		}
	}
	if err := configureTileConnectionDirectory(ctx, connection, directory); err != nil {
		return "", err
	}
	slog.InfoContext(ctx, "DuckDB extensions ready", "directory", directory)
	return directory, nil
}
