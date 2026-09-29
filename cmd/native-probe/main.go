package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/uber/h3-go/v4"
)

const (
	probeResolution = 9
	probeLatitude  = 37.775938728915946
	probeLongitude = -122.41795063018799
	dataPathEnv    = "KO_DATA_PATH"
	extensionDirEnv = "OVERTURE_DUCKDB_EXTENSION_DIRECTORY"
	httpfsExtensionName = "httpfs"
	parquetExtensionName = "parquet"
	spatialExtensionName = "spatial"
	extensionInventoryQuery = "SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE extension_name IN (?, ?, ?) ORDER BY extension_name"
)

var bundledExtensionNames = []string{
	httpfsExtensionName,
	parquetExtensionName,
	spatialExtensionName,
}

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("native dependency probe failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	cell, err := h3.LatLngToCell(h3.NewLatLng(probeLatitude, probeLongitude), probeResolution)
	if err != nil {
		return fmt.Errorf("convert coordinates to H3: %w", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close DuckDB: %w", closeErr)
				return
			}
			slog.Error("close DuckDB after probe failure", "error", closeErr)
		}
	}()
	if err := configureDuckDB(ctx, db); err != nil {
		return err
	}
	for _, extensionName := range bundledExtensionNames {
		if _, err := db.ExecContext(ctx, "LOAD "+extensionName); err != nil {
			return fmt.Errorf("load DuckDB extension: name=%s: %w", extensionName, err)
		}
	}

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("query DuckDB version: %w", err)
	}

	rows, err := db.QueryContext(ctx, extensionInventoryQuery, httpfsExtensionName, parquetExtensionName, spatialExtensionName)
	if err != nil {
		return fmt.Errorf("query DuckDB extensions: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close extension rows: %w", closeErr)
				return
			}
			slog.Error("close extension rows after probe failure", "error", closeErr)
		}
	}()

	seenExtensions := make(map[string]struct{}, len(bundledExtensionNames))
	for rows.Next() {
		var name string
		var loaded bool
		var installed bool
		if err := rows.Scan(&name, &loaded, &installed); err != nil {
			return fmt.Errorf("scan DuckDB extension: %w", err)
		}
		if !installed {
			return fmt.Errorf("DuckDB extension is not bundled: name=%s", name)
		}
		if !loaded {
			return fmt.Errorf("DuckDB extension is not loaded: name=%s", name)
		}
		seenExtensions[name] = struct{}{}
		slog.Info("verified bundled DuckDB extension", "name", name, "loaded", loaded)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate DuckDB extensions: %w", err)
	}
	for _, extensionName := range bundledExtensionNames {
		if _, ok := seenExtensions[extensionName]; !ok {
			return fmt.Errorf("DuckDB extension inventory incomplete: name=%s", extensionName)
		}
	}

	slog.Info("native dependencies verified", "duckdb_version", version, "h3_cell", cell.String(), "h3_resolution", probeResolution)
	return nil
}

func configureDuckDB(ctx context.Context, db *sql.DB) error {
	extensionDirectory := os.Getenv(extensionDirEnv)
	if extensionDirectory == "" {
		dataPath := os.Getenv(dataPathEnv)
		if dataPath == "" {
			return fmt.Errorf("DuckDB extension directory is not configured: set %s or %s", extensionDirEnv, dataPathEnv)
		}
		extensionDirectory = filepath.Join(dataPath, "extensions")
	}
	if _, err := os.Stat(extensionDirectory); err != nil {
		return fmt.Errorf("stat DuckDB extension directory: path=%s: %w", extensionDirectory, err)
	}
	settings := []struct {
		query string
		value string
	}{
		{query: "SET extension_directory = ?", value: extensionDirectory},
		{query: "SET autoload_known_extensions = false"},
		{query: "SET autoinstall_known_extensions = false"},
	}
	for _, setting := range settings {
		var err error
		if setting.value == "" {
			_, err = db.ExecContext(ctx, setting.query)
		} else {
			_, err = db.ExecContext(ctx, setting.query, setting.value)
		}
		if err != nil {
			return fmt.Errorf("configure DuckDB: query=%q: %w", setting.query, err)
		}
	}
	return nil
}
