//nolint:goconst // Independent cases keep query expectations readable.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestCachedTileMatchesDirectMembership(t *testing.T) {
	extensionDirectory := os.Getenv("OVERTURE_DUCKDB_EXTENSION_DIRECTORY")
	if extensionDirectory == "" {
		t.Skip("requires bundled spatial extension; make test on Linux supplies it")
	}
	// Output limits have separate subprocess tests; keep this query/export test
	// from changing the test process's permanent file-size limit.
	previousLimit := setFileSizeLimit
	setFileSizeLimit = func(int64) error { return nil }
	t.Cleanup(func() { setFileSizeLimit = previousLimit })
	for _, test := range []struct {
		name  string
		point h3.LatLng
		empty bool
	}{
		{name: "ordinary", point: h3.LatLng{Lat: 37.775938728915946, Lng: -122.41795063018799}},
		{name: "antimeridian", point: h3.LatLng{Lat: 0, Lng: 179.9999}},
		{name: "empty", point: h3.LatLng{Lat: 60.17, Lng: 24.94}, empty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := openDuckDBConnection(t)
			_, err := connection.ExecContext(t.Context(), "SET extension_directory = ?", extensionDirectory)
			assert.NilError(t, err)
			_, err = connection.ExecContext(t.Context(), "LOAD spatial")
			assert.NilError(t, err)
			assert.NilError(t, h3filter.RegisterCellContains(connection))
			cell, err := h3.LatLngToCell(test.point, 9)
			assert.NilError(t, err)
			bounds, err := h3filter.CellBounds(cell)
			assert.NilError(t, err)
			fixture, err := os.ReadFile(filepath.Join("testdata", "cached-points.sql"))
			assert.NilError(t, err)
			for index, interval := range bounds.LongitudeIntervals {
				prefix := "INSERT INTO points "
				if index == 0 {
					prefix = "CREATE TABLE points AS "
				}
				query := prefix + string(fixture)
				if test.empty {
					query += " WHERE FALSE"
				}
				_, err = connection.ExecContext(t.Context(), query, strconv.Itoa(index), interval.MinDeg, interval.MaxDeg-interval.MinDeg, bounds.LatitudeMinDeg, bounds.LatitudeMaxDeg-bounds.LatitudeMinDeg)
				assert.NilError(t, err)
			}
			snapshot := testSnapshot()
			fields := []string{"id", "geometry", "names"}
			plan, err := BuildQueryPlan(TileRequest{Cell: cell.String()}, fields, snapshot.Schema)
			assert.NilError(t, err)
			cells, err := h3filter.CoveringCells(cell, cell.Resolution()-1)
			assert.NilError(t, err)
			directory := t.TempDir()
			paths := make([]string, 0, len(cells))
			for _, coveringCell := range cells {
				sourcePlan, planErr := BuildQueryPlan(TileRequest{Cell: coveringCell.String()}, fields, snapshot.Schema)
				assert.NilError(t, planErr)
				metadata, metadataErr := candidateMetadata(sourcePlan)
				assert.NilError(t, metadataErr)
				sourcePath := filepath.Join(directory, coveringCell.String()+".parquet")
				sql := fmt.Sprintf("SELECT %s FROM points WHERE h3_cell_contains(ST_Y(geometry), ST_X(geometry), CAST(%d AS UBIGINT))", candidateProjectionSQL(plan.Columns), uint64(coveringCell))
				copySQL, copyErr := geoparquet.CopySQL(sql, sourcePath, metadata)
				assert.NilError(t, copyErr)
				_, err = connection.ExecContext(t.Context(), copySQL)
				assert.NilError(t, err)
				paths = append(paths, sourcePath)
			}
			outputPath := filepath.Join(directory, "derived.parquet")
			request := RuntimeTileRequest{
				Conn: connection, Plan: plan, Snapshot: snapshot, SourcePaths: paths,
				MaxRows: 10000, OutputPath: outputPath,
				Settings: RuntimeSettings{MemoryBytes: 64 << 20, Threads: 1, ScratchDirectory: t.TempDir(), ScratchMaxBytes: 1 << 20, MaxOutputBytes: 1 << 20, TileTimeout: time.Second * 10},
			}
			result, err := BuildTileWithSettings(t.Context(), request)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			validation, err := geoparquet.ValidateFile(outputPath, fields, request.Settings.MaxOutputBytes)
			assert.NilError(t, err)
			assert.Equal(t, validation.Digest, result.Digest)
			assert.Equal(t, validation.RowCount, result.RowCount)
			var differences int64
			err = connection.QueryRowContext(t.Context(), `
 WITH expected AS (
 SELECT id, hex(ST_AsWKB(geometry)) AS geometry, to_json(names) AS names
 FROM points WHERE h3_cell_contains(ST_Y(geometry), ST_X(geometry), CAST(? AS UBIGINT))
 ), actual AS (
 SELECT id, hex(ST_AsWKB(geometry)) AS geometry, to_json(names) AS names FROM read_parquet(?)
 )
 SELECT count(*) FROM (
 (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
 )`, plan.CellValue, outputPath).Scan(&differences)
			assert.NilError(t, err)
			assert.Equal(t, differences, int64(0))
			if test.empty {
				assert.Equal(t, result.RowCount, int64(0))
				return
			}
			assert.Assert(t, result.RowCount > 1)
			parent, err := cell.Parent(cell.Resolution() - 1)
			assert.NilError(t, err)
			var outsideParent int64
			err = connection.QueryRowContext(t.Context(), `SELECT count(*) FROM points WHERE h3_cell_contains(ST_Y(geometry), ST_X(geometry), CAST(? AS UBIGINT)) AND NOT h3_cell_contains(ST_Y(geometry), ST_X(geometry), CAST(? AS UBIGINT))`, plan.CellValue, uint64(parent)).Scan(&outsideParent)
			assert.NilError(t, err)
			if test.name == "ordinary" {
				assert.Assert(t, outsideParent > 0, "fixture must exercise child points outside the logical parent")
			}
			assert.NilError(t, os.Remove(outputPath))
			for _, failure := range []struct {
				name   string
				mutate func(*RuntimeTileRequest)
				want   error
			}{
				{name: "row limit", mutate: func(request *RuntimeTileRequest) { request.MaxRows = 1 }, want: ErrTooManyRows},
				{name: "byte limit", mutate: func(request *RuntimeTileRequest) { request.Settings.MaxOutputBytes = 1 }, want: ErrOutputTooLarge},
				{name: "timeout", mutate: func(request *RuntimeTileRequest) { request.Settings.TileTimeout = time.Nanosecond }, want: ErrTileTimeout},
				{name: "canceled", mutate: func(_ *RuntimeTileRequest) {}, want: ErrTileCanceled},
			} {
				t.Run(failure.name, func(t *testing.T) {
					failing := request
					failure.mutate(&failing)
					ctx := t.Context()
					if errors.Is(failure.want, ErrTileCanceled) {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					_, buildErr := BuildTileWithSettings(ctx, failing)
					assert.Assert(t, errors.Is(buildErr, failure.want), buildErr)
					_, statErr := os.Stat(outputPath)
					assert.Assert(t, errors.Is(statErr, os.ErrNotExist))
				})
			}
			corruptPaths := slices.Clone(paths)
			corruptPaths[0] = filepath.Join(directory, "corrupt.parquet")
			assert.NilError(t, os.WriteFile(corruptPaths[0], []byte("not parquet"), 0o600))
			request.SourcePaths = corruptPaths
			_, err = BuildTileWithSettings(t.Context(), request)
			assert.ErrorContains(t, err, "materialize candidates")
		})
	}
}

func TestCachedCandidateQueryRejectsInvalidSources(t *testing.T) {
	plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, []string{"id", "geometry"}, testSnapshot().Schema)
	assert.NilError(t, err)
	path := filepath.Join(t.TempDir(), "cached.parquet")
	for _, test := range []struct {
		name  string
		paths []string
	}{
		{name: "empty"},
		{name: "relative", paths: []string{"tile.parquet"}},
		{name: "remote", paths: []string{"https://example.com/tile.parquet"}},
		{name: "glob", paths: []string{filepath.Join(t.TempDir(), "*.parquet")}},
		{name: "duplicate", paths: []string{path, path}},
		{name: "too many", paths: make([]string, h3filter.MaxCoveringCells+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, queryErr := BuildCachedCandidateQuery(plan, test.paths, 100)
			assert.Assert(t, queryErr != nil)
		})
	}
	query, err := BuildCachedCandidateQuery(plan, []string{path}, 100)
	assert.NilError(t, err)
	assert.DeepEqual(t, query.Args, []any{path, plan.CellValue, int64(101)})
}
