//nolint:goconst // Independent cases keep spatial expectations readable.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"gotest.tools/v3/assert"
)

func TestPinnedAssetSelectionIsConservative(t *testing.T) {
	ordinary := h3filter.Bounds{LatitudeMinDeg: -1, LatitudeMaxDeg: 1, LongitudeIntervals: []h3filter.LongitudeInterval{{MinDeg: 10, MaxDeg: 20}}}
	wrapped := h3filter.Bounds{LatitudeMinDeg: -1, LatitudeMaxDeg: 1, LongitudeIntervals: []h3filter.LongitudeInterval{{MinDeg: 179, MaxDeg: 180}, {MinDeg: -180, MaxDeg: -179}}}
	cases := []struct {
		name     string
		bounds   h3filter.Bounds
		bbox     [4]float64
		selected bool
	}{
		{name: "overlap", bounds: ordinary, bbox: [4]float64{12, -2, 18, 2}, selected: true},
		{name: "touching", bounds: ordinary, bbox: [4]float64{20, 1, 30, 3}, selected: true},
		{name: "distant", bounds: ordinary, bbox: [4]float64{30, -2, 40, 2}},
		{name: "wrapped cell east", bounds: wrapped, bbox: [4]float64{179.5, -1, 180, 1}, selected: true},
		{name: "wrapped cell west", bounds: wrapped, bbox: [4]float64{-180, -1, -179.5, 1}, selected: true},
		{name: "wrapped asset", bounds: wrapped, bbox: [4]float64{179.5, -1, -179.5, 1}, selected: true},
		{name: "wrapped asset distant", bounds: ordinary, bbox: [4]float64{179, -1, -179, 1}},
		{name: "pole", bounds: h3filter.Bounds{LatitudeMinDeg: 89, LatitudeMaxDeg: 90, LongitudeIntervals: []h3filter.LongitudeInterval{{MinDeg: -180, MaxDeg: 180}}}, bbox: [4]float64{0, 89.5, 1, 90}, selected: true},
		{name: "global", bounds: h3filter.GlobalBounds(), bbox: [4]float64{100, 50, 101, 51}, selected: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot()
			snapshot.Manifest = snapshot.Manifest[:1]
			snapshot.Manifest[0].BBox = test.bbox
			urls, selectedBytes, err := pinnedAssetURLs(snapshot, test.bounds)
			assert.NilError(t, err)
			if test.selected {
				assert.DeepEqual(t, urls, []string{snapshot.Manifest[0].Href})
				assert.Equal(t, selectedBytes, snapshot.Manifest[0].SizeBytes)
			} else {
				assert.Equal(t, len(urls), 0)
				assert.Equal(t, selectedBytes, int64(0))
			}
		})
	}
}

func TestBuildCandidateQuerySelectsFilesAndHandlesEmptyTiles(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected", true: "empty"}[empty], func(t *testing.T) {
			snapshot := testSnapshot()
			snapshot.Manifest[0].BBox = [4]float64{10, 10, 11, 11}
			if empty {
				snapshot.Manifest[1].BBox = snapshot.Manifest[0].BBox
			}
			plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, []string{"id", "geometry", "names"}, snapshot.Schema)
			assert.NilError(t, err)
			query, err := BuildCandidateQuery(plan, snapshot, 7)
			assert.NilError(t, err)
			if empty {
				assert.Equal(t, query.SelectedAssetCount, 0)
				assert.Equal(t, query.SelectedAssetBytes, int64(0))
				assert.Equal(t, query.SQL, `SELECT "id", ST_AsWKB("geometry") AS "geometry", "names" FROM read_parquet([?]) WHERE FALSE`)
				assert.DeepEqual(t, query.Args, []any{snapshot.Manifest[0].Href})
			} else {
				assert.Equal(t, query.SelectedAssetCount, 1)
				assert.Equal(t, query.SelectedAssetBytes, snapshot.Manifest[1].SizeBytes)
				assert.Equal(t, query.Args[0], snapshot.Manifest[1].Href)
			}
		})
	}
}

func TestReportedHelsinkiCellSelectsOnePartition(t *testing.T) {
	snapshot := testSnapshot()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", "stac", "manifest.json"))
	assert.NilError(t, err)
	if err != nil {
		return
	}
	var manifest struct {
		Assets []catalog.Asset `json:"manifest"`
	}
	assert.NilError(t, json.Unmarshal(body, &manifest))
	snapshot.Manifest = manifest.Assets
	assert.Equal(t, len(snapshot.Manifest), 16)
	plan, err := BuildQueryPlan(TileRequest{Cell: "8a1126d33027fff"}, []string{"id", "geometry"}, snapshot.Schema)
	assert.NilError(t, err)
	query, err := BuildCandidateQuery(plan, snapshot, 100000)
	assert.NilError(t, err)
	assert.Equal(t, query.SelectedAssetCount, 1)
	assert.Equal(t, query.SelectedAssetBytes, int64(632751442))
	assetURL, ok := query.Args[0].(string)
	assert.Assert(t, ok)
	assert.Assert(t, strings.Contains(assetURL, "part-00011-"))
}

func TestEmptySelectionPreservesNestedOutputSchema(t *testing.T) {
	connection := openDuckDBConnection(t)
	ctx := context.Background()
	for _, statement := range []string{"SET autoload_known_extensions=false", "SET autoinstall_known_extensions=false", "CREATE MACRO ST_AsWKB(value) AS CAST(value AS BLOB)"} {
		_, err := connection.ExecContext(ctx, statement)
		assert.NilError(t, err)
		if err != nil {
			return
		}
	}
	snapshot := testSnapshot()
	for index := range snapshot.Manifest {
		snapshot.Manifest[index].BBox = [4]float64{10, 10, 11, 11}
	}
	fields := []string{"id", "geometry", "names"}
	plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, fields, snapshot.Schema)
	assert.NilError(t, err)
	query, err := BuildCandidateQuery(plan, snapshot, 7)
	assert.NilError(t, err)
	query.Args[0] = filepath.Join("..", "..", "integration", "testdata", "empty.parquet")
	candidates, err := materializeCandidates(ctx, connection, query, 7)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	defer func() { assert.NilError(t, candidates.drop(ctx, connection)) }()
	assert.Equal(t, candidates.rowCount, int64(0))
	var nestedType string
	err = connection.QueryRowContext(ctx, "SELECT data_type FROM duckdb_columns() WHERE table_name = '__overture_candidates' AND column_name = 'names'").Scan(&nestedType)
	assert.NilError(t, err)
	assert.Assert(t, strings.HasPrefix(nestedType, "STRUCT("), nestedType)
	metadata, err := candidateMetadata(plan)
	assert.NilError(t, err)
	outputPath := filepath.Join(t.TempDir(), "empty-tile.parquet")
	copySQL, err := geoparquet.CopySQL("SELECT * FROM "+candidateTableName, outputPath, metadata)
	assert.NilError(t, err)
	_, err = connection.ExecContext(ctx, copySQL)
	assert.NilError(t, err)
	validation, err := geoparquet.ValidateFile(outputPath, fields, 1<<20)
	assert.NilError(t, err)
	assert.Equal(t, validation.RowCount, int64(0))
}

func TestCandidateBranchUnionAppliesOneGlobalRowLimit(t *testing.T) {
	connection := openDuckDBConnection(t)
	cell := testCell(t)
	center, err := cell.LatLng()
	assert.NilError(t, err)
	assert.NilError(t, h3filter.RegisterCellContains(connection))
	for _, statement := range []string{
		"SET autoload_known_extensions=false",
		"SET autoinstall_known_extensions=false",
		"CREATE MACRO ST_AsWKB(value) AS 'point'::BLOB",
		"CREATE MACRO ST_GeometryType(value) AS 'ST_POINT'",
		"CREATE MACRO ST_X(value) AS value.longitude",
		"CREATE MACRO ST_Y(value) AS value.latitude",
	} {
		_, err := connection.ExecContext(t.Context(), statement)
		assert.NilError(t, err)
	}
	filePath := filepath.Join(t.TempDir(), "source.parquet")
	_, err = connection.ExecContext(t.Context(), `CREATE TABLE source_rows AS
		SELECT i::VARCHAR AS id, struct_pack(latitude := ?, longitude := ?) AS geometry,
		CASE i
		WHEN 0 THEN struct_pack(xmin := ?, xmax := ?, ymin := ?, ymax := ?)
		WHEN 1 THEN struct_pack(xmin := ?, xmax := ?, ymin := ?, ymax := ?)
		WHEN 2 THEN struct_pack(xmin := ?, xmax := ?, ymin := ?, ymax := ?)
		WHEN 3 THEN NULL
		ELSE struct_pack(xmin := 200.0, xmax := 201.0, ymin := 0.0, ymax := 1.0)
		END AS bbox FROM range(5) AS source(i)
	`, center.Lat, center.Lng, center.Lng, center.Lng, center.Lat, center.Lat, center.Lng, center.Lng, center.Lat, center.Lat, 20.0, -20.0, center.Lat, center.Lat)
	assert.NilError(t, err)
	_, err = connection.ExecContext(t.Context(), "COPY source_rows TO ? (FORMAT PARQUET)", filePath)
	assert.NilError(t, err)
	snapshot := testSnapshot()
	snapshot.Manifest = snapshot.Manifest[:1]
	plan, err := BuildQueryPlan(TileRequest{Cell: cell.String()}, []string{"id", "geometry"}, snapshot.Schema)
	assert.NilError(t, err)
	for _, test := range []struct {
		maxRows int64
		tooMany bool
	}{
		{maxRows: 0, tooMany: true},
		{maxRows: 1, tooMany: true},
		{maxRows: 3},
	} {
		query, err := BuildCandidateQuery(plan, snapshot, test.maxRows)
		assert.NilError(t, err)
		for index, arg := range query.Args {
			if arg == snapshot.Manifest[0].Href {
				query.Args[index] = filePath
			}
		}
		rows, err := materializeCandidates(t.Context(), connection, query, test.maxRows)
		if test.tooMany {
			assert.Assert(t, errors.Is(err, ErrTooManyRows))
			tooMany, ok := errors.AsType[*TooManyRowsError](err)
			assert.Assert(t, ok)
			assert.Equal(t, tooMany.ActualRows, test.maxRows+1)
		} else {
			assert.NilError(t, err)
			assert.Equal(t, rows.rowCount, int64(3))
			assert.NilError(t, rows.drop(t.Context(), connection))
		}
	}
}
