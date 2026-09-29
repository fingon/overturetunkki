package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/geoparquet"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestParseCanonicalCell(t *testing.T) {
	cell := testCell(t)
	canonical := cell.String()
	cases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "empty", wantErr: "valid H3 cell"},
		{name: "prefixed", value: "0x" + canonical, wantErr: "not canonical"},
		{name: "uppercase", value: strings.ToUpper(canonical), wantErr: "not canonical"},
		{name: "whitespace", value: " " + canonical, wantErr: "not a valid H3 cell"},
		{name: "invalid", value: "ffffffffffffffff", wantErr: "not a valid H3 cell"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseCanonicalCell(test.value)
			assert.ErrorContains(t, err, test.wantErr)
			assert.Assert(t, errors.Is(err, ErrInvalidCell))
			assert.Equal(t, uint64(got), uint64(0))
		})
	}

	got, err := ParseCanonicalCell(canonical)
	assert.NilError(t, err)
	assert.Equal(t, got, cell)
}

func TestBuildQueryPlanValidatesProjection(t *testing.T) {
	cell := testCell(t)
	schema := catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: "VARCHAR"},
			{Name: "geometry", Type: "GEOMETRY"},
			{Name: "names", Type: "STRUCT"},
		},
		GeoParquetVersion: "1.1.0",
		PrimaryGeometry:   geoparquet.GeometryColumnName,
	}
	plan, err := BuildQueryPlan(TileRequest{Cell: cell.String()}, []string{"names", "id", "geometry"}, schema)
	assert.NilError(t, err)
	assert.Equal(t, plan.Cell, cell)
	assert.Equal(t, plan.CellText, cell.String())
	assert.Equal(t, plan.CellValue, uint64(cell))
	assert.Equal(t, len(plan.Columns), 3)
	assert.Equal(t, plan.Columns[0].Name, "names")
	assert.Equal(t, plan.ProjectionSQL, `"names", "id", "geometry"`)
}

func TestBuildQueryPlanRejectsUntrustedProjectionInput(t *testing.T) {
	schema := catalog.Schema{
		Columns:         []catalog.Column{{Name: "id"}, {Name: "geometry"}, {Name: "names"}},
		PrimaryGeometry: geoparquet.GeometryColumnName,
	}
	cases := []struct {
		name    string
		fields  []string
		schema  catalog.Schema
		message string
	}{
		{name: "empty fields", message: "must not be empty"},
		{name: "missing id", fields: []string{"geometry"}, message: `include "id"`},
		{name: "missing geometry", fields: []string{"id"}, message: `include "geometry"`},
		{name: "unknown field", fields: []string{"id", "geometry", "secret"}, message: "not in the catalog schema"},
		{name: "duplicate field", fields: []string{"id", "geometry", "id"}, message: "is repeated"},
		{name: "SQL expression", fields: []string{"id", "geometry", "names OR TRUE"}, message: "simple identifier"},
		{name: "duplicate schema column", fields: []string{"id", "geometry"}, schema: catalog.Schema{Columns: []catalog.Column{{Name: "id"}, {Name: "id"}, {Name: "geometry"}}, PrimaryGeometry: geoparquet.GeometryColumnName}, message: "schema column \"id\" is repeated"},
		{name: "invalid schema column", fields: []string{"id", "geometry"}, schema: catalog.Schema{Columns: []catalog.Column{{Name: "id"}, {Name: "geometry; DROP TABLE places"}}, PrimaryGeometry: geoparquet.GeometryColumnName}, message: "schema column"},
		{name: "wrong primary geometry", fields: []string{"id", "geometry"}, schema: catalog.Schema{Columns: []catalog.Column{{Name: "id"}, {Name: "geometry"}}, PrimaryGeometry: "shape"}, message: "primary geometry"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			caseSchema := test.schema
			if caseSchema.Columns == nil {
				caseSchema = schema
			}
			_, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, test.fields, caseSchema)
			assert.ErrorContains(t, err, "invalid source projection")
			assert.ErrorContains(t, err, test.message)
			assert.Assert(t, errors.Is(err, ErrInvalidProjection))
		})
	}
}

func TestBuildCandidateQueryBindsManifestAndFiltersExactly(t *testing.T) {
	cell := testCell(t)
	snapshot := testSnapshot()
	plan, err := BuildQueryPlan(TileRequest{Cell: cell.String()}, []string{"id", "geometry", "names"}, snapshot.Schema)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	query, err := BuildCandidateQuery(plan, snapshot, 7)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	assert.Assert(t, strings.Contains(query.SQL, "FROM read_parquet([?, ?])"), query.SQL)
	assert.Assert(t, strings.Contains(query.SQL, `ST_AsWKB("geometry") AS "geometry"`), query.SQL)
	assert.Assert(t, strings.Contains(query.SQL, "ST_GeometryType(\"geometry\")"), query.SQL)
	assert.Assert(t, strings.Contains(query.SQL, "h3_cell_contains"), query.SQL)
	assert.Assert(t, strings.Contains(query.SQL, "LIMIT ?"), query.SQL)
	assert.Assert(t, !strings.Contains(query.SQL, snapshot.Manifest[0].Href), query.SQL)
	assert.Equal(t, query.Args[0], snapshot.Manifest[0].Href)
	assert.Equal(t, query.Args[1], snapshot.Manifest[1].Href)
	assert.Equal(t, query.Args[len(query.Args)-2], uint64(cell))
	assert.Equal(t, query.Args[len(query.Args)-1], int64(8))
}

func TestBuildCandidateQueryRejectsUntrustedManifest(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*catalog.Snapshot)
		message string
	}{
		{name: "empty manifest", mutate: func(snapshot *catalog.Snapshot) { snapshot.Manifest = nil }, message: "manifest is empty"},
		{name: "wrong host", mutate: func(snapshot *catalog.Snapshot) {
			snapshot.Manifest[0].Href = "https://example.com/release/test/theme=places/type=place/part-00000-a.zstd.parquet"
		}, message: "trusted HTTPS asset"},
		{name: "query string", mutate: func(snapshot *catalog.Snapshot) { snapshot.Manifest[0].Href += "?version=1" }, message: "trusted HTTPS asset"},
		{name: "wrong path", mutate: func(snapshot *catalog.Snapshot) {
			snapshot.Manifest[0].Href = strings.Replace(snapshot.Manifest[0].Href, "/theme=places/", "/theme=building/", 1)
		}, message: "trusted places prefix"},
		{name: "repeated URL", mutate: func(snapshot *catalog.Snapshot) {
			snapshot.Manifest[1].PartitionID = snapshot.Manifest[0].PartitionID
			snapshot.Manifest[1].Href = snapshot.Manifest[0].Href
		}, message: "URL is repeated"},
		{name: "invalid metadata", mutate: func(snapshot *catalog.Snapshot) { snapshot.Manifest[0].SizeBytes = 0 }, message: "metadata is invalid"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot()
			test.mutate(&snapshot)
			plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, []string{"id", "geometry"}, snapshot.Schema)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			_, err = BuildCandidateQuery(plan, snapshot, 1)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestBuildCandidateQueryValidatesRowLimit(t *testing.T) {
	plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, []string{"id", "geometry"}, testSnapshot().Schema)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	for _, maxRows := range []int64{-1, int64(1<<63 - 1)} {
		_, err := BuildCandidateQuery(plan, testSnapshot(), maxRows)
		assert.ErrorContains(t, err, "candidate row limit")
	}
}

func TestMaterializeCandidatesUsesOnlyTheConfiguredRowAllowance(t *testing.T) {
	connection := openDuckDBConnection(t)
	query := PreparedQuery{
		SQL:  "SELECT i::BIGINT AS id, repeat('x', 1) AS geometry FROM range(?) AS source(i)",
		Args: []any{int64(3)},
	}
	_, err := materializeCandidates(context.Background(), connection, query, 2)
	assert.Assert(t, errors.Is(err, ErrTooManyRows))
	assert.ErrorContains(t, err, "produced 3 rows")
	var count int64
	err = connection.QueryRowContext(context.Background(), "SELECT count(*) FROM "+candidateTableName).Scan(&count)
	assert.Assert(t, err != nil)

	query.Args = []any{int64(2)}
	rows, err := materializeCandidates(context.Background(), connection, query, 2)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	t.Cleanup(func() { assert.NilError(t, rows.drop(context.Background(), connection)) })
	assert.Equal(t, rows.rowCount, int64(2))
	err = connection.QueryRowContext(context.Background(), "SELECT count(*) FROM "+candidateTableName).Scan(&count)
	assert.NilError(t, err)
	assert.Equal(t, count, int64(2))
}

func TestCandidateMetadataUsesConservativeCellBounds(t *testing.T) {
	plan, err := BuildQueryPlan(TileRequest{Cell: testCell(t).String()}, []string{"id", "geometry"}, testSnapshot().Schema)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	encoded, err := candidateMetadata(plan)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	var metadata geoparquet.GeoMetadata
	assert.NilError(t, json.Unmarshal(encoded, &metadata))
	geometry, ok := metadata.Columns[geoparquet.GeometryColumnName]
	assert.Assert(t, ok)
	if !ok {
		return
	}
	assert.Equal(t, metadata.Version, geoparquet.GeoParquetVersion)
	assert.Equal(t, metadata.PrimaryColumn, geoparquet.GeometryColumnName)
	assert.Equal(t, geometry.Encoding, geoparquet.WKBEncoding)
	assert.Equal(t, len(geometry.BBox), 4)
	assert.Assert(t, geometry.BBox[0] < geometry.BBox[2])
	assert.Assert(t, geometry.BBox[1] < geometry.BBox[3])
}

func testSnapshot() catalog.Snapshot {
	return catalog.Snapshot{
		Release:        "2026-09-23.1",
		CatalogVersion: "2026-09-23.1+sha256:test",
		CollectionID:   catalog.DefaultCollectionID,
		Manifest: []catalog.Asset{
			{
				PartitionID:   "00000",
				Href:          fmt.Sprintf("https://%s/release/2026-09-23.1/theme=places/type=place/part-00000-a%s", catalog.DefaultAssetHost, assetPathSuffix),
				RowCount:      10,
				RowGroupCount: 1,
				SizeBytes:     100,
			},
			{
				PartitionID:   "00001",
				Href:          fmt.Sprintf("https://%s/release/2026-09-23.1/theme=places/type=place/part-00001-b%s", catalog.DefaultAssetHost, assetPathSuffix),
				RowCount:      20,
				RowGroupCount: 2,
				SizeBytes:     200,
			},
		},
		Schema: catalog.Schema{
			Columns:           []catalog.Column{{Name: "id"}, {Name: "geometry"}, {Name: "names"}},
			GeoParquetVersion: "1.1.0",
			PrimaryGeometry:   geoparquet.GeometryColumnName,
		},
	}
}

func testCell(t testing.TB) h3.Cell {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(t, err)
	return cell
}
