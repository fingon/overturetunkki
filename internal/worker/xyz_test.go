//nolint:goconst // Independent geographic cases keep expected IDs readable.
package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

func TestSquareAncestorFilteringOwnsEveryBoundaryOnce(t *testing.T) {
	extensionDirectory := os.Getenv("OVERTURE_DUCKDB_EXTENSION_DIRECTORY")
	if extensionDirectory == "" {
		t.Skip("requires bundled spatial extension")
	}
	connection := openDuckDBConnection(t)
	_, err := connection.ExecContext(t.Context(), "SET extension_directory = ?", extensionDirectory)
	assert.NilError(t, err)
	_, err = connection.ExecContext(t.Context(), "LOAD spatial")
	assert.NilError(t, err)
	fixture, err := os.ReadFile(filepath.Join("testdata", "xyz-points.sql"))
	assert.NilError(t, err)
	_, err = connection.ExecContext(t.Context(), "CREATE TABLE points AS "+string(fixture))
	assert.NilError(t, err)
	fields := []string{"id", "geometry", "names"}
	world := tilequery.Square{}
	plan, err := BuildQueryPlan(TileRequest{Square: &world}, fields, testSnapshot().Schema)
	assert.NilError(t, err)
	metadata, err := candidateMetadata(plan)
	assert.NilError(t, err)
	parentPath := filepath.Join(t.TempDir(), "parent.parquet")
	copySQL, err := geoparquet.CopySQL("SELECT "+candidateProjectionSQL(plan.Columns)+" FROM points WHERE "+plan.membershipSQL(), parentPath, metadata)
	assert.NilError(t, err)
	_, err = connection.ExecContext(t.Context(), copySQL, plan.membershipArgs()...)
	assert.NilError(t, err)
	for _, test := range []struct {
		name   string
		square tilequery.Square
		ids    string
	}{
		{"world", world, "east,ne,nw,origin,se,sw,west"},
		{"northwest", tilequery.Square{Z: 1}, "nw"},
		{"northeast", tilequery.Square{Z: 1, X: 1}, "ne"},
		{"southwest", tilequery.Square{Z: 1, Y: 1}, "east,sw,west"},
		{"southeast", tilequery.Square{Z: 1, X: 1, Y: 1}, "origin,se"},
	} {
		t.Run(test.name, func(t *testing.T) {
			childPlan, planErr := BuildQueryPlan(TileRequest{Square: &test.square}, fields, testSnapshot().Schema)
			assert.NilError(t, planErr)
			query, queryErr := BuildCachedCandidateQuery(childPlan, []string{parentPath}, 100)
			assert.NilError(t, queryErr)
			var ids string
			scanErr := connection.QueryRowContext(t.Context(), "SELECT string_agg(id, ',' ORDER BY id) FROM ("+query.SQL+")", query.Args...).Scan(&ids)
			assert.NilError(t, scanErr)
			assert.Equal(t, ids, test.ids)
		})
	}
}

func TestSquareUpstreamQueryIncludesNormalizedAntimeridian(t *testing.T) {
	square := tilequery.Square{Z: 10, Y: 512}
	plan, err := BuildQueryPlan(TileRequest{Square: &square}, []string{"id", "geometry"}, testSnapshot().Schema)
	assert.NilError(t, err)
	bounds, err := plan.bounds()
	assert.NilError(t, err)
	assert.Assert(t, bounds.ContainsPoint(0, 180))
	query, err := BuildCandidateQuery(plan, testSnapshot(), 100)
	assert.NilError(t, err)
	assert.Equal(t, query.SelectedAssetCount, 2)
	assert.Equal(t, query.Args[len(query.Args)-1], int64(101))
	assert.Assert(t, len(query.SQL) > 0)
}

func TestPolarEmptyQueryPreservesSourceSchemaWithoutRows(t *testing.T) {
	extensionDirectory := os.Getenv("OVERTURE_DUCKDB_EXTENSION_DIRECTORY")
	if extensionDirectory == "" {
		t.Skip("requires bundled spatial extension")
	}
	connection := openDuckDBConnection(t)
	_, err := connection.ExecContext(t.Context(), "SET extension_directory = ?", extensionDirectory)
	assert.NilError(t, err)
	_, err = connection.ExecContext(t.Context(), "LOAD spatial")
	assert.NilError(t, err)
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 90}, 9)
	assert.NilError(t, err)
	snapshot := testSnapshot()
	plan, err := BuildQueryPlan(TileRequest{Cell: cell.String()}, []string{"id", "geometry", "names"}, snapshot.Schema)
	assert.NilError(t, err)
	assert.Assert(t, plan.Empty)
	query, err := PrepareTileQuery(plan, snapshot, nil, 10)
	assert.NilError(t, err)
	assert.Equal(t, len(query.Args), 1)
	query.Args[0] = filepath.Join("..", "..", "integration", "testdata", "places.parquet")
	candidates, err := materializeCandidates(t.Context(), connection, query, 10)
	assert.NilError(t, err)
	defer func() { assert.NilError(t, candidates.drop(t.Context(), connection)) }()
	assert.Equal(t, candidates.rowCount, int64(0))
	var nestedType string
	err = connection.QueryRowContext(t.Context(), "SELECT data_type FROM duckdb_columns() WHERE table_name = '__overture_candidates' AND column_name = 'names'").Scan(&nestedType)
	assert.NilError(t, err)
	assert.Assert(t, len(nestedType) > 0)
}
