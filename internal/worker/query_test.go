package worker

import (
	"errors"
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

func testCell(t testing.TB) h3.Cell {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), 9)
	assert.NilError(t, err)
	return cell
}
