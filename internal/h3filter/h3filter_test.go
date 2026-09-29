package h3filter

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

const syntheticPointCount = 50_000

type membershipCase struct {
	name          string
	latitudeDeg   float64
	longitudeDeg  float64
	requestedCell h3.Cell
	want          bool
}

type syntheticPoint struct {
	id             int
	latitudeDeg    float64
	longitudeDeg   float64
	wrappedBBox    bool
	nullBBox       bool
	invalidBBox    bool
}

func TestCellContains(t *testing.T) {
	connection := openDuckDBConnection(t)
	normalCell := mustCell(t, 37.775938728915946, -122.41795063018799, 9)
	pentagons, err := h3.Pentagons(3)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	pentagonCell := pentagons[0]
	pentagonCenter := mustCellCenter(t, pentagonCell)
	poleCell := mustCell(t, 89.0, 45.0, 7)
	poleCenter := mustCellCenter(t, poleCell)
	antimeridianCell := mustAntimeridianCell(t)
	antimeridianCenter := mustCellCenter(t, antimeridianCell)
	normalBoundary, err := h3.CellToBoundary(normalCell)
	assert.NilError(t, err)
	if err != nil {
		return
	}

	cases := []membershipCase{
		{
			name:          "normal center",
			latitudeDeg:   37.775938728915946,
			longitudeDeg:  -122.41795063018799,
			requestedCell: normalCell,
			want:          true,
		},
		{
			name:          "normal neighboring cell center",
			latitudeDeg:   37.776,
			longitudeDeg:  -122.41,
			requestedCell: normalCell,
			want:          false,
		},
		{
			name:          "pentagon center",
			latitudeDeg:   pentagonCenter.Lat,
			longitudeDeg:  pentagonCenter.Lng,
			requestedCell: pentagonCell,
			want:          true,
		},
		{
			name:          "pole center",
			latitudeDeg:   poleCenter.Lat,
			longitudeDeg:  poleCenter.Lng,
			requestedCell: poleCell,
			want:          true,
		},
		{
			name:          "antimeridian center",
			latitudeDeg:   antimeridianCenter.Lat,
			longitudeDeg:  antimeridianCenter.Lng,
			requestedCell: antimeridianCell,
			want:          true,
		},
	}
	for index, vertex := range normalBoundary {
		actualCell, cellErr := h3.LatLngToCell(vertex, normalCell.Resolution())
		assert.NilError(t, cellErr)
		if cellErr != nil {
			continue
		}
		cases = append(cases, membershipCase{
			name:          fmt.Sprintf("boundary vertex %d", index),
			latitudeDeg:   vertex.Lat,
			longitudeDeg:  vertex.Lng,
			requestedCell: normalCell,
			want:          actualCell == normalCell,
		})
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := CellContains(test.latitudeDeg, test.longitudeDeg, test.requestedCell)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, got, test.want)

			var sqlGot bool
			err = connection.QueryRowContext(
				context.Background(),
				"SELECT h3_cell_contains(CAST(? AS DOUBLE), CAST(? AS DOUBLE), CAST(? AS UBIGINT))",
				test.latitudeDeg,
				test.longitudeDeg,
				uint64(test.requestedCell),
			).Scan(&sqlGot)
			assert.NilError(t, err)
			if err == nil {
				assert.Equal(t, sqlGot, test.want)
			}
		})
	}

	t.Run("null input is null", func(t *testing.T) {
		var got sql.NullBool
		err := connection.QueryRowContext(
			context.Background(),
			"SELECT h3_cell_contains(NULL::DOUBLE, 0::DOUBLE, CAST(? AS UBIGINT))",
			uint64(normalCell),
		).Scan(&got)
		assert.NilError(t, err)
		if err == nil {
			assert.Assert(t, !got.Valid)
		}
	})

	t.Run("invalid coordinate is an error", func(t *testing.T) {
		_, err := CellContains(91, 0, normalCell)
		assert.ErrorContains(t, err, "outside WGS84 bounds")
	})
}

func TestCellBoundsAreConservative(t *testing.T) {
	normalCell := mustCell(t, 37.775938728915946, -122.41795063018799, 9)
	pentagons, err := h3.Pentagons(3)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	poleCell := mustCell(t, 89.0, 45.0, 7)
	antimeridianCell := mustAntimeridianCell(t)
	cases := []struct {
		name string
		cell h3.Cell
	}{
		{name: "ordinary cell", cell: normalCell},
		{name: "pentagon", cell: pentagons[0]},
		{name: "pole", cell: poleCell},
		{name: "antimeridian", cell: antimeridianCell},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			bounds, err := CellBounds(test.cell)
			assert.NilError(t, err)
			if err != nil {
				return
			}

			center, err := h3.CellToLatLng(test.cell)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			assert.Assert(t, bounds.ContainsPoint(center.Lat, center.Lng))

			boundary, err := h3.CellToBoundary(test.cell)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			for index, vertex := range boundary {
				assert.Assert(t, bounds.ContainsPoint(vertex.Lat, vertex.Lng), "vertex", index)
				for step := 0; step <= 20; step++ {
					fraction := float64(step) / 20
					latitudeDeg := center.Lat + (vertex.Lat-center.Lat)*fraction
					longitudeDeltaDeg := normalizeLongitude(vertex.Lng - center.Lng)
					longitudeDeg := normalizeLongitude(center.Lng + longitudeDeltaDeg*fraction)
					actualCell, cellErr := h3.LatLngToCell(h3.NewLatLng(latitudeDeg, longitudeDeg), test.cell.Resolution())
					assert.NilError(t, cellErr)
					if cellErr == nil && actualCell == test.cell {
						assert.Assert(t, bounds.ContainsPoint(latitudeDeg, longitudeDeg), "sample", index, step)
					}
				}
			}

			if test.name == "pentagon" {
				assert.Assert(t, bounds.IsGlobal())
			}
			if test.name == "antimeridian" {
				assert.Equal(t, len(bounds.LongitudeIntervals), 2)
			}
		})
	}
}

func TestBoundsIntersectsBBox(t *testing.T) {
	bounds := Bounds{
		LatitudeMinDeg: -10,
		LatitudeMaxDeg: 10,
		LongitudeIntervals: []LongitudeInterval{
			{MinDeg: -180, MaxDeg: -170},
			{MinDeg: 170, MaxDeg: 180},
		},
	}
	cases := []struct {
		name string
		minLongitudeDeg float64
		maxLongitudeDeg float64
		minLatitudeDeg  float64
		maxLatitudeDeg  float64
		want             bool
	}{
		{name: "ordinary overlap", minLongitudeDeg: 175, maxLongitudeDeg: 179, minLatitudeDeg: -1, maxLatitudeDeg: 1, want: true},
		{name: "wrapped overlap", minLongitudeDeg: 179, maxLongitudeDeg: -179, minLatitudeDeg: -1, maxLatitudeDeg: 1, want: true},
		{name: "longitude gap", minLongitudeDeg: -20, maxLongitudeDeg: 20, minLatitudeDeg: -1, maxLatitudeDeg: 1, want: false},
		{name: "latitude gap", minLongitudeDeg: 175, maxLongitudeDeg: 179, minLatitudeDeg: 11, maxLatitudeDeg: 12, want: false},
		{name: "invalid longitude", minLongitudeDeg: math.NaN(), maxLongitudeDeg: 179, minLatitudeDeg: -1, maxLatitudeDeg: 1, want: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := bounds.IntersectsBBox(test.minLongitudeDeg, test.maxLongitudeDeg, test.minLatitudeDeg, test.maxLatitudeDeg)
			assert.Equal(t, got, test.want)
		})
	}
}

func TestBBoxPruningMatchesUnprunedMembership(t *testing.T) {
	connection := openDuckDBConnection(t)
	cells := []h3.Cell{
		mustCell(t, 37.775938728915946, -122.41795063018799, 9),
		mustAntimeridianCell(t),
		mustCell(t, 89.0, 45.0, 7),
	}
	pentagons, err := h3.Pentagons(3)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	cells = append(cells, pentagons[0])

	points := make([]syntheticPoint, 0, len(cells)*8+1)
	pointID := syntheticPointCount + 1
	for _, cell := range cells {
		center := mustCellCenter(t, cell)
		points = append(points, syntheticPoint{
			id:           pointID,
			latitudeDeg:  center.Lat,
			longitudeDeg: center.Lng,
		})
		pointID++
		boundary, boundaryErr := h3.CellToBoundary(cell)
		assert.NilError(t, boundaryErr)
		if boundaryErr != nil {
			return
		}
		for _, vertex := range boundary {
			points = append(points, syntheticPoint{
				id:           pointID,
				latitudeDeg:  vertex.Lat,
				longitudeDeg: vertex.Lng,
			})
			pointID++
		}
	}
	antiCenter := mustCellCenter(t, cells[1])
	points = append(points, syntheticPoint{
		id:           pointID,
		latitudeDeg:  antiCenter.Lat,
		longitudeDeg: antiCenter.Lng,
		wrappedBBox:  true,
	})
	pointID++
	nullBBoxCenter := mustCellCenter(t, cells[0])
	points = append(points, syntheticPoint{
		id:           pointID,
		latitudeDeg:  nullBBoxCenter.Lat,
		longitudeDeg: nullBBoxCenter.Lng,
		nullBBox:     true,
	})
	pointID++
	invalidBBoxCenter := mustCellCenter(t, cells[0])
	points = append(points, syntheticPoint{
		id:           pointID,
		latitudeDeg:  invalidBBoxCenter.Lat,
		longitudeDeg: invalidBBoxCenter.Lng,
		invalidBBox:  true,
	})

	createSyntheticPointTable(t, connection, points)
	var sawPruning bool
	for index, cell := range cells {
		t.Run(fmt.Sprintf("cell_%d_%s", index, cell), func(t *testing.T) {
			bounds, err := CellBounds(cell)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			predicate, predicateArgs := bounds.DuckDBBBoxPredicate()
			unpruned := queryIDs(t, connection, "TRUE", nil, cell)
			pruned := queryIDs(t, connection, predicate, predicateArgs, cell)
			assert.DeepEqual(t, pruned, unpruned)

			candidateCount := countCandidates(t, connection, predicate, predicateArgs)
			assert.Assert(t, candidateCount >= int64(len(unpruned)))
			if !bounds.IsGlobal() {
				assert.Assert(t, candidateCount < syntheticPointCount+int64(len(points)))
				sawPruning = true
			}
		})
	}
	assert.Assert(t, sawPruning)
}

func BenchmarkCellMembershipPruning(b *testing.B) {
	connection := openDuckDBConnection(b)
	createSyntheticPointTable(b, connection, nil)
	cell := mustCell(b, 37.775938728915946, -122.41795063018799, 3)
	bounds := mustCellBounds(b, cell)
	predicate, predicateArgs := bounds.DuckDBBBoxPredicate()
	candidateCount := countCandidates(b, connection, predicate, predicateArgs)

	b.Run("unpruned", func(b *testing.B) {
		b.ReportMetric(float64(syntheticPointCount), "candidate_rows")
		for range b.N {
			countMembership(b, connection, "TRUE", nil, cell)
		}
	})
	b.Run("pruned", func(b *testing.B) {
		b.ReportMetric(float64(candidateCount), "candidate_rows")
		for range b.N {
			countMembership(b, connection, predicate, predicateArgs, cell)
		}
	})
}

func openDuckDBConnection(t testing.TB) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	connection, err := db.Conn(context.Background())
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		assert.NilError(t, connection.Close())
	})
	assert.NilError(t, RegisterCellContains(connection))
	return connection
}

func createSyntheticPointTable(t testing.TB, connection *sql.Conn, points []syntheticPoint) {
	t.Helper()
	_, err := connection.ExecContext(context.Background(), `
		CREATE TABLE points AS
		SELECT
			i::INTEGER AS id,
			(-89.5 + ((i * 37) % 178)::DOUBLE) AS latitude,
			(-179.5 + ((i * 71) % 358)::DOUBLE) AS longitude,
			struct_pack(
				xmin := (-179.5 + ((i * 71) % 358)::DOUBLE),
				xmax := (-179.5 + ((i * 71) % 358)::DOUBLE),
				ymin := (-89.5 + ((i * 37) % 178)::DOUBLE),
				ymax := (-89.5 + ((i * 37) % 178)::DOUBLE)
			) AS bbox
		FROM range(?) AS source(i)`, syntheticPointCount)
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}

	for _, point := range points {
		if point.nullBBox {
			_, err = connection.ExecContext(
				context.Background(),
				"INSERT INTO points (id, latitude, longitude, bbox) VALUES (?, ?, ?, NULL)",
				point.id,
				point.latitudeDeg,
				point.longitudeDeg,
			)
		} else if point.invalidBBox {
			_, err = connection.ExecContext(
				context.Background(),
				"INSERT INTO points (id, latitude, longitude, bbox) VALUES (?, ?, ?, struct_pack(xmin := 200.0, xmax := 201.0, ymin := 20.0, ymax := 21.0))",
				point.id,
				point.latitudeDeg,
				point.longitudeDeg,
			)
		} else if point.wrappedBBox {
			_, err = connection.ExecContext(
				context.Background(),
				"INSERT INTO points (id, latitude, longitude, bbox) VALUES (?, ?, ?, struct_pack(xmin := 179.0, xmax := -179.0, ymin := ?, ymax := ?))",
				point.id,
				point.latitudeDeg,
				point.longitudeDeg,
				point.latitudeDeg,
				point.latitudeDeg,
			)
		} else {
			_, err = connection.ExecContext(
				context.Background(),
				"INSERT INTO points (id, latitude, longitude, bbox) VALUES (?, ?, ?, struct_pack(xmin := ?, xmax := ?, ymin := ?, ymax := ?))",
				point.id,
				point.latitudeDeg,
				point.longitudeDeg,
				point.longitudeDeg,
				point.longitudeDeg,
				point.latitudeDeg,
				point.latitudeDeg,
			)
		}
		assert.NilError(t, err)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func queryIDs(t testing.TB, connection *sql.Conn, predicate string, predicateArgs []any, cell h3.Cell) []int64 {
	t.Helper()
	args := append([]any(nil), predicateArgs...)
	args = append(args, uint64(cell))
	rows, err := connection.QueryContext(
		context.Background(),
		fmt.Sprintf("SELECT id FROM points WHERE %s AND h3_cell_contains(latitude, longitude, CAST(? AS UBIGINT)) ORDER BY id", predicate),
		args...,
	)
	assert.NilError(t, err)
	if err != nil {
		return nil
	}
	defer func() { assert.NilError(t, rows.Close()) }()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		assert.NilError(t, err)
		if err != nil {
			return nil
		}
		ids = append(ids, id)
	}
	assert.NilError(t, rows.Err())
	return ids
}

func countCandidates(t testing.TB, connection *sql.Conn, predicate string, predicateArgs []any) int64 {
	t.Helper()
	var count int64
	err := connection.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT count(*) FROM points WHERE %s", predicate),
		predicateArgs...,
	).Scan(&count)
	assert.NilError(t, err)
	return count
}

func countMembership(t testing.TB, connection *sql.Conn, predicate string, predicateArgs []any, cell h3.Cell) {
	t.Helper()
	args := append([]any(nil), predicateArgs...)
	args = append(args, uint64(cell))
	var count int64
	err := connection.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT count(*) FROM points WHERE %s AND h3_cell_contains(latitude, longitude, CAST(? AS UBIGINT))", predicate),
		args...,
	).Scan(&count)
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
}

func mustCell(t testing.TB, latitudeDeg, longitudeDeg float64, resolution int) h3.Cell {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(latitudeDeg, longitudeDeg), resolution)
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	return cell
}

func mustCellCenter(t testing.TB, cell h3.Cell) h3.LatLng {
	t.Helper()
	center, err := h3.CellToLatLng(cell)
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	return center
}

func mustCellBounds(t testing.TB, cell h3.Cell) Bounds {
	t.Helper()
	bounds, err := CellBounds(cell)
	assert.NilError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	return bounds
}

func mustAntimeridianCell(t testing.TB) h3.Cell {
	t.Helper()
	for resolution := 1; resolution <= h3.MaxResolution; resolution++ {
		for longitudeDeg := 179.9; longitudeDeg <= 180; longitudeDeg += 0.01 {
			cell := mustCell(t, 0, longitudeDeg, resolution)
			bounds := mustCellBounds(t, cell)
			if len(bounds.LongitudeIntervals) == 2 {
				return cell
			}
		}
	}
	t.Fatal("could not find an H3 cell whose conservative bounds cross the antimeridian")
	return 0
}
