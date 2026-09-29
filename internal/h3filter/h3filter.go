package h3filter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/uber/h3-go/v4"
)

const (
	CellContainsFunctionName = "h3_cell_contains"
	minLatitudeDeg           = -90.0
	maxLatitudeDeg           = 90.0
	minLongitudeDeg          = -180.0
	maxLongitudeDeg          = 180.0
	angularMarginRad         = 1e-12
)

type LongitudeInterval struct {
	MinDeg float64
	MaxDeg float64
}

type Bounds struct {
	LatitudeMinDeg     float64
	LatitudeMaxDeg     float64
	LongitudeIntervals []LongitudeInterval
}

func GlobalBounds() Bounds {
	return Bounds{
		LatitudeMinDeg: minLatitudeDeg,
		LatitudeMaxDeg: maxLatitudeDeg,
		LongitudeIntervals: []LongitudeInterval{{
			MinDeg: minLongitudeDeg,
			MaxDeg: maxLongitudeDeg,
		}},
	}
}

func CellBounds(cell h3.Cell) (Bounds, error) {
	if !cell.IsValid() {
		return Bounds{}, fmt.Errorf("cell %d is invalid: %w", uint64(cell), h3.ErrCellInvalid)
	}

	faces, err := cell.IcosahedronFaces()
	if err != nil {
		return Bounds{}, fmt.Errorf("find icosahedron faces for cell %s: %w", cell, err)
	}
	if cell.IsPentagon() || len(faces) != 1 {
		return GlobalBounds(), nil
	}

	center, err := h3.CellToLatLng(cell)
	if err != nil {
		return Bounds{}, fmt.Errorf("find center for cell %s: %w", cell, err)
	}
	boundary, err := h3.CellToBoundary(cell)
	if err != nil {
		return Bounds{}, fmt.Errorf("find boundary for cell %s: %w", cell, err)
	}
	if len(boundary) == 0 {
		return Bounds{}, fmt.Errorf("cell %s has no boundary vertices", cell)
	}

	maxCenterDistanceRad := 0.0
	maxEdgeDistanceRad := 0.0
	for index, vertex := range boundary {
		centerDistanceRad := h3.GreatCircleDistanceRads(center, vertex)
		if !isFinite(centerDistanceRad) {
			return Bounds{}, fmt.Errorf("cell %s has a non-finite center distance", cell)
		}
		if centerDistanceRad > maxCenterDistanceRad {
			maxCenterDistanceRad = centerDistanceRad
		}

		nextVertex := boundary[(index+1)%len(boundary)]
		edgeDistanceRad := h3.GreatCircleDistanceRads(vertex, nextVertex)
		if !isFinite(edgeDistanceRad) {
			return Bounds{}, fmt.Errorf("cell %s has a non-finite edge distance", cell)
		}
		if edgeDistanceRad > maxEdgeDistanceRad {
			maxEdgeDistanceRad = edgeDistanceRad
		}
	}

	radiusRad := maxCenterDistanceRad + maxEdgeDistanceRad + angularMarginRad
	latitudeRadiusDeg := radiusRad * h3.RadsToDegs
	latitudeMinDeg := max(minLatitudeDeg, center.Lat-latitudeRadiusDeg)
	latitudeMaxDeg := min(maxLatitudeDeg, center.Lat+latitudeRadiusDeg)
	if radiusRad >= math.Pi || latitudeRadiusDeg >= 180 {
		return GlobalBounds(), nil
	}

	centerLatitudeRad := center.Lat * h3.DegsToRads
	if centerLatitudeRad-radiusRad <= -math.Pi/2 || centerLatitudeRad+radiusRad >= math.Pi/2 {
		return Bounds{
			LatitudeMinDeg:     latitudeMinDeg,
			LatitudeMaxDeg:     latitudeMaxDeg,
			LongitudeIntervals: []LongitudeInterval{{MinDeg: minLongitudeDeg, MaxDeg: maxLongitudeDeg}},
		}, nil
	}

	cosCenterLatitude := math.Cos(center.Lat * h3.DegsToRads)
	if cosCenterLatitude <= 0 || !isFinite(cosCenterLatitude) {
		return Bounds{
			LatitudeMinDeg:     latitudeMinDeg,
			LatitudeMaxDeg:     latitudeMaxDeg,
			LongitudeIntervals: []LongitudeInterval{{MinDeg: minLongitudeDeg, MaxDeg: maxLongitudeDeg}},
		}, nil
	}

	sineRadius := math.Sin(radiusRad)
	longitudeRatio := sineRadius / cosCenterLatitude
	if longitudeRatio >= 1 || longitudeRatio <= -1 || !isFinite(longitudeRatio) {
		return Bounds{
			LatitudeMinDeg:     latitudeMinDeg,
			LatitudeMaxDeg:     latitudeMaxDeg,
			LongitudeIntervals: []LongitudeInterval{{MinDeg: minLongitudeDeg, MaxDeg: maxLongitudeDeg}},
		}, nil
	}

	halfLongitudeSpanDeg := math.Asin(longitudeRatio) * h3.RadsToDegs
	if halfLongitudeSpanDeg >= 180 || !isFinite(halfLongitudeSpanDeg) {
		return Bounds{
			LatitudeMinDeg:     latitudeMinDeg,
			LatitudeMaxDeg:     latitudeMaxDeg,
			LongitudeIntervals: []LongitudeInterval{{MinDeg: minLongitudeDeg, MaxDeg: maxLongitudeDeg}},
		}, nil
	}

	centerLongitudeDeg := normalizeLongitude(center.Lng)
	longitudeMinDeg := centerLongitudeDeg - halfLongitudeSpanDeg
	longitudeMaxDeg := centerLongitudeDeg + halfLongitudeSpanDeg
	intervals := []LongitudeInterval{{MinDeg: longitudeMinDeg, MaxDeg: longitudeMaxDeg}}
	if longitudeMinDeg < minLongitudeDeg {
		intervals = []LongitudeInterval{
			{MinDeg: minLongitudeDeg, MaxDeg: longitudeMaxDeg},
			{MinDeg: longitudeMinDeg + 360, MaxDeg: maxLongitudeDeg},
		}
	} else if longitudeMaxDeg > maxLongitudeDeg {
		intervals = []LongitudeInterval{
			{MinDeg: minLongitudeDeg, MaxDeg: longitudeMaxDeg - 360},
			{MinDeg: longitudeMinDeg, MaxDeg: maxLongitudeDeg},
		}
	}

	return Bounds{
		LatitudeMinDeg:     latitudeMinDeg,
		LatitudeMaxDeg:     latitudeMaxDeg,
		LongitudeIntervals: intervals,
	}, nil
}

func (bounds Bounds) ContainsPoint(latitudeDeg, longitudeDeg float64) bool {
	if !isFinite(latitudeDeg) || !isFinite(longitudeDeg) {
		return false
	}
	if latitudeDeg < bounds.LatitudeMinDeg || latitudeDeg > bounds.LatitudeMaxDeg {
		return false
	}

	longitudeDeg = normalizeLongitude(longitudeDeg)
	for _, interval := range bounds.LongitudeIntervals {
		if longitudeDeg >= interval.MinDeg && longitudeDeg <= interval.MaxDeg {
			return true
		}
	}
	return false
}

func (bounds Bounds) IntersectsBBox(bboxMinLongitudeDeg, bboxMaxLongitudeDeg, bboxMinLatitudeDeg, bboxMaxLatitudeDeg float64) bool {
	if bounds.IsGlobal() {
		return true
	}
	if !isFinite(bboxMinLongitudeDeg) || !isFinite(bboxMaxLongitudeDeg) ||
		!isFinite(bboxMinLatitudeDeg) || !isFinite(bboxMaxLatitudeDeg) ||
		bboxMinLongitudeDeg < minLongitudeDeg || bboxMaxLongitudeDeg > maxLongitudeDeg ||
		bboxMinLatitudeDeg < minLatitudeDeg || bboxMaxLatitudeDeg > maxLatitudeDeg ||
		bboxMinLatitudeDeg > bboxMaxLatitudeDeg {
		return true
	}
	if bboxMaxLatitudeDeg < bounds.LatitudeMinDeg || bboxMinLatitudeDeg > bounds.LatitudeMaxDeg {
		return false
	}

	bboxIntervals := []LongitudeInterval{{MinDeg: bboxMinLongitudeDeg, MaxDeg: bboxMaxLongitudeDeg}}
	if bboxMinLongitudeDeg > bboxMaxLongitudeDeg {
		bboxIntervals = []LongitudeInterval{
			{MinDeg: bboxMinLongitudeDeg, MaxDeg: maxLongitudeDeg},
			{MinDeg: minLongitudeDeg, MaxDeg: bboxMaxLongitudeDeg},
		}
	}
	for _, boundsInterval := range bounds.LongitudeIntervals {
		for _, bboxInterval := range bboxIntervals {
			if boundsInterval.MaxDeg >= bboxInterval.MinDeg && boundsInterval.MinDeg <= bboxInterval.MaxDeg {
				return true
			}
		}
	}
	return false
}

func (bounds Bounds) IsGlobal() bool {
	return bounds.LatitudeMinDeg <= minLatitudeDeg && bounds.LatitudeMaxDeg >= maxLatitudeDeg &&
		len(bounds.LongitudeIntervals) == 1 &&
		bounds.LongitudeIntervals[0].MinDeg <= minLongitudeDeg &&
		bounds.LongitudeIntervals[0].MaxDeg >= maxLongitudeDeg
}

func (bounds Bounds) DuckDBBBoxPredicate() (string, []any) {
	if bounds.IsGlobal() || len(bounds.LongitudeIntervals) == 0 {
		return "TRUE", nil
	}

	longitudePredicates := make([]string, 0, len(bounds.LongitudeIntervals))
	args := make([]any, 0, 2+len(bounds.LongitudeIntervals)*4)
	args = append(args, bounds.LatitudeMinDeg, bounds.LatitudeMaxDeg)
	for range bounds.LongitudeIntervals {
		longitudePredicates = append(longitudePredicates,
			"((bbox.xmin <= bbox.xmax AND bbox.xmax >= ? AND bbox.xmin <= ?) OR "+
				"(bbox.xmin > bbox.xmax AND (bbox.xmin <= ? OR bbox.xmax >= ?)))")
	}
	for _, interval := range bounds.LongitudeIntervals {
		args = append(args, interval.MinDeg, interval.MaxDeg, interval.MaxDeg, interval.MinDeg)
	}

	return "(bbox.xmin IS NULL OR bbox.xmax IS NULL OR bbox.ymin IS NULL OR bbox.ymax IS NULL OR " +
		"bbox.ymin > bbox.ymax OR bbox.xmin < -180 OR bbox.xmin > 180 OR " +
		"bbox.xmax < -180 OR bbox.xmax > 180 OR bbox.ymin < -90 OR " +
		"bbox.ymin > 90 OR bbox.ymax < -90 OR bbox.ymax > 90 OR " +
		"(bbox.ymax >= ? AND bbox.ymin <= ? AND (" + joinPredicates(longitudePredicates) + "))) ", args
}

func CellContains(latitudeDeg, longitudeDeg float64, requestedCell h3.Cell) (bool, error) {
	if !requestedCell.IsValid() {
		return false, fmt.Errorf("requested cell %d is invalid: %w", uint64(requestedCell), h3.ErrCellInvalid)
	}
	if !isFinite(latitudeDeg) || !isFinite(longitudeDeg) ||
		latitudeDeg < minLatitudeDeg || latitudeDeg > maxLatitudeDeg ||
		longitudeDeg < minLongitudeDeg || longitudeDeg > maxLongitudeDeg {
		return false, fmt.Errorf("coordinate latitude=%v longitude=%v is outside WGS84 bounds: %w", latitudeDeg, longitudeDeg, h3.ErrLatLngDomain)
	}

	actualCell, err := h3.LatLngToCell(h3.NewLatLng(latitudeDeg, longitudeDeg), requestedCell.Resolution())
	if err != nil {
		return false, fmt.Errorf("convert latitude=%v longitude=%v to H3: %w", latitudeDeg, longitudeDeg, err)
	}
	return actualCell == requestedCell, nil
}

func RegisterCellContains(conn *sql.Conn) error {
	if conn == nil {
		return fmt.Errorf("register %s: nil DuckDB connection", CellContainsFunctionName)
	}

	var function *cellContainsUDF
	if err := duckdb.RegisterScalarUDF(conn, CellContainsFunctionName, function); err != nil {
		return fmt.Errorf("register DuckDB function %s: %w", CellContainsFunctionName, err)
	}
	return nil
}

type cellContainsUDF struct{}

func (*cellContainsUDF) Config() duckdb.ScalarFuncConfig {
	doubleType, err := duckdb.NewTypeInfo(duckdb.TYPE_DOUBLE)
	if err != nil {
		panic(err)
	}
	cellType, err := duckdb.NewTypeInfo(duckdb.TYPE_UBIGINT)
	if err != nil {
		panic(err)
	}
	booleanType, err := duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	if err != nil {
		panic(err)
	}
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{doubleType, doubleType, cellType},
		ResultTypeInfo: booleanType,
	}
}

func (*cellContainsUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		ChunkContextExecutor: func(ctx context.Context, chunk *duckdb.ChunkIteratorState) error {
			for row, err := range chunk.Rows() {
				if err != nil {
					return fmt.Errorf("read H3 UDF input row: %w", err)
				}
				latitudeDeg, err := float64Value(*row.GetValuePtr(0), "latitude")
				if err != nil {
					return err
				}
				longitudeDeg, err := float64Value(*row.GetValuePtr(1), "longitude")
				if err != nil {
					return err
				}
				cellValue, ok := (*row.GetValuePtr(2)).(uint64)
				if !ok {
					return fmt.Errorf("H3 UDF requested_cell has type %T, want uint64", *row.GetValuePtr(2))
				}
				requestedCell, err := cellFromUint64(cellValue)
				if err != nil {
					return err
				}
				contains, err := CellContains(latitudeDeg, longitudeDeg, requestedCell)
				if err != nil {
					return err
				}
				if err := row.SetResult(contains); err != nil {
					return fmt.Errorf("write H3 UDF result: %w", err)
				}
				if ctx != nil {
					if err := ctx.Err(); err != nil {
						return fmt.Errorf("H3 UDF context: %w", err)
					}
				}
			}
			return nil
		},
	}
}

func float64Value(value driver.Value, name string) (float64, error) {
	floatValue, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("H3 UDF %s has type %T, want float64", name, value)
	}
	return floatValue, nil
}

func cellFromUint64(value uint64) (h3.Cell, error) {
	const maxInt64Uint64 = uint64(1<<63 - 1)
	if value > maxInt64Uint64 {
		return 0, fmt.Errorf("requested cell %d cannot be represented by H3", value)
	}
	return h3.Cell(int64(value)), nil
}

func joinPredicates(predicates []string) string {
	if len(predicates) == 1 {
		return predicates[0]
	}
	joined := predicates[0]
	for _, predicate := range predicates[1:] {
		joined += " OR " + predicate
	}
	return joined
}

func normalizeLongitude(longitudeDeg float64) float64 {
	for longitudeDeg < minLongitudeDeg {
		longitudeDeg += 360
	}
	for longitudeDeg > maxLongitudeDeg {
		longitudeDeg -= 360
	}
	return longitudeDeg
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
