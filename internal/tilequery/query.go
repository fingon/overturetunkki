package tilequery

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/uber/h3-go/v4"
)

const (
	MaxZoom          = 22
	MaxSources       = 4
	MinLongitudeDeg  = -180.0
	MaxLongitudeDeg  = 180.0
	LongitudeSpanDeg = 360.0
	DegreesPerRadian = 180 / math.Pi
	XYZPrefix        = "xyz/"
)

var LatitudeLimitDeg = math.Atan(math.Sinh(math.Pi)) * DegreesPerRadian

type Query struct {
	Cell   string
	Square *Square
}

func (query Query) String() string {
	if query.Square != nil {
		return query.Square.String()
	}
	return query.Cell
}

func (query Query) Validate() error {
	if query.Square != nil {
		if query.Cell != "" {
			return errors.New("query must select exactly one of H3 and XYZ")
		}
		return query.Square.Validate()
	}
	if query.Cell == "" {
		return errors.New("query must select H3 or XYZ")
	}
	return nil
}

type Square struct {
	Z int `json:"z"`
	X int `json:"x"`
	Y int `json:"y"`
}

type Rect struct {
	WestDeg  float64
	SouthDeg float64
	EastDeg  float64
	NorthDeg float64
}

func (square Square) Validate() error {
	if square.Z < 0 || square.Z > MaxZoom {
		return fmt.Errorf("XYZ zoom must be between 0 and %d", MaxZoom)
	}
	count := 1 << square.Z
	if square.X < 0 || square.X >= count || square.Y < 0 || square.Y >= count {
		return fmt.Errorf("XYZ coordinates must be between 0 and %d at zoom %d", count-1, square.Z)
	}
	return nil
}

func (square Square) String() string {
	return fmt.Sprintf("%s%d/%d/%d", XYZPrefix, square.Z, square.X, square.Y)
}

func ParseSquare(value string) (Square, error) {
	parts := strings.Split(strings.TrimPrefix(value, XYZPrefix), "/")
	if len(parts) != 3 {
		return Square{}, errors.New("XYZ query must contain zoom/x/y")
	}
	values := [3]int{}
	for index, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 || strconv.Itoa(value) != part {
			return Square{}, fmt.Errorf("XYZ coordinate %q is not a canonical unsigned integer", part)
		}
		values[index] = value
	}
	square := Square{Z: values[0], X: values[1], Y: values[2]}
	return square, square.Validate()
}

func (square Square) Parent() Square {
	return Square{Z: square.Z - 1, X: square.X / 2, Y: square.Y / 2}
}

func (square Square) Bounds() Rect {
	count := float64(int64(1) << square.Z)
	latitude := func(y int) float64 {
		return math.Atan(math.Sinh(math.Pi*(1-2*float64(y)/count))) * DegreesPerRadian
	}
	return Rect{
		WestDeg:  float64(square.X)/count*LongitudeSpanDeg + MinLongitudeDeg,
		EastDeg:  float64(square.X+1)/count*LongitudeSpanDeg + MinLongitudeDeg,
		NorthDeg: latitude(square.Y), SouthDeg: latitude(square.Y + 1),
	}
}

func (square Square) Contains(latitudeDeg, longitudeDeg float64) bool {
	if math.IsNaN(latitudeDeg) || math.IsNaN(longitudeDeg) || math.IsInf(latitudeDeg, 0) || math.IsInf(longitudeDeg, 0) ||
		latitudeDeg < -LatitudeLimitDeg || latitudeDeg > LatitudeLimitDeg || longitudeDeg < MinLongitudeDeg || longitudeDeg > MaxLongitudeDeg {
		return false
	}
	if longitudeDeg == MaxLongitudeDeg {
		longitudeDeg = MinLongitudeDeg
	}
	bounds := square.Bounds()
	return longitudeDeg >= bounds.WestDeg && longitudeDeg < bounds.EastDeg &&
		latitudeDeg <= bounds.NorthDeg && (latitudeDeg > bounds.SouthDeg || (square.Y == (1<<square.Z)-1 && latitudeDeg == bounds.SouthDeg))
}

// Cover chooses the finest conservative cover containing at most four squares.
// Rectangles must split antimeridian crossings into ordinary longitude ranges.
func Cover(rectangles []Rect) ([]Square, error) {
	for _, rect := range rectangles {
		if !finiteRect(rect) || rect.WestDeg < MinLongitudeDeg || rect.EastDeg > MaxLongitudeDeg || rect.WestDeg > rect.EastDeg || rect.SouthDeg > rect.NorthDeg {
			return nil, errors.New("invalid square coverage bounds")
		}
	}
	for zoom := MaxZoom; zoom >= 0; zoom-- {
		if cover, fits := coverAtZoom(rectangles, zoom); fits {
			return cover, nil
		}
	}
	return nil, errors.New("square coverage exceeds source limit")
}

func finiteRect(rect Rect) bool {
	for _, value := range []float64{rect.WestDeg, rect.EastDeg, rect.SouthDeg, rect.NorthDeg} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

func coverAtZoom(rectangles []Rect, zoom int) ([]Square, bool) {
	count := 1 << zoom
	cover := make([]Square, 0, MaxSources)
	seen := make(map[Square]bool)
	for _, rect := range rectangles {
		if rect.SouthDeg > LatitudeLimitDeg || rect.NorthDeg < -LatitudeLimitDeg {
			continue
		}
		minX := max(0, min(count-1, int(math.Floor((rect.WestDeg-MinLongitudeDeg)/LongitudeSpanDeg*float64(count)))))
		maxX := max(0, min(count-1, int(math.Floor((rect.EastDeg-MinLongitudeDeg)/LongitudeSpanDeg*float64(count)))))
		minY := latitudeRow(min(rect.NorthDeg, LatitudeLimitDeg), count)
		maxY := latitudeRow(max(rect.SouthDeg, -LatitudeLimitDeg), count)
		if (maxX-minX+1)*(maxY-minY+1) > MaxSources {
			return nil, false
		}
		for x := minX; x <= maxX; x++ {
			for y := minY; y <= maxY; y++ {
				square := Square{Z: zoom, X: x, Y: y}
				if !seen[square] {
					seen[square] = true
					cover = append(cover, square)
				}
				if len(cover) > MaxSources {
					return nil, false
				}
			}
		}
		// Longitude +180 is assigned to the first column.
		if rect.EastDeg == MaxLongitudeDeg && minX > 0 {
			for y := minY; y <= maxY; y++ {
				square := Square{Z: zoom, X: 0, Y: y}
				if !seen[square] {
					seen[square] = true
					cover = append(cover, square)
				}
				if len(cover) > MaxSources {
					return nil, false
				}
			}
		}
	}
	return cover, true
}

func latitudeRow(latitudeDeg float64, count int) int {
	latitudeRad := latitudeDeg / DegreesPerRadian
	row := int(math.Floor((1 - math.Asinh(math.Tan(latitudeRad))/math.Pi) / 2 * float64(count)))
	return max(0, min(count-1, row))
}

func MinimumXYZZoom(minResolution int) (int, error) {
	areaM2, err := h3.HexagonAreaAvgM2(minResolution)
	if err != nil {
		return 0, fmt.Errorf("derive minimum XYZ zoom: %w", err)
	}
	if minResolution == 0 {
		return 0, nil
	}
	const equatorialRadiusM = 6378137.0
	circumferenceM := 2 * math.Pi * equatorialRadiusM
	zoom := int(math.Round(math.Log2(circumferenceM*circumferenceM/areaM2) / 2))
	return min(MaxZoom, max(0, zoom)), nil
}
