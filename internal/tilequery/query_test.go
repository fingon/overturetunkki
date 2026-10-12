package tilequery

import (
	"math"
	"testing"

	"gotest.tools/v3/assert"
)

func TestParseSquare(t *testing.T) {
	for _, test := range []struct {
		value string
		valid bool
	}{
		{"0/0/0", true},
		{"xyz/22/4194303/4194303", true},
		{"23/0/0", false},
		{"1/2/0", false},
		{"1/0/2", false},
		{"-1/0/0", false},
		{"01/0/0", false},
		{"+1/0/0", false},
		{"1/0", false},
		{"1/0/0/0", false},
		{"1/0/a", false},
	} {
		t.Run(test.value, func(t *testing.T) {
			square, err := ParseSquare(test.value)
			assert.Equal(t, err == nil, test.valid)
			if test.valid {
				assert.NilError(t, square.Validate())
			}
		})
	}
}

func TestSquareBoundsAndBoundaryOwnership(t *testing.T) {
	world := Square{}
	bounds := world.Bounds()
	assert.Equal(t, bounds.WestDeg, MinLongitudeDeg)
	assert.Equal(t, bounds.EastDeg, MaxLongitudeDeg)
	assert.Equal(t, bounds.NorthDeg, LatitudeLimitDeg)
	assert.Equal(t, bounds.SouthDeg, -LatitudeLimitDeg)
	children := []Square{{Z: 1}, {Z: 1, X: 1}, {Z: 1, Y: 1}, {Z: 1, X: 1, Y: 1}}
	for _, point := range [][2]float64{{0, 0}, {10, 10}, {-10, -10}, {0, 180}, {0, -180}, {LatitudeLimitDeg, 0}, {-LatitudeLimitDeg, 0}} {
		owners := 0
		for _, child := range children {
			assert.Equal(t, child.Parent(), world)
			if child.Contains(point[0], point[1]) {
				owners++
			}
		}
		assert.Equal(t, owners, 1, "point %v", point)
	}
	for _, point := range [][2]float64{{90, 0}, {-90, 0}, {0, math.NaN()}, {math.Inf(1), 0}} {
		assert.Assert(t, !world.Contains(point[0], point[1]))
	}
	assert.Assert(t, (Square{Z: 18, X: 232798, Y: 103246}).Contains(35.6590699, 139.7006793))
}

func TestCoverIsCompleteAndBounded(t *testing.T) {
	for _, test := range []struct {
		name   string
		bounds []Rect
		empty  bool
	}{
		{"ordinary", []Rect{{WestDeg: 24.93, EastDeg: 24.95, SouthDeg: 60.16, NorthDeg: 60.18}}, false},
		{"antimeridian", []Rect{{WestDeg: 179.99, EastDeg: 180, SouthDeg: -0.01, NorthDeg: 0.01}, {WestDeg: -180, EastDeg: -179.99, SouthDeg: -0.01, NorthDeg: 0.01}}, false},
		{"world", []Rect{{WestDeg: -180, EastDeg: 180, SouthDeg: -90, NorthDeg: 90}}, false},
		{"north", []Rect{{WestDeg: 0, EastDeg: 10, SouthDeg: 86, NorthDeg: 90}}, true},
		{"south", []Rect{{WestDeg: 0, EastDeg: 10, SouthDeg: -90, NorthDeg: -86}}, true},
		{"clipped", []Rect{{WestDeg: 0, EastDeg: 10, SouthDeg: 84, NorthDeg: 90}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cover, err := Cover(test.bounds)
			assert.NilError(t, err)
			assert.Assert(t, len(cover) <= MaxSources)
			assert.Equal(t, len(cover) == 0, test.empty)
			for _, rect := range test.bounds {
				for x := range 11 {
					for y := range 11 {
						longitudeDeg := rect.WestDeg + (rect.EastDeg-rect.WestDeg)*float64(x)/10
						latitudeDeg := rect.SouthDeg + (rect.NorthDeg-rect.SouthDeg)*float64(y)/10
						if math.Abs(latitudeDeg) > LatitudeLimitDeg {
							continue
						}
						owners := 0
						for _, square := range cover {
							if square.Contains(latitudeDeg, longitudeDeg) {
								owners++
							}
						}
						assert.Equal(t, owners, 1, "point %v,%v", latitudeDeg, longitudeDeg)
					}
				}
			}
			if len(cover) > 0 && cover[0].Z < MaxZoom {
				_, fits := coverAtZoom(test.bounds, cover[0].Z+1)
				assert.Assert(t, !fits)
			}
		})
	}
}
