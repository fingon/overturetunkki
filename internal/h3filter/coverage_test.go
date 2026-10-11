package h3filter

import (
	"errors"
	"slices"
	"testing"

	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

const (
	testCoverageOrdinary     = "ordinary"
	testCoverageAntimeridian = "antimeridian"
	testCoverageFaceCrossing = "face crossing"
	testCoveragePentagon     = "pentagon"
)

func TestCoveringCellsIncludesDirectMembership(t *testing.T) {
	pentagons, err := h3.Pentagons(9)
	assert.NilError(t, err)
	for _, test := range []struct {
		name string
		cell h3.Cell
	}{
		{name: testCoverageOrdinary, cell: mustCell(t, 37.775938728915946, -122.41795063018799, 9)},
		{name: testCoverageAntimeridian, cell: mustCell(t, 0, 179.9999, 9)},
		{name: testCoverageFaceCrossing, cell: h3.CellFromString(reportedFaceCrossingCell)},
		{name: testCoveragePentagon, cell: pentagons[0]},
	} {
		t.Run(test.name, func(t *testing.T) {
			cell := test.cell
			bounds, err := CellBounds(cell)
			assert.NilError(t, err)
			assert.Assert(t, !bounds.IsGlobal())
			for resolution := cell.Resolution() - 1; resolution >= 0; resolution-- {
				cover, coverErr := CoveringCells(cell, resolution)
				assert.NilError(t, coverErr)
				assert.Assert(t, len(cover) > 0 && len(cover) <= MaxCoveringCells)
				assert.Assert(t, slices.IsSorted(cover))
				matched := 0
				const samples = 40
				for _, interval := range bounds.LongitudeIntervals {
					for latitudeIndex := range samples + 1 {
						latitudeDeg := bounds.LatitudeMinDeg + (bounds.LatitudeMaxDeg-bounds.LatitudeMinDeg)*float64(latitudeIndex)/samples
						for longitudeIndex := range samples + 1 {
							longitudeDeg := interval.MinDeg + (interval.MaxDeg-interval.MinDeg)*float64(longitudeIndex)/samples
							point := h3.LatLng{Lat: latitudeDeg, Lng: longitudeDeg}
							fineCell, pointErr := h3.LatLngToCell(point, cell.Resolution())
							assert.NilError(t, pointErr)
							if fineCell != cell {
								continue
							}
							coarseCell, pointErr := h3.LatLngToCell(point, resolution)
							assert.NilError(t, pointErr)
							assert.Assert(t, slices.Contains(cover, coarseCell), "missing direct membership at %v", point)
							matched++
						}
					}
				}
				assert.Assert(t, matched > 0)
			}
		})
	}
}

func TestCoveringCellsUnsafeBoundsAndInvalidInput(t *testing.T) {
	cover, err := CoveringCells(mustCell(t, 90, 0, 9), 8)
	assert.NilError(t, err)
	assert.Equal(t, len(cover), 0)
	cell := mustCell(t, 60.17, 24.94, 9)
	for _, resolution := range []int{-1, cell.Resolution(), h3.MaxResolution + 1} {
		_, coverErr := CoveringCells(cell, resolution)
		assert.ErrorContains(t, coverErr, "must be coarser")
	}
	_, err = CoveringCells(0, 0)
	assert.Assert(t, errors.Is(err, h3.ErrCellInvalid))
}
