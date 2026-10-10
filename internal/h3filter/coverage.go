package h3filter

import (
	"errors"
	"fmt"
	"slices"

	"github.com/uber/h3-go/v4"
)

const MaxCoveringCells = 64

// CoveringCells returns a conservative coarser cover, or no cover when reuse
// would require excessive sources or global/polar bounds.
func CoveringCells(cell h3.Cell, resolution int) ([]h3.Cell, error) {
	if !cell.IsValid() {
		return nil, fmt.Errorf("cover tile: %w", h3.ErrCellInvalid)
	}
	if resolution < 0 || resolution >= cell.Resolution() {
		return nil, fmt.Errorf("cover tile: resolution %d must be coarser than %d", resolution, cell.Resolution())
	}
	bounds, err := CellBounds(cell)
	if err != nil {
		return nil, fmt.Errorf("cover tile bounds: %w", err)
	}
	if bounds.LatitudeMinDeg <= minLatitudeDeg || bounds.LatitudeMaxDeg >= maxLatitudeDeg {
		return nil, nil
	}
	cells := make(map[h3.Cell]struct{})
	for _, interval := range bounds.LongitudeIntervals {
		if interval.MaxDeg-interval.MinDeg >= 180 {
			return nil, nil
		}
		polygon := h3.GeoPolygon{GeoLoop: h3.GeoLoop{
			{Lat: bounds.LatitudeMinDeg, Lng: interval.MinDeg},
			{Lat: bounds.LatitudeMinDeg, Lng: interval.MaxDeg},
			{Lat: bounds.LatitudeMaxDeg, Lng: interval.MaxDeg},
			{Lat: bounds.LatitudeMaxDeg, Lng: interval.MinDeg},
		}}
		covering, coverErr := h3.PolygonToCellsExperimental(polygon, resolution, h3.ContainmentOverlappingBbox, MaxCoveringCells)
		if errors.Is(coverErr, h3.ErrMemoryBounds) {
			return nil, nil
		}
		if coverErr != nil {
			return nil, fmt.Errorf("cover tile at resolution %d: %w", resolution, coverErr)
		}
		if len(covering) == 0 {
			return nil, errors.New("cover tile: overlap enumeration returned no cells")
		}
		for _, coveringCell := range covering {
			cells[coveringCell] = struct{}{}
		}
		if len(cells) > MaxCoveringCells {
			return nil, nil
		}
	}
	result := make([]h3.Cell, 0, len(cells))
	for coveringCell := range cells {
		result = append(result, coveringCell)
	}
	slices.Sort(result)
	return result, nil
}
