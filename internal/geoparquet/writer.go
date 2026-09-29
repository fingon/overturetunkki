package geoparquet

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

const (
	GeoMetadataKey     = "geo"
	GeoParquetVersion  = "1.1.0"
	GeometryColumnName = "geometry"
	WKBEncoding        = "WKB"
)

type GeoMetadata struct {
	Version       string               `json:"version"`
	PrimaryColumn string               `json:"primary_column"`
	Columns       map[string]GeoColumn `json:"columns"`
}

type GeoColumn struct {
	BBox          []float64 `json:"bbox"`
	Encoding      string    `json:"encoding"`
	GeometryTypes []string  `json:"geometry_types"`
}

func PointMetadata(minLongitudeDeg, minLatitudeDeg, maxLongitudeDeg, maxLatitudeDeg float64) ([]byte, error) {
	values := []float64{minLongitudeDeg, minLatitudeDeg, maxLongitudeDeg, maxLatitudeDeg}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("GeoParquet bbox contains non-finite value %v", value)
		}
	}
	if minLongitudeDeg > maxLongitudeDeg || minLatitudeDeg > maxLatitudeDeg {
		return nil, fmt.Errorf("GeoParquet bbox has inverted bounds: [%v %v %v %v]", values[0], values[1], values[2], values[3])
	}
	metadata := GeoMetadata{
		Version:       GeoParquetVersion,
		PrimaryColumn: GeometryColumnName,
		Columns: map[string]GeoColumn{
			GeometryColumnName: {
				BBox:          values,
				Encoding:      WKBEncoding,
				GeometryTypes: []string{"Point"},
			},
		},
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode GeoParquet metadata: %w", err)
	}
	return encoded, nil
}

func CopySQL(selectSQL, outputPath string, metadata []byte) (string, error) {
	if strings.TrimSpace(selectSQL) == "" {
		return "", fmt.Errorf("GeoParquet COPY query is empty")
	}
	if outputPath == "" {
		return "", fmt.Errorf("GeoParquet COPY output path is empty")
	}
	if !json.Valid(metadata) {
		return "", fmt.Errorf("GeoParquet metadata is not valid JSON")
	}
	quotedPath := strings.ReplaceAll(outputPath, "'", "''")
	quotedMetadata := strings.ReplaceAll(string(metadata), "'", "''")
	return fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA {'%s': '%s'})",
		selectSQL,
		quotedPath,
		GeoMetadataKey,
		quotedMetadata,
	), nil
}
