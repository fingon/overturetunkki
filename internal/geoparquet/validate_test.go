package geoparquet

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestValidateFileChecksFooterSchemaAndDigest(t *testing.T) {
	cases := []struct {
		name     string
		fileName string
		rows     int64
	}{
		{name: "non-empty", fileName: "places.parquet", rows: 2},
		{name: "empty", fileName: "empty.parquet", rows: 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			validation, err := ValidateFile(filepath.Join(fixtureDirectory(), test.fileName), []string{"id", GeometryColumnName, "names"}, 1024*1024)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, validation.RowCount, test.rows)
			assert.DeepEqual(t, validation.Fields, []string{"id", GeometryColumnName, "names"})
			assert.Equal(t, validation.Metadata.Version, GeoParquetVersion)
			assert.Equal(t, validation.Metadata.PrimaryColumn, GeometryColumnName)
			assert.Assert(t, strings.HasPrefix(validation.Digest, "sha256:"))
			assert.Equal(t, len(validation.Digest), len("sha256:")+64)
		})
	}
}

func TestValidateFileRejectsSizeAndSchemaMismatches(t *testing.T) {
	path := filepath.Join(fixtureDirectory(), "places.parquet")
	_, err := ValidateFile(path, []string{"id", GeometryColumnName, "names"}, 1)
	assert.Assert(t, errors.Is(err, ErrFileTooLarge))
	var tooLarge *FileTooLargeError
	assert.Assert(t, errors.As(err, &tooLarge))
	if tooLarge != nil {
		assert.Assert(t, tooLarge.ActualBytes > tooLarge.LimitBytes)
	}

	_, err = ValidateFile(path, []string{"id", GeometryColumnName}, 1024*1024)
	assert.Assert(t, errors.Is(err, ErrInvalidGeoParquet))
	assert.ErrorContains(t, err, "top-level fields")
}
