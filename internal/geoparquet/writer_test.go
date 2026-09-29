//nolint:goconst // Repeated literals keep independent test cases readable.
package geoparquet

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	_ "github.com/duckdb/duckdb-go/v2"
	"gotest.tools/v3/assert"
)

const (
	fixtureUpdateEnv         = "OVERTURE_UPDATE_GEOPARQUET_FIXTURES"
	sampleMinLongitudeDeg    = -122.4194
	sampleMinLatitudeDeg     = 37.7749
	sampleMaxLongitudeDeg    = 24.941
	sampleMaxLatitudeDeg     = 60.171
	sampleFirstLongitudeDeg  = 24.941
	sampleFirstLatitudeDeg   = 60.171
	sampleSecondLongitudeDeg = -122.4194
	sampleSecondLatitudeDeg  = 37.7749
	sampleNonEmptyRowCount   = 2
	sampleNestedFieldCount   = 2
)

type parquetInspection struct {
	Rows             int64
	Fields           []string
	Metadata         GeoMetadata
	GeometryWKB      [][]byte
	NestedField      bool
	NestedFieldCount int
	Zstd             bool
}

func TestDuckDBWritesGeoParquet(t *testing.T) {
	connection := openDuckDBConnection(t)
	metadata, err := PointMetadata(sampleMinLongitudeDeg, sampleMinLatitudeDeg, sampleMaxLongitudeDeg, sampleMaxLatitudeDeg)
	assert.NilError(t, err)
	if err != nil {
		return
	}

	cases := []struct {
		name       string
		selectSQL  string
		wantRows   int64
		wantPoints [][2]float64
	}{
		{
			name:      "nested non-empty zstd",
			selectSQL: sampleSelectSQL(),
			wantRows:  sampleNonEmptyRowCount,
			wantPoints: [][2]float64{
				{sampleFirstLongitudeDeg, sampleFirstLatitudeDeg},
				{sampleSecondLongitudeDeg, sampleSecondLatitudeDeg},
			},
		},
		{
			name:      "empty result",
			selectSQL: fmt.Sprintf("SELECT * FROM (%s) AS selected WHERE false", sampleSelectSQL()),
			wantRows:  0,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "tile.parquet")
			copySQL, err := CopySQL(test.selectSQL, outputPath, metadata)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			_, err = connection.ExecContext(context.Background(), copySQL)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			inspection, err := inspectParquet(outputPath)
			assert.NilError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, inspection.Rows, test.wantRows)
			assertGeoParquetSchema(t, inspection)
			if test.wantRows > 0 {
				assert.Assert(t, inspection.Zstd)
			}
			assert.DeepEqual(t, inspection.GeometryWKB, expectedWKB(test.wantPoints))
		})
	}
}

func TestCommittedGeoParquetFixtures(t *testing.T) {
	cases := []struct {
		name       string
		fileName   string
		wantRows   int64
		wantPoints [][2]float64
	}{
		{
			name:     "places",
			fileName: "places.parquet",
			wantRows: sampleNonEmptyRowCount,
			wantPoints: [][2]float64{
				{sampleFirstLongitudeDeg, sampleFirstLatitudeDeg},
				{sampleSecondLongitudeDeg, sampleSecondLatitudeDeg},
			},
		},
		{name: "empty", fileName: "empty.parquet", wantRows: 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			inspection, err := inspectParquet(filepath.Join(fixtureDirectory(), test.fileName))
			assert.NilError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, inspection.Rows, test.wantRows)
			assertGeoParquetSchema(t, inspection)
			if test.wantRows > 0 {
				assert.Assert(t, inspection.Zstd)
			}
			assert.DeepEqual(t, inspection.GeometryWKB, expectedWKB(test.wantPoints))
		})
	}
}

func TestGenerateGeoParquetFixtures(t *testing.T) {
	if os.Getenv(fixtureUpdateEnv) != "1" {
		t.Skip("fixture generation is opt-in")
	}

	if err := os.MkdirAll(fixtureDirectory(), 0o755); err != nil {
		t.Fatal(err)
	}
	connection := openDuckDBConnection(t)
	metadata, err := PointMetadata(sampleMinLongitudeDeg, sampleMinLatitudeDeg, sampleMaxLongitudeDeg, sampleMaxLatitudeDeg)
	assert.NilError(t, err)
	if err != nil {
		return
	}
	fixtures := []struct {
		fileName  string
		selectSQL string
	}{
		{fileName: "places.parquet", selectSQL: sampleSelectSQL()},
		{fileName: "empty.parquet", selectSQL: fmt.Sprintf("SELECT * FROM (%s) AS selected WHERE false", sampleSelectSQL())},
	}
	for _, fixture := range fixtures {
		outputPath := filepath.Join(fixtureDirectory(), fixture.fileName)
		copySQL, err := CopySQL(fixture.selectSQL, outputPath, metadata)
		assert.NilError(t, err)
		if err != nil {
			return
		}
		if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		_, err = connection.ExecContext(context.Background(), copySQL)
		assert.NilError(t, err)
		if err != nil {
			return
		}
	}
}

func inspectParquet(path string) (inspection parquetInspection, err error) {
	reader, err := file.OpenParquetFile(path, false)
	if err != nil {
		return parquetInspection{}, fmt.Errorf("open Parquet fixture %q: %w", path, err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close Parquet fixture %q: %w", path, closeErr)
		}
	}()

	fileMetadata := reader.MetaData()
	keyValueMetadata := fileMetadata.KeyValueMetadata()
	geoMetadataCount := 0
	for _, key := range keyValueMetadata.Keys() {
		if key == GeoMetadataKey {
			geoMetadataCount++
		}
	}
	if geoMetadataCount != 1 {
		return parquetInspection{}, fmt.Errorf("Parquet fixture %q has %d %q metadata entries, want one", path, geoMetadataCount, GeoMetadataKey)
	}
	geoValue := keyValueMetadata.FindValue(GeoMetadataKey)
	if geoValue == nil {
		return parquetInspection{}, fmt.Errorf("Parquet fixture %q has no %q metadata", path, GeoMetadataKey)
	}
	if err := json.Unmarshal([]byte(*geoValue), &inspection.Metadata); err != nil {
		return parquetInspection{}, fmt.Errorf("decode GeoParquet metadata from %q: %w", path, err)
	}
	inspection.Rows = reader.NumRows()
	root := fileMetadata.Schema.Root()
	fieldCount := root.NumFields()
	for index := range fieldCount {
		inspection.Fields = append(inspection.Fields, root.Field(index).Name())
	}
	namesIndex := fileMetadata.Schema.Root().FieldIndexByName("names")
	if namesIndex >= 0 {
		namesField := fileMetadata.Schema.Root().Field(namesIndex)
		inspection.NestedField = namesField.Type() == schema.Group
		if namesGroup, ok := namesField.(*schema.GroupNode); ok {
			inspection.NestedFieldCount = namesGroup.NumFields()
		}
	}
	geometryColumnIndex := fileMetadata.Schema.ColumnIndexByName(GeometryColumnName)
	if geometryColumnIndex < 0 {
		return parquetInspection{}, fmt.Errorf("Parquet fixture %q has no %q column", path, GeometryColumnName)
	}
	geometryColumn := fileMetadata.Schema.Column(geometryColumnIndex)
	if geometryColumn.PhysicalType() != parquet.Types.ByteArray {
		return parquetInspection{}, fmt.Errorf("Parquet geometry column has physical type %s, want BYTE_ARRAY", geometryColumn.PhysicalType())
	}
	if geometryColumn.ConvertedType() != schema.ConvertedTypes.None {
		return parquetInspection{}, fmt.Errorf("Parquet geometry column has logical type %s, want none", geometryColumn.LogicalType())
	}
	if reader.NumRowGroups() > 0 {
		rowGroup := fileMetadata.RowGroup(0)
		inspection.Zstd = true
		columnCount := rowGroup.NumColumns()
		for columnIndex := range columnCount {
			column, columnErr := rowGroup.ColumnChunk(columnIndex)
			if columnErr != nil {
				return parquetInspection{}, fmt.Errorf("read Parquet column metadata: %w", columnErr)
			}
			if column.Compression() != compress.Codecs.Zstd {
				inspection.Zstd = false
			}
		}
	}

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return parquetInspection{}, fmt.Errorf("create independent Arrow reader: %w", err)
	}
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return parquetInspection{}, fmt.Errorf("read Parquet fixture with independent Arrow reader: %w", err)
	}
	defer table.Release()
	if table.NumRows() != inspection.Rows {
		return parquetInspection{}, fmt.Errorf("Arrow row count %d differs from footer row count %d", table.NumRows(), inspection.Rows)
	}
	geometryIndex := table.Schema().FieldIndices(GeometryColumnName)
	if len(geometryIndex) != 1 {
		return parquetInspection{}, fmt.Errorf("Arrow schema has %d geometry columns", len(geometryIndex))
	}
	inspection.GeometryWKB = make([][]byte, 0, table.NumRows())
	if table.NumRows() > 0 {
		for _, chunk := range table.Column(geometryIndex[0]).Data().Chunks() {
			binaryArray, ok := chunk.(*array.Binary)
			if !ok {
				return parquetInspection{}, fmt.Errorf("Arrow geometry column has type %T, want binary", chunk)
			}
			rowCount := binaryArray.Len()
			for rowIndex := range rowCount {
				inspection.GeometryWKB = append(inspection.GeometryWKB, append([]byte(nil), binaryArray.Value(rowIndex)...))
			}
		}
	}
	return inspection, nil
}

func sampleSelectSQL() string {
	firstPoint := hex.EncodeToString(pointWKB(sampleFirstLongitudeDeg, sampleFirstLatitudeDeg))
	secondPoint := hex.EncodeToString(pointWKB(sampleSecondLongitudeDeg, sampleSecondLatitudeDeg))
	return fmt.Sprintf(`
		SELECT *
		FROM (VALUES
			('poi-1', unhex('%s'), struct_pack(primary_name := 'Cafe', aliases := ['Coffee'])),
			('poi-2', unhex('%s'), struct_pack(primary_name := 'Park', aliases := ['Garden']))
		) AS source(id, geometry, names)`, firstPoint, secondPoint)
}

func pointWKB(longitudeDeg, latitudeDeg float64) []byte {
	wkb := make([]byte, 21)
	wkb[0] = 1
	binary.LittleEndian.PutUint32(wkb[1:5], 1)
	binary.LittleEndian.PutUint64(wkb[5:13], math.Float64bits(longitudeDeg))
	binary.LittleEndian.PutUint64(wkb[13:21], math.Float64bits(latitudeDeg))
	return wkb
}

func expectedWKB(points [][2]float64) [][]byte {
	result := make([][]byte, 0, len(points))
	for _, point := range points {
		result = append(result, pointWKB(point[0], point[1]))
	}
	return result
}

func assertGeoMetadata(t *testing.T, inspection parquetInspection) {
	t.Helper()
	assert.Equal(t, inspection.Metadata.Version, GeoParquetVersion)
	assert.Equal(t, inspection.Metadata.PrimaryColumn, GeometryColumnName)
	geometryMetadata, ok := inspection.Metadata.Columns[GeometryColumnName]
	assert.Assert(t, ok)
	if !ok {
		return
	}
	assert.DeepEqual(t, geometryMetadata.BBox, []float64{
		sampleMinLongitudeDeg,
		sampleMinLatitudeDeg,
		sampleMaxLongitudeDeg,
		sampleMaxLatitudeDeg,
	})
	assert.Equal(t, geometryMetadata.Encoding, WKBEncoding)
	assert.DeepEqual(t, geometryMetadata.GeometryTypes, []string{"Point"})
	assert.Assert(t, inspection.NestedField)
	assert.Equal(t, inspection.NestedFieldCount, sampleNestedFieldCount)
}

func assertGeoParquetSchema(t *testing.T, inspection parquetInspection) {
	t.Helper()
	assert.DeepEqual(t, inspection.Fields, []string{"id", GeometryColumnName, "names"})
	assertGeoMetadata(t, inspection)
}

func fixtureDirectory() string {
	return filepath.Join(testRepoRoot(), "testdata", "geoparquet")
}

func testRepoRoot() string {
	_, sourcePath, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourcePath), "..", ".."))
}

func openDuckDBConnection(tb testing.TB) *sql.Conn {
	tb.Helper()
	db, err := sql.Open("duckdb", "")
	assert.NilError(tb, err)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	tb.Cleanup(func() { assert.NilError(tb, db.Close()) })
	connection, err := db.Conn(context.Background())
	assert.NilError(tb, err)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { assert.NilError(tb, connection.Close()) })
	return connection
}
