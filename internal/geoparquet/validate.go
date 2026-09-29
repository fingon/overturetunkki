package geoparquet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

var (
	ErrInvalidGeoParquet = errors.New("invalid GeoParquet file")
	ErrFileTooLarge      = errors.New("GeoParquet file exceeds byte limit")
)

type FileValidation struct {
	SizeBytes int64
	RowCount  int64
	Fields    []string
	Metadata  GeoMetadata
	Digest    string
}

type FileTooLargeError struct {
	ActualBytes int64
	LimitBytes  int64
}

func (err *FileTooLargeError) Error() string {
	return fmt.Sprintf("GeoParquet file is %d bytes, limit is %d bytes", err.ActualBytes, err.LimitBytes)
}

func (*FileTooLargeError) Unwrap() error {
	return ErrFileTooLarge
}

func ValidateFile(path string, expectedFields []string, maxBytes int64) (validation FileValidation, err error) {
	if path == "" {
		return FileValidation{}, fmt.Errorf("validate GeoParquet: path is empty")
	}
	if len(expectedFields) == 0 {
		return FileValidation{}, fmt.Errorf("validate GeoParquet: expected fields are empty")
	}
	if maxBytes <= 0 {
		return FileValidation{}, fmt.Errorf("validate GeoParquet: byte limit must be positive, got %d", maxBytes)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return FileValidation{}, fmt.Errorf("stat GeoParquet %q: %w", path, err)
	}
	if !fileInfo.Mode().IsRegular() {
		return FileValidation{}, fmt.Errorf("validate GeoParquet %q: not a regular file", path)
	}
	if fileInfo.Size() > maxBytes {
		return FileValidation{}, &FileTooLargeError{ActualBytes: fileInfo.Size(), LimitBytes: maxBytes}
	}

	reader, err := file.OpenParquetFile(path, false)
	if err != nil {
		return FileValidation{}, fmt.Errorf("%w: open footer %q: %v", ErrInvalidGeoParquet, path, err)
	}
	defer func() {
		closeErr := reader.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("close GeoParquet %q: %w", path, closeErr)
		}
	}()

	fileMetadata := reader.MetaData()
	root := fileMetadata.Schema.Root()
	if root.NumFields() != len(expectedFields) {
		return FileValidation{}, invalidFile(path, "footer has %d top-level fields, want %d", root.NumFields(), len(expectedFields))
	}
	fields := make([]string, root.NumFields())
	for index := 0; index < root.NumFields(); index++ {
		fields[index] = root.Field(index).Name()
		if fields[index] != expectedFields[index] {
			return FileValidation{}, invalidFile(path, "footer field %d is %q, want %q", index, fields[index], expectedFields[index])
		}
	}

	geoMetadata, err := readGeoMetadata(path, fileMetadata.KeyValueMetadata())
	if err != nil {
		return FileValidation{}, err
	}
	if geoMetadata.Version != GeoParquetVersion || geoMetadata.PrimaryColumn != GeometryColumnName {
		return FileValidation{}, invalidFile(path, "GeoParquet metadata has version %q and primary column %q", geoMetadata.Version, geoMetadata.PrimaryColumn)
	}
	geometryMetadata, ok := geoMetadata.Columns[GeometryColumnName]
	if !ok || len(geoMetadata.Columns) != 1 || geometryMetadata.Encoding != WKBEncoding || len(geometryMetadata.GeometryTypes) != 1 || geometryMetadata.GeometryTypes[0] != "Point" {
		return FileValidation{}, invalidFile(path, "GeoParquet geometry metadata is not a single WKB Point column")
	}
	geometryColumnIndex := fileMetadata.Schema.ColumnIndexByName(GeometryColumnName)
	if geometryColumnIndex < 0 {
		return FileValidation{}, invalidFile(path, "footer has no %q column", GeometryColumnName)
	}
	geometryColumn := fileMetadata.Schema.Column(geometryColumnIndex)
	if geometryColumn.PhysicalType() != parquet.Types.ByteArray || geometryColumn.ConvertedType() != schema.ConvertedTypes.None {
		return FileValidation{}, invalidFile(path, "geometry column is not an unannotated BYTE_ARRAY")
	}

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return FileValidation{}, fmt.Errorf("%w: create independent reader for %q: %v", ErrInvalidGeoParquet, path, err)
	}
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return FileValidation{}, fmt.Errorf("%w: read %q: %v", ErrInvalidGeoParquet, path, err)
	}
	defer table.Release()
	if table.NumRows() != reader.NumRows() {
		return FileValidation{}, invalidFile(path, "Arrow reader has %d rows, footer has %d", table.NumRows(), reader.NumRows())
	}
	geometryIndices := table.Schema().FieldIndices(GeometryColumnName)
	if len(geometryIndices) != 1 {
		return FileValidation{}, invalidFile(path, "Arrow schema has %d geometry columns", len(geometryIndices))
	}
	for _, chunk := range table.Column(geometryIndices[0]).Data().Chunks() {
		if _, ok := chunk.(*array.Binary); !ok {
			return FileValidation{}, invalidFile(path, "Arrow geometry column has type %T, want binary", chunk)
		}
	}

	digest, err := digestFile(path, fileInfo.Size())
	if err != nil {
		return FileValidation{}, err
	}
	return FileValidation{
		SizeBytes: fileInfo.Size(),
		RowCount:  reader.NumRows(),
		Fields:    fields,
		Metadata:  geoMetadata,
		Digest:    digest,
	}, nil
}

func readGeoMetadata(path string, keyValueMetadata metadata.KeyValueMetadata) (GeoMetadata, error) {
	geoMetadataCount := 0
	for _, key := range keyValueMetadata.Keys() {
		if key == GeoMetadataKey {
			geoMetadataCount++
		}
	}
	if geoMetadataCount != 1 {
		return GeoMetadata{}, invalidFile(path, "has %d %q metadata entries, want one", geoMetadataCount, GeoMetadataKey)
	}
	geoValue := keyValueMetadata.FindValue(GeoMetadataKey)
	if geoValue == nil {
		return GeoMetadata{}, invalidFile(path, "has no %q metadata value", GeoMetadataKey)
	}
	var decoded GeoMetadata
	if err := json.Unmarshal([]byte(*geoValue), &decoded); err != nil {
		return GeoMetadata{}, fmt.Errorf("%w: decode %q metadata: %v", ErrInvalidGeoParquet, path, err)
	}
	return decoded, nil
}

func digestFile(path string, expectedSize int64) (string, error) {
	input, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open GeoParquet %q for digest: %w", path, err)
	}
	hasher := sha256.New()
	bytesRead, copyErr := io.Copy(hasher, input)
	closeErr := input.Close()
	if copyErr != nil {
		return "", fmt.Errorf("hash GeoParquet %q: %w", path, copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close GeoParquet %q after digest: %w", path, closeErr)
	}
	if bytesRead != expectedSize {
		return "", fmt.Errorf("%w: GeoParquet %q changed during validation", ErrInvalidGeoParquet, path)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat GeoParquet %q after digest: %w", path, err)
	}
	if fileInfo.Size() != expectedSize {
		return "", fmt.Errorf("%w: GeoParquet %q changed during validation", ErrInvalidGeoParquet, path)
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func invalidFile(path, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidGeoParquet, path, fmt.Sprintf(format, args...))
}
