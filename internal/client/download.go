package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	DefaultMaxDownloadBytes int64 = 64 << 20
	tileDigestPrefix              = "sha256:"
	parquetMagic                  = "PAR1"
	digestHexLength               = 64
)

type DownloadOptions struct {
	Destination            string
	Force                  bool
	MaxDownloadBytes       int64
	ExpectedCatalogVersion string
	ExpectedProjectionID   string
}

type DownloadResult struct {
	StatusCode      int
	NotModified     bool
	PublishedPath   string
	Release         string
	CatalogVersion  string
	ProjectionID    string
	ETag            string
	DownloadedBytes int64
}

func DownloadTileResponse(ctx context.Context, response *http.Response, options DownloadOptions) (result DownloadResult, err error) {
	if ctx == nil {
		return DownloadResult{}, fmt.Errorf("download tile: context is nil")
	}
	if response == nil || response.Body == nil {
		return DownloadResult{}, fmt.Errorf("download tile: response body is nil")
	}
	result.StatusCode = response.StatusCode
	if response.StatusCode == http.StatusNotModified {
		if closeErr := response.Body.Close(); closeErr != nil {
			return DownloadResult{}, fmt.Errorf("download tile: close 304 response: %w", closeErr)
		}
		result.NotModified = true
		return result, nil
	}
	if response.StatusCode != http.StatusOK {
		return DownloadResult{}, ParseHTTPError(response, DefaultErrorBodyBytes)
	}
	if err := validateDownloadOptions(options); err != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			return DownloadResult{}, fmt.Errorf("%w; close tile response: %v", err, closeErr)
		}
		return DownloadResult{}, err
	}
	metadata, err := validateTileHeaders(response, options)
	if err != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			return DownloadResult{}, fmt.Errorf("%w; close tile response: %v", err, closeErr)
		}
		return DownloadResult{}, err
	}
	if err := rejectExistingDestination(options.Destination, options.Force); err != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			return DownloadResult{}, fmt.Errorf("%w; close tile response: %v", err, closeErr)
		}
		return DownloadResult{}, err
	}
	parent := filepath.Dir(options.Destination)
	temporary, err := os.CreateTemp(parent, "."+filepath.Base(options.Destination)+".partial-*")
	if err != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			return DownloadResult{}, fmt.Errorf("create temporary tile: %w; close tile response: %v", err, closeErr)
		}
		return DownloadResult{}, fmt.Errorf("create temporary tile: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryClosed := false
	published := false
	defer func() {
		if !temporaryClosed {
			closeErr := temporary.Close()
			temporaryClosed = true
			if closeErr != nil {
				if err == nil {
					err = fmt.Errorf("close temporary tile: %w", closeErr)
				} else {
					err = fmt.Errorf("%w; close temporary tile: %v", err, closeErr)
				}
			}
		}
		if published {
			return
		}
		if cleanupErr := os.Remove(temporaryPath); cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) {
			if err == nil {
				err = fmt.Errorf("remove temporary tile: %w", cleanupErr)
				return
			}
			err = fmt.Errorf("%w; remove temporary tile: %v", err, cleanupErr)
		}
	}()

	digest := sha256.New()
	writer := io.MultiWriter(temporary, digest)
	downloaded, copyErr := io.Copy(writer, io.LimitReader(contextReader{ctx: ctx, reader: response.Body}, downloadReadLimit(options.MaxDownloadBytes)))
	closeResponseErr := response.Body.Close()
	if copyErr != nil {
		if closeResponseErr != nil {
			return DownloadResult{}, fmt.Errorf("stream tile response: %w; close tile response: %v", copyErr, closeResponseErr)
		}
		return DownloadResult{}, fmt.Errorf("stream tile response: %w", copyErr)
	}
	if closeResponseErr != nil {
		return DownloadResult{}, fmt.Errorf("close tile response: %w", closeResponseErr)
	}
	if downloaded > options.MaxDownloadBytes {
		return DownloadResult{}, fmt.Errorf("tile response exceeds %d bytes", options.MaxDownloadBytes)
	}
	if downloaded != metadata.contentLength {
		return DownloadResult{}, fmt.Errorf("tile response has %d bytes, Content-Length is %d", downloaded, metadata.contentLength)
	}
	if err := temporary.Sync(); err != nil {
		return DownloadResult{}, fmt.Errorf("sync temporary tile: %w", err)
	}
	if err := temporary.Close(); err != nil {
		temporaryClosed = true
		return DownloadResult{}, fmt.Errorf("close temporary tile: %w", err)
	}
	temporaryClosed = true
	if err := validateParquetMagic(temporaryPath); err != nil {
		return DownloadResult{}, err
	}
	actualETag := `"` + tileDigestPrefix + hex.EncodeToString(digest.Sum(nil)) + `"`
	if actualETag != metadata.etag {
		return DownloadResult{}, fmt.Errorf("tile digest %s does not match ETag %s", actualETag, metadata.etag)
	}
	if err := publishTemporaryTile(temporaryPath, options.Destination, options.Force); err != nil {
		return DownloadResult{}, err
	}
	published = true
	return DownloadResult{
		StatusCode:      response.StatusCode,
		PublishedPath:   options.Destination,
		Release:         metadata.release,
		CatalogVersion:  metadata.catalogVersion,
		ProjectionID:    metadata.projectionID,
		ETag:            metadata.etag,
		DownloadedBytes: downloaded,
	}, nil
}

func downloadReadLimit(maxBytes int64) int64 {
	if maxBytes == 1<<63-1 {
		return maxBytes
	}
	return maxBytes + 1
}

type tileHeaders struct {
	release        string
	catalogVersion string
	projectionID   string
	etag           string
	contentLength  int64
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(value []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(value)
}

func validateDownloadOptions(options DownloadOptions) error {
	if options.Destination == "" {
		return fmt.Errorf("download tile: destination must not be empty")
	}
	if options.MaxDownloadBytes <= 0 {
		return fmt.Errorf("download tile: max download bytes must be positive, got %d", options.MaxDownloadBytes)
	}
	if options.ExpectedCatalogVersion == "" {
		return fmt.Errorf("download tile: expected catalog version must not be empty")
	}
	return nil
}

func validateTileHeaders(response *http.Response, options DownloadOptions) (tileHeaders, error) {
	contentType := response.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != ContentTypeParquet {
		return tileHeaders{}, fmt.Errorf("tile response Content-Type %q is not %q", contentType, ContentTypeParquet)
	}
	contentLength := response.Header.Get("Content-Length")
	if contentLength == "" {
		return tileHeaders{}, fmt.Errorf("tile response is missing Content-Length")
	}
	length, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil || length < 0 {
		return tileHeaders{}, fmt.Errorf("tile response Content-Length %q is invalid", contentLength)
	}
	if length > options.MaxDownloadBytes {
		return tileHeaders{}, fmt.Errorf("tile response Content-Length %d exceeds %d bytes", length, options.MaxDownloadBytes)
	}
	release := response.Header.Get(ReleaseHeader)
	if release == "" {
		return tileHeaders{}, fmt.Errorf("tile response is missing %s", ReleaseHeader)
	}
	catalogVersion := response.Header.Get(CatalogVersionHeader)
	if catalogVersion == "" {
		return tileHeaders{}, fmt.Errorf("tile response is missing %s", CatalogVersionHeader)
	}
	if catalogVersion != options.ExpectedCatalogVersion {
		return tileHeaders{}, fmt.Errorf("tile response catalog version %q does not match requested %q", catalogVersion, options.ExpectedCatalogVersion)
	}
	projectionID := response.Header.Get(ProjectionHeader)
	if projectionID == "" {
		return tileHeaders{}, fmt.Errorf("tile response is missing %s", ProjectionHeader)
	}
	if options.ExpectedProjectionID != "" && projectionID != options.ExpectedProjectionID {
		return tileHeaders{}, fmt.Errorf("tile response projection ID %q does not match requested %q", projectionID, options.ExpectedProjectionID)
	}
	etag := response.Header.Get("ETag")
	if !validDigestETag(etag) {
		return tileHeaders{}, fmt.Errorf("tile response ETag %q is not a quoted sha256 digest", etag)
	}
	return tileHeaders{release: release, catalogVersion: catalogVersion, projectionID: projectionID, etag: etag, contentLength: length}, nil
}

func validDigestETag(value string) bool {
	quotedDigestPrefix := `"` + tileDigestPrefix
	if len(value) != len(quotedDigestPrefix)+digestHexLength+1 || !strings.HasPrefix(value, quotedDigestPrefix) || !strings.HasSuffix(value, `"`) {
		return false
	}
	for _, character := range value[len(quotedDigestPrefix) : len(value)-1] {
		if (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') {
			continue
		}
		return false
	}
	return true
}

func rejectExistingDestination(destination string, force bool) error {
	_, err := os.Stat(destination)
	if err == nil {
		if force {
			return nil
		}
		return fmt.Errorf("destination %q already exists; use force to overwrite", destination)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat destination %q: %w", destination, err)
	}
	return nil
}

func validateParquetMagic(path string) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open temporary Parquet tile: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close temporary Parquet tile: %w", closeErr)
				return
			}
			err = fmt.Errorf("%w; close temporary Parquet tile: %v", err, closeErr)
		}
	}()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil {
		return fmt.Errorf("read temporary Parquet header: %w", err)
	}
	if string(magic) != parquetMagic {
		return fmt.Errorf("temporary tile has invalid Parquet header")
	}
	if _, err := file.Seek(-4, io.SeekEnd); err != nil {
		return fmt.Errorf("seek temporary Parquet footer: %w", err)
	}
	if _, err := io.ReadFull(file, magic); err != nil {
		return fmt.Errorf("read temporary Parquet footer: %w", err)
	}
	if string(magic) != parquetMagic {
		return fmt.Errorf("temporary tile has invalid Parquet footer")
	}
	return nil
}

func publishTemporaryTile(temporaryPath, destination string, force bool) error {
	if force {
		if err := os.Rename(temporaryPath, destination); err != nil {
			return fmt.Errorf("publish tile %q: %w", destination, err)
		}
		return nil
	}
	if err := os.Link(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish tile %q without overwrite: %w", destination, err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove published tile temporary link: %w", err)
	}
	return nil
}
