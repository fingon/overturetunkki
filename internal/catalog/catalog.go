//nolint:tagliatelle // External STAC and API schemas define these JSON names.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultCatalogURL                    = "https://stac.overturemaps.org/catalog.json"
	DefaultCatalogHost                   = "stac.overturemaps.org"
	DefaultAssetHost                     = "overturemaps-us-west-2.s3.us-west-2.amazonaws.com"
	DefaultAssetProvider                 = "aws"
	DefaultStorageRegion                 = "us-west-2"
	DefaultAssetType                     = "application/vnd.apache.parquet"
	DefaultCollectionID                  = "place"
	DefaultMaxResponseBytes        int64 = 16 * 1024 * 1024
	DefaultProjectionH3Semantics         = "latlng-to-cell-v1"
	DefaultProjectionBBoxSemantics       = "valid-bbox-only-v1"
	DefaultProjectionWriter              = "geoparquet-1.1-wkb-zstd-v1"

	stacCatalogType       = "Catalog"
	stacCollectionType    = "Collection"
	stacFeatureType       = "Feature"
	stacChildRel          = "child"
	stacPlacesID          = "places"
	stacPrimaryGeometry   = "geometry"
	stacDataRole          = "data"
	stacPlacesTheme       = stacPlacesID
	stacPlaceType         = DefaultCollectionID
	stacPartitionGlobTail = "*.parquet"
	stacAssetPathSuffix   = ".zstd.parquet"
)

type Options struct {
	CatalogURL       string
	CatalogHost      string
	AssetHost        string
	AssetProvider    string
	StorageRegion    string
	AssetType        string
	CollectionID     string
	Fields           []string
	Timeout          time.Duration
	MaxResponseBytes int64
	Client           *http.Client
}

type Manager struct {
	options     Options
	rootURL     *url.URL
	client      *http.Client
	refreshGate chan struct{}
	responsesMu sync.Mutex
	responses   map[string]cachedResponse
}

type cachedResponse struct {
	body         []byte
	etag         string
	lastModified string
}

type Snapshot struct {
	Release        string
	CatalogVersion string
	ProjectionID   string
	CollectionID   string
	AssetHost      string
	Manifest       []Asset
	Schema         Schema
}

type Asset struct {
	PartitionID   string     `json:"partition_id"`
	Href          string     `json:"href"`
	BBox          [4]float64 `json:"bbox"`
	RowCount      int64      `json:"row_count"`
	RowGroupCount int64      `json:"row_group_count"`
	SizeBytes     int64      `json:"size_bytes"`
}

type Schema struct {
	Columns           []Column `json:"columns"`
	GeoParquetVersion string   `json:"geoparquet_version"`
	PrimaryGeometry   string   `json:"primary_geometry"`
}

type Column struct {
	Name     string `json:"name"`
	Nullable string `json:"nullable,omitempty"`
	Type     string `json:"type,omitempty"`
}

type rootCatalog struct {
	Type   string     `json:"type"`
	ID     string     `json:"id"`
	Latest string     `json:"latest"`
	Links  []stacLink `json:"links"`
}

type releaseCatalog struct {
	Type           string     `json:"type"`
	ID             string     `json:"id"`
	ReleaseVersion string     `json:"release:version"`
	Links          []stacLink `json:"links"`
}

type placesCatalog struct {
	Type  string     `json:"type"`
	ID    string     `json:"id"`
	Links []stacLink `json:"links"`
}

type collectionDocument struct {
	Type               string        `json:"type"`
	ID                 string        `json:"id"`
	Links              []stacLink    `json:"links"`
	GeoParquetVersion  string        `json:"geoparquet:version"`
	PrimaryGeometry    string        `json:"table:primary_geometry"`
	TableColumns       []tableColumn `json:"table:columns"`
	PartitionScheme    string        `json:"partition:scheme"`
	PartitionGlob      string        `json:"partition:glob"`
	PartitionFileCount int           `json:"partition:file_count"`
}

type tableColumn struct {
	Name     string `json:"name"`
	Nullable string `json:"nullable"`
	Type     string `json:"type"`
}

type itemDocument struct {
	Type       string               `json:"type"`
	ID         string               `json:"id"`
	BBox       []json.RawMessage    `json:"bbox"`
	Properties itemProperties       `json:"properties"`
	Assets     map[string]stacAsset `json:"assets"`
}

type itemProperties struct {
	RowCount      int64 `json:"num_rows"`
	RowGroupCount int64 `json:"num_row_groups"`
}

type stacAsset struct {
	Href      string   `json:"href"`
	Type      string   `json:"type"`
	Roles     []string `json:"roles"`
	SizeBytes int64    `json:"file:size"`
}

type stacLink struct {
	Href  string `json:"href"`
	Rel   string `json:"rel"`
	Title string `json:"title"`
}

func New(options Options) (*Manager, error) {
	options = withDefaults(options)
	rootURL, err := url.Parse(options.CatalogURL)
	if err != nil {
		return nil, fmt.Errorf("parse catalog URL: %w", err)
	}
	if err := validateTrustedURL(rootURL, options.CatalogHost, true); err != nil {
		return nil, fmt.Errorf("validate catalog URL: %w", err)
	}
	if options.Timeout <= 0 {
		return nil, errors.New("catalog timeout must be positive")
	}
	if options.MaxResponseBytes <= 0 {
		return nil, errors.New("catalog response limit must be positive")
	}
	if options.CollectionID != DefaultCollectionID {
		return nil, fmt.Errorf("catalog collection %q is unsupported", options.CollectionID)
	}
	if err := validateFields(options.Fields); err != nil {
		return nil, err
	}
	clientCopy := *http.DefaultClient
	if options.Client != nil {
		clientCopy = *options.Client
	}
	client := &clientCopy
	existingRedirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if err := validateTrustedURL(request.URL, options.CatalogHost, true); err != nil {
			return fmt.Errorf("reject catalog redirect: %w", err)
		}
		if existingRedirect != nil {
			return existingRedirect(request, via)
		}
		return nil
	}
	return &Manager{
		options:     options,
		rootURL:     rootURL,
		client:      client,
		refreshGate: newRefreshGate(),
		responses:   make(map[string]cachedResponse),
	}, nil
}

func (m *Manager) Refresh(ctx context.Context) (Snapshot, error) {
	return m.refreshWithObservation(ctx, nil)
}

func (m *Manager) RefreshObserved(ctx context.Context, onObserved func(Observation)) (Snapshot, error) {
	return m.refreshWithObservation(ctx, onObserved)
}

func (m *Manager) refreshWithObservation(ctx context.Context, onObserved func(Observation)) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, errors.New("catalog refresh context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("catalog refresh canceled before start: %w", err)
	}
	refreshContext, cancel := context.WithTimeout(ctx, m.options.Timeout)
	defer cancel()
	select {
	case <-m.refreshGate:
		defer func() { m.refreshGate <- struct{}{} }()
	case <-refreshContext.Done():
		return Snapshot{}, fmt.Errorf("catalog refresh serialization: %w", refreshContext.Err())
	}
	snapshot, err := m.refresh(refreshContext, onObserved)
	if err != nil {
		return Snapshot{}, fmt.Errorf("refresh catalog: %w", err)
	}
	return snapshot, nil
}

func (m *Manager) refresh(ctx context.Context, onObserved func(Observation)) (Snapshot, error) {
	rootBytes, err := m.fetchJSON(ctx, m.rootURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("fetch root catalog: %w", err)
	}
	var root rootCatalog
	if err := json.Unmarshal(rootBytes, &root); err != nil {
		return Snapshot{}, fmt.Errorf("decode root catalog: %w", err)
	}
	if root.Type != stacCatalogType || root.Latest == "" {
		return Snapshot{}, errors.New("root catalog has invalid type or latest release")
	}
	if err := validateReleaseName(root.Latest); err != nil {
		return Snapshot{}, fmt.Errorf("root catalog latest release: %w", err)
	}
	if onObserved != nil {
		onObserved(Observation{Release: root.Latest})
	}
	releaseURL, err := m.findReleaseURL(root.Links, root.Latest)
	if err != nil {
		return Snapshot{}, err
	}
	releaseBytes, err := m.fetchJSON(ctx, releaseURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("fetch release catalog: %w", err)
	}
	var release releaseCatalog
	if err := json.Unmarshal(releaseBytes, &release); err != nil {
		return Snapshot{}, fmt.Errorf("decode release catalog: %w", err)
	}
	if release.Type != stacCatalogType || release.ID != root.Latest || (release.ReleaseVersion != "" && release.ReleaseVersion != root.Latest) {
		return Snapshot{}, fmt.Errorf("release catalog does not match latest release %q", root.Latest)
	}
	placesURL, err := m.findNamedLink(release.Links, stacChildRel, stacPlacesID, releaseURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolve places catalog: %w", err)
	}
	placesBytes, err := m.fetchJSON(ctx, placesURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("fetch places catalog: %w", err)
	}
	var places placesCatalog
	if err := json.Unmarshal(placesBytes, &places); err != nil {
		return Snapshot{}, fmt.Errorf("decode places catalog: %w", err)
	}
	if places.Type != stacCatalogType || places.ID != stacPlacesID {
		return Snapshot{}, errors.New("places catalog has invalid identity")
	}
	collectionURL, err := m.findNamedLink(places.Links, stacChildRel, m.options.CollectionID, placesURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolve collection: %w", err)
	}
	collectionBytes, err := m.fetchJSON(ctx, collectionURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("fetch collection: %w", err)
	}
	var collection collectionDocument
	if err := json.Unmarshal(collectionBytes, &collection); err != nil {
		return Snapshot{}, fmt.Errorf("decode collection: %w", err)
	}
	schema, itemLinks, err := m.validateCollection(collection, collectionURL, root.Latest)
	if err != nil {
		return Snapshot{}, err
	}
	manifest, err := m.fetchManifest(ctx, collectionURL, itemLinks, root.Latest)
	if err != nil {
		return Snapshot{}, err
	}
	versionInput := map[string]any{
		"asset_selection": map[string]string{
			"provider":       m.options.AssetProvider,
			"storage_region": m.options.StorageRegion,
			"type":           m.options.AssetType,
		},
		"collection_id": m.options.CollectionID,
		"manifest":      manifest,
		"release":       root.Latest,
		"schema":        schema,
	}
	catalogVersionHash, _, err := canonicalSHA256(versionInput)
	if err != nil {
		return Snapshot{}, fmt.Errorf("hash catalog manifest: %w", err)
	}
	catalogVersion := root.Latest + "+" + catalogVersionHash
	if onObserved != nil {
		onObserved(Observation{Release: root.Latest, CatalogVersion: catalogVersion})
	}
	projectionID, err := projectionID(m.options.Fields, schema)
	if err != nil {
		return Snapshot{}, fmt.Errorf("hash projection: %w", err)
	}
	return Snapshot{
		Release:        root.Latest,
		CatalogVersion: catalogVersion,
		ProjectionID:   projectionID,
		CollectionID:   m.options.CollectionID,
		AssetHost:      m.options.AssetHost,
		Manifest:       manifest,
		Schema:         schema,
	}, nil
}

func (m *Manager) findReleaseURL(links []stacLink, release string) (*url.URL, error) {
	for _, link := range links {
		if link.Rel != stacChildRel {
			continue
		}
		candidate, err := m.resolveCatalogURL(m.rootURL, link.Href)
		if err != nil {
			return nil, fmt.Errorf("validate release link: %w", err)
		}
		if link.Title == release || strings.Contains(candidate.Path, "/"+release+"/") {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("root catalog has no child for latest release %q", release)
}

func (m *Manager) findNamedLink(links []stacLink, rel, name string, baseURL *url.URL) (*url.URL, error) {
	for _, link := range links {
		if link.Rel != rel || (link.Title != "" && link.Title != name) {
			continue
		}
		candidate, err := m.resolveCatalogURL(baseURL, link.Href)
		if err != nil {
			return nil, err
		}
		if strings.Contains(candidate.Path, "/"+name+"/") || strings.HasSuffix(candidate.Path, "/"+name+"/catalog.json") || strings.HasSuffix(candidate.Path, "/"+name+"/collection.json") {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("no %s link named %q", rel, name)
}

func (m *Manager) resolveCatalogURL(baseURL *url.URL, rawURL string) (*url.URL, error) {
	if rawURL == "" {
		return nil, errors.New("catalog link is empty")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse catalog link %q: %w", rawURL, err)
	}
	candidate := baseURL.ResolveReference(parsed)
	if err := validateTrustedURL(candidate, m.options.CatalogHost, true); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (m *Manager) validateCollection(collection collectionDocument, collectionURL *url.URL, release string) (Schema, []stacLink, error) {
	if collection.Type != stacCollectionType || collection.ID != m.options.CollectionID {
		return Schema{}, nil, errors.New("collection has invalid identity")
	}
	if collection.GeoParquetVersion != "1.1.0" {
		return Schema{}, nil, fmt.Errorf("collection GeoParquet version %q is unsupported", collection.GeoParquetVersion)
	}
	if collection.PrimaryGeometry != stacPrimaryGeometry {
		return Schema{}, nil, fmt.Errorf("collection primary geometry %q is unsupported", collection.PrimaryGeometry)
	}
	if collection.PartitionScheme != "hive" || collection.PartitionFileCount <= 0 || collection.PartitionGlob == "" {
		return Schema{}, nil, errors.New("collection partition metadata is invalid")
	}
	globURL, err := url.Parse(collection.PartitionGlob)
	if err != nil {
		return Schema{}, nil, fmt.Errorf("validate partition glob: %w", err)
	}
	if err := validateTrustedURL(globURL, m.options.AssetHost, true); err != nil {
		return Schema{}, nil, fmt.Errorf("validate partition glob: %w", err)
	}
	prefix := placesAssetPrefix(release)
	if !strings.HasPrefix(globURL.Path, prefix) || !strings.HasSuffix(globURL.Path, stacPartitionGlobTail) || path.Clean(globURL.Path) != globURL.Path {
		return Schema{}, nil, errors.New("partition glob is outside the trusted places asset prefix")
	}
	columns := make([]Column, 0, len(collection.TableColumns))
	seen := make(map[string]struct{}, len(collection.TableColumns))
	for _, column := range collection.TableColumns {
		if !isIdentifier(column.Name) {
			return Schema{}, nil, fmt.Errorf("collection has invalid column name %q", column.Name)
		}
		if _, ok := seen[column.Name]; ok {
			return Schema{}, nil, fmt.Errorf("collection repeats column %q", column.Name)
		}
		seen[column.Name] = struct{}{}
		columns = append(columns, Column(column))
	}
	for _, field := range m.options.Fields {
		if _, ok := seen[field]; !ok {
			return Schema{}, nil, fmt.Errorf("collection does not contain selected field %q", field)
		}
	}
	if len(columns) == 0 {
		return Schema{}, nil, errors.New("collection schema has no columns")
	}
	itemLinks := make([]stacLink, 0, len(collection.Links))
	seenItems := make(map[string]struct{})
	for _, link := range collection.Links {
		if link.Rel != "item" {
			continue
		}
		itemURL, err := m.resolveCatalogURL(collectionURL, link.Href)
		if err != nil {
			return Schema{}, nil, fmt.Errorf("validate item link: %w", err)
		}
		if _, ok := seenItems[itemURL.String()]; ok {
			return Schema{}, nil, fmt.Errorf("collection repeats item link %q", itemURL)
		}
		seenItems[itemURL.String()] = struct{}{}
		itemLinks = append(itemLinks, stacLink{Href: itemURL.String(), Rel: link.Rel, Title: link.Title})
	}
	if len(itemLinks) != collection.PartitionFileCount {
		return Schema{}, nil, fmt.Errorf("collection has %d item links, want %d", len(itemLinks), collection.PartitionFileCount)
	}
	return Schema{Columns: columns, GeoParquetVersion: collection.GeoParquetVersion, PrimaryGeometry: collection.PrimaryGeometry}, itemLinks, nil
}

func (m *Manager) fetchManifest(ctx context.Context, collectionURL *url.URL, itemLinks []stacLink, release string) ([]Asset, error) {
	assets := make([]Asset, len(itemLinks))
	errorsByIndex := make([]error, len(itemLinks))
	var waitGroup sync.WaitGroup
	for index, link := range itemLinks {
		waitGroup.Add(1)
		go func(index int, link stacLink) {
			defer waitGroup.Done()
			itemURL, err := m.resolveCatalogURL(collectionURL, link.Href)
			if err != nil {
				errorsByIndex[index] = fmt.Errorf("resolve item %d: %w", index, err)
				return
			}
			expectedID, err := itemIDFromURL(itemURL)
			if err != nil {
				errorsByIndex[index] = fmt.Errorf("validate item %d URL: %w", index, err)
				return
			}
			itemBytes, err := m.fetchJSON(ctx, itemURL)
			if err != nil {
				errorsByIndex[index] = fmt.Errorf("fetch item %d: %w", index, err)
				return
			}
			var item itemDocument
			if err := json.Unmarshal(itemBytes, &item); err != nil {
				errorsByIndex[index] = fmt.Errorf("decode item %d: %w", index, err)
				return
			}
			if item.ID != expectedID {
				errorsByIndex[index] = fmt.Errorf("validate item %d: id %q does not match URL partition %q", index, item.ID, expectedID)
				return
			}
			asset, err := m.assetFromItem(item, release)
			if err != nil {
				errorsByIndex[index] = fmt.Errorf("validate item %d: %w", index, err)
				return
			}
			assets[index] = asset
		}(index, link)
	}
	waitGroup.Wait()
	for _, err := range errorsByIndex {
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(assets, func(left, right int) bool { return assets[left].Href < assets[right].Href })
	seenPartitions := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		if _, ok := seenPartitions[asset.PartitionID]; ok {
			return nil, fmt.Errorf("manifest repeats partition %q", asset.PartitionID)
		}
		seenPartitions[asset.PartitionID] = struct{}{}
	}
	return assets, nil
}

func (m *Manager) assetFromItem(item itemDocument, release string) (Asset, error) {
	if item.Type != stacFeatureType || item.ID == "" {
		return Asset{}, errors.New("item has invalid type or id")
	}
	if len(item.BBox) != 4 {
		return Asset{}, fmt.Errorf("item %q has invalid bbox", item.ID)
	}
	var bbox [4]float64
	for index, encodedValue := range item.BBox {
		var value *float64
		if err := json.Unmarshal(encodedValue, &value); err != nil || value == nil {
			return Asset{}, fmt.Errorf("item %q bbox value %d is not a number", item.ID, index)
		}
		bbox[index] = *value
	}
	if err := validateBBox(bbox); err != nil {
		return Asset{}, fmt.Errorf("item %q bbox: %w", item.ID, err)
	}
	asset, ok := item.Assets[m.options.AssetProvider]
	if !ok {
		return Asset{}, fmt.Errorf("item %q has no %q asset", item.ID, m.options.AssetProvider)
	}
	if asset.Type != m.options.AssetType || !contains(asset.Roles, stacDataRole) {
		return Asset{}, fmt.Errorf("item %q asset has invalid type or role", item.ID)
	}
	assetURL, err := url.Parse(asset.Href)
	if err != nil {
		return Asset{}, fmt.Errorf("item %q asset URL: %w", item.ID, err)
	}
	if err := validateTrustedURL(assetURL, m.options.AssetHost, true); err != nil {
		return Asset{}, fmt.Errorf("item %q asset URL: %w", item.ID, err)
	}
	prefix := placesAssetPrefix(release)
	assetName := path.Base(assetURL.Path)
	if !strings.HasPrefix(assetURL.Path, prefix) || !strings.HasPrefix(assetName, "part-"+item.ID+"-") || !strings.HasSuffix(assetName, stacAssetPathSuffix) || path.Clean(assetURL.Path) != assetURL.Path {
		return Asset{}, fmt.Errorf("item %q asset URL is outside the trusted places prefix", item.ID)
	}
	if asset.SizeBytes <= 0 || item.Properties.RowCount <= 0 || item.Properties.RowGroupCount <= 0 {
		return Asset{}, fmt.Errorf("item %q has invalid size or row metadata", item.ID)
	}
	return Asset{
		PartitionID:   item.ID,
		Href:          assetURL.String(),
		BBox:          bbox,
		RowCount:      item.Properties.RowCount,
		RowGroupCount: item.Properties.RowGroupCount,
		SizeBytes:     asset.SizeBytes,
	}, nil
}

func (m *Manager) fetchJSON(ctx context.Context, target *url.URL) ([]byte, error) {
	if err := validateTrustedURL(target, m.options.CatalogHost, true); err != nil {
		return nil, err
	}
	key := target.String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, fmt.Errorf("create catalog request: %w", err)
	}
	m.responsesMu.Lock()
	cached, hasCached := m.responses[key]
	m.responsesMu.Unlock()
	if hasCached && cached.etag != "" {
		request.Header.Set("If-None-Match", cached.etag)
	}
	if hasCached && cached.lastModified != "" {
		request.Header.Set("If-Modified-Since", cached.lastModified)
	}
	response, err := m.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", key, err)
	}
	if response.StatusCode == http.StatusNotModified {
		if err := closeResponse(response); err != nil {
			return nil, err
		}
		if !hasCached {
			return nil, fmt.Errorf("catalog returned 304 without a cached body for %s", key)
		}
		cached.etag = headerOr(cached.etag, response.Header.Get("ETag"))
		cached.lastModified = headerOr(cached.lastModified, response.Header.Get("Last-Modified"))
		m.responsesMu.Lock()
		m.responses[key] = cached
		m.responsesMu.Unlock()
		return append([]byte(nil), cached.body...), nil
	}
	if response.StatusCode != http.StatusOK {
		status := response.Status
		if err := closeResponse(response); err != nil {
			return nil, fmt.Errorf("catalog request %s returned %s: %w", key, status, err)
		}
		return nil, fmt.Errorf("catalog request %s returned %s", key, status)
	}
	body, err := readResponse(response, m.options.MaxResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("read catalog response %s: %w", key, err)
	}
	m.responsesMu.Lock()
	m.responses[key] = cachedResponse{
		body:         append([]byte(nil), body...),
		etag:         response.Header.Get("ETag"),
		lastModified: response.Header.Get("Last-Modified"),
	}
	m.responsesMu.Unlock()
	return body, nil
}

func readResponse(response *http.Response, maxBytes int64) ([]byte, error) {
	if response.ContentLength > maxBytes {
		if err := closeResponse(response); err != nil {
			return nil, fmt.Errorf("response exceeds %d bytes and close failed: %w", maxBytes, err)
		}
		return nil, fmt.Errorf("response exceeds %d bytes", maxBytes)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read response body: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close response body: %w", closeErr)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response exceeds %d bytes", maxBytes)
	}
	return body, nil
}

func closeResponse(response *http.Response) error {
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close catalog response: %w", err)
	}
	return nil
}

func withDefaults(options Options) Options {
	if options.CatalogURL == "" {
		options.CatalogURL = DefaultCatalogURL
	}
	if options.CatalogHost == "" {
		options.CatalogHost = DefaultCatalogHost
	}
	if options.AssetHost == "" {
		options.AssetHost = DefaultAssetHost
	}
	if options.AssetProvider == "" {
		options.AssetProvider = DefaultAssetProvider
	}
	if options.StorageRegion == "" {
		options.StorageRegion = DefaultStorageRegion
	}
	if options.AssetType == "" {
		options.AssetType = DefaultAssetType
	}
	if options.CollectionID == "" {
		options.CollectionID = DefaultCollectionID
	}
	if len(options.Fields) == 0 {
		options.Fields = []string{"id", stacPrimaryGeometry, "names", "basic_category"}
	}
	if options.Timeout == 0 {
		options.Timeout = 10 * time.Second
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = DefaultMaxResponseBytes
	}
	fields := make([]string, len(options.Fields))
	for index, field := range options.Fields {
		fields[index] = strings.TrimSpace(field)
	}
	options.Fields = fields
	return options
}

func newRefreshGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

func validateTrustedURL(target *url.URL, trustedHost string, requireHTTPS bool) error {
	if target == nil || target.Host == "" {
		return errors.New("URL has no host")
	}
	if requireHTTPS && target.Scheme != "https" {
		return fmt.Errorf("URL scheme %q is not HTTPS", target.Scheme)
	}
	if !strings.EqualFold(target.Host, trustedHost) {
		return fmt.Errorf("URL host %q is not trusted", target.Host)
	}
	if target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return errors.New("URL contains user info, query, or fragment")
	}
	return nil
}

func validateFields(fields []string) error {
	if len(fields) == 0 {
		return errors.New("catalog fields must not be empty")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if !isIdentifier(field) {
			return fmt.Errorf("catalog field %q is not a simple identifier", field)
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("catalog field %q is repeated", field)
		}
		seen[field] = struct{}{}
	}
	for _, required := range []string{"id", stacPrimaryGeometry} {
		if _, ok := seen[required]; !ok {
			return fmt.Errorf("catalog fields must include %q", required)
		}
	}
	return nil
}

func isIdentifier(value string) bool {
	if value == "" || (value[0] < 'A' || value[0] > 'Z') && (value[0] < 'a' || value[0] > 'z') && value[0] != '_' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validateBBox(bbox [4]float64) error {
	for _, value := range bbox {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return errors.New("bbox contains a non-finite value")
		}
	}
	if bbox[0] < -180 || bbox[0] > 180 || bbox[2] < -180 || bbox[2] > 180 || bbox[1] < -90 || bbox[1] > 90 || bbox[3] < -90 || bbox[3] > 90 {
		return errors.New("bbox is outside longitude/latitude limits")
	}
	if bbox[1] > bbox[3] {
		return errors.New("bbox latitude bounds are inverted")
	}
	return nil
}

func validateReleaseName(release string) error {
	if release == "." || release == ".." || strings.ContainsAny(release, `/\\?#%`) {
		return fmt.Errorf("release name %q is not a safe path segment", release)
	}
	for _, character := range release {
		if character < 0x20 || character == 0x7f {
			return errors.New("release name contains control characters")
		}
	}
	return nil
}

func placesAssetPrefix(release string) string {
	return fmt.Sprintf("/release/%s/theme=%s/type=%s/", release, stacPlacesTheme, stacPlaceType)
}

func itemIDFromURL(target *url.URL) (string, error) {
	cleanPath := path.Clean(target.Path)
	if cleanPath != target.Path {
		return "", errors.New("item URL path is not clean")
	}
	filename := path.Base(cleanPath)
	if !strings.HasSuffix(filename, ".json") {
		return "", errors.New("item URL does not name a JSON document")
	}
	itemID := strings.TrimSuffix(filename, ".json")
	if itemID == "" || path.Base(path.Dir(cleanPath)) != itemID {
		return "", errors.New("item URL does not use the partition ID path")
	}
	return itemID, nil
}

func contains(values []string, wanted string) bool {
	return slices.Contains(values, wanted)
}

func headerOr(previous, current string) string {
	if current != "" {
		return current
	}
	return previous
}

func projectionID(fields []string, schema Schema) (string, error) {
	selectedColumns := make([]Column, 0, len(fields))
	for _, field := range fields {
		found := false
		for _, column := range schema.Columns {
			if column.Name == field {
				selectedColumns = append(selectedColumns, column)
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("selected field %q is not in schema", field)
		}
	}
	input := map[string]any{
		"fields":           fields,
		"h3_semantics":     DefaultProjectionH3Semantics,
		"bbox_semantics":   DefaultProjectionBBoxSemantics,
		"resolved_columns": selectedColumns,
		"writer_format":    DefaultProjectionWriter,
	}
	digest, _, err := canonicalSHA256(input)
	return digest, err
}
