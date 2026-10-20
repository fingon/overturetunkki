package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/h3filter"
	"github.com/fingon/overturetunkki/internal/tilequery"
	"github.com/uber/h3-go/v4"
)

const (
	identifierID       = "id"
	assetPathSuffix    = ".zstd.parquet"
	placesTheme        = "places"
	placesType         = "place"
	geometryPointType  = "ST_POINT"
	geometryInvalidSQL = "CAST('invalid geometry' AS DOUBLE)"
	candidateTableName = `"__overture_candidates"`
)

var (
	ErrInvalidCell         = errors.New("invalid H3 cell")
	ErrInvalidProjection   = errors.New("invalid source projection")
	ErrZoomTooCoarse       = errors.New("tile zoom is below the minimum")
	ErrResolutionTooCoarse = errors.New("tile resolution is below the minimum")
	ErrTooManyRows         = errors.New("candidate rows exceed row limit")
)

type ResolutionTooCoarseError struct {
	ActualResolution int
	MinResolution    int
}

func (err *ResolutionTooCoarseError) Error() string {
	return fmt.Sprintf("tile resolution %d is below minimum resolution %d", err.ActualResolution, err.MinResolution)
}

func (*ResolutionTooCoarseError) Unwrap() error {
	return ErrResolutionTooCoarse
}

type ZoomTooCoarseError struct {
	ActualZoom int
	MinZoom    int
}

func (err *ZoomTooCoarseError) Error() string {
	return fmt.Sprintf("tile zoom %d is below minimum zoom %d", err.ActualZoom, err.MinZoom)
}

func (*ZoomTooCoarseError) Unwrap() error { return ErrZoomTooCoarse }

type TileRequest = tilequery.Query

type QueryPlan struct {
	Square        *tilequery.Square
	Empty         bool
	Cell          h3.Cell
	CellText      string
	CellValue     uint64
	Columns       []catalog.Column
	ProjectionSQL string
}

type PreparedQuery struct {
	Profile            *TileDiagnostics
	SQL                string
	Args               []any
	SelectedAssetCount int
	SelectedAssetBytes int64
}

type TileBuildRequest struct {
	Conn        *sql.Conn
	Plan        QueryPlan
	Snapshot    catalog.Snapshot
	MaxRows     int64
	OutputPath  string
	MaxBytes    int64
	Diagnostics *TileDiagnostics
	SourcePaths []string
}

type candidateCopyRequest struct {
	Context    context.Context
	Conn       *sql.Conn
	OutputPath string
	Metadata   []byte
	MaxBytes   int64
}

type SourceSquareError struct {
	Square tilequery.Square
	Err    error
}

func (err *SourceSquareError) Error() string {
	return fmt.Sprintf("source square %s is too large: %v", err.Square, err.Err)
}
func (err *SourceSquareError) Unwrap() error { return err.Err }

type TooManyRowsError struct {
	ActualRows int64
	LimitRows  int64
}

func (err *TooManyRowsError) Error() string {
	return fmt.Sprintf("candidate query produced %d rows, limit is %d rows", err.ActualRows, err.LimitRows)
}

func (*TooManyRowsError) Unwrap() error {
	return ErrTooManyRows
}

type candidateRows struct {
	rowCount int64
}

func BuildCandidateQuery(plan QueryPlan, snapshot catalog.Snapshot, maxRows int64) (PreparedQuery, error) {
	if !plan.valid() {
		return PreparedQuery{}, fmt.Errorf("%w: query plan cell is invalid", ErrInvalidCell)
	}
	if len(plan.Columns) == 0 {
		return PreparedQuery{}, fmt.Errorf("%w: query plan has no selected columns", ErrInvalidProjection)
	}
	if snapshot.Release == "" {
		return PreparedQuery{}, errors.New("build candidate query: catalog release is empty")
	}
	if snapshot.CollectionID != catalog.DefaultCollectionID {
		return PreparedQuery{}, fmt.Errorf("build candidate query: catalog collection %q is unsupported", snapshot.CollectionID)
	}
	rowLimit, err := CandidateRowLimit(maxRows)
	if err != nil {
		return PreparedQuery{}, fmt.Errorf("build candidate query: %w", err)
	}
	bounds, err := plan.bounds()
	if err != nil {
		return PreparedQuery{}, fmt.Errorf("build candidate query bounds: %w", err)
	}
	assetURLs, selectedBytes, err := pinnedAssetURLs(snapshot, bounds)
	if err != nil {
		return PreparedQuery{}, err
	}
	selectedCount := len(assetURLs)
	if selectedCount == 0 {
		return PreparedQuery{
			SQL:  "SELECT " + candidateProjectionSQL(plan.Columns) + " FROM read_parquet([?]) WHERE FALSE",
			Args: []any{snapshot.Manifest[0].Href},
		}, nil
	}
	projectionSQL := candidateProjectionSQL(plan.Columns)
	placeholders := make([]string, len(assetURLs))
	for index := range assetURLs {
		placeholders[index] = "?"
	}
	branches := bounds.DuckDBBBoxBranches()
	queries := make([]string, 0, len(branches))
	var args []any
	for _, branch := range branches {
		queries = append(queries, fmt.Sprintf("SELECT %s FROM read_parquet([%s]) WHERE (%s) AND %s",
			projectionSQL, strings.Join(placeholders, ", "), branch.Predicate, plan.membershipSQL()))
		for _, assetURL := range assetURLs {
			args = append(args, assetURL)
		}
		args = append(args, branch.Args...)
		args = append(args, plan.membershipArgs()...)
	}
	args = append(args, rowLimit)
	return PreparedQuery{
		SQL:                "SELECT * FROM (" + strings.Join(queries, " UNION ALL ") + ") AS candidates LIMIT ?",
		Args:               args,
		SelectedAssetCount: selectedCount,
		SelectedAssetBytes: selectedBytes,
	}, nil
}

func BuildCachedCandidateQuery(plan QueryPlan, sourcePaths []string, maxRows int64) (PreparedQuery, error) {
	if !plan.valid() || len(plan.Columns) == 0 {
		return PreparedQuery{}, errors.New("build cached candidate query: invalid query plan")
	}
	if len(sourcePaths) == 0 || len(sourcePaths) > tilequery.MaxSources {
		return PreparedQuery{}, errors.New("build cached candidate query: invalid source count")
	}
	rowLimit, err := CandidateRowLimit(maxRows)
	if err != nil {
		return PreparedQuery{}, fmt.Errorf("build cached candidate query: %w", err)
	}
	placeholders := make([]string, len(sourcePaths))
	args := make([]any, 0, len(sourcePaths)+2)
	seen := make(map[string]struct{}, len(sourcePaths))
	for index, sourcePath := range sourcePaths {
		if !filepath.IsAbs(sourcePath) || filepath.Ext(sourcePath) != ".parquet" || strings.ContainsAny(sourcePath, "*?[") {
			return PreparedQuery{}, fmt.Errorf("build cached candidate query: source is not an absolute parquet file: %q", sourcePath)
		}
		if _, exists := seen[sourcePath]; exists {
			return PreparedQuery{}, errors.New("build cached candidate query: duplicate source file")
		}
		seen[sourcePath] = struct{}{}
		placeholders[index] = "?"
		args = append(args, sourcePath)
	}
	args = append(args, plan.membershipArgs()...)
	args = append(args, rowLimit)
	return PreparedQuery{
		SQL:  fmt.Sprintf("SELECT %s FROM read_parquet([%s]) WHERE %s QUALIFY row_number() OVER (PARTITION BY id ORDER BY id) = 1 LIMIT ?", candidateProjectionSQL(plan.Columns), strings.Join(placeholders, ", "), plan.membershipSQL()),
		Args: args,
	}, nil
}

func PrepareTileQuery(plan QueryPlan, snapshot catalog.Snapshot, sourcePaths []string, maxRows int64) (PreparedQuery, error) {
	if plan.Empty {
		return emptyCandidateQuery(plan, snapshot, maxRows)
	}
	if len(sourcePaths) > 0 {
		return BuildCachedCandidateQuery(plan, sourcePaths, maxRows)
	}
	return BuildCandidateQuery(plan, snapshot, maxRows)
}

func emptyCandidateQuery(plan QueryPlan, snapshot catalog.Snapshot, maxRows int64) (PreparedQuery, error) {
	if !plan.valid() || len(plan.Columns) == 0 {
		return PreparedQuery{}, errors.New("empty tile: invalid query plan")
	}
	if _, err := CandidateRowLimit(maxRows); err != nil {
		return PreparedQuery{}, err
	}
	assetURLs, _, err := pinnedAssetURLs(snapshot, h3filter.GlobalBounds())
	if err != nil {
		return PreparedQuery{}, err
	}
	if len(assetURLs) == 0 {
		return PreparedQuery{}, errors.New("empty tile: catalog has no schema source")
	}
	return PreparedQuery{SQL: "SELECT " + candidateProjectionSQL(plan.Columns) + " FROM read_parquet([?]) WHERE FALSE", Args: []any{assetURLs[0]}}, nil
}

func BuildTile(ctx context.Context, request TileBuildRequest) (CopyResult, error) {
	if ctx == nil {
		return CopyResult{}, errors.New("build tile: context is nil")
	}
	if request.Conn == nil {
		return CopyResult{}, errors.New("build tile: DuckDB connection is nil")
	}
	finishPlanning := request.Diagnostics.start(tileStagePlanning)
	query, err := PrepareTileQuery(request.Plan, request.Snapshot, request.SourcePaths, request.MaxRows)
	finishPlanning()
	if err != nil {
		return CopyResult{}, err
	}
	if request.Diagnostics != nil {
		request.Diagnostics.SelectedAssetCount = query.SelectedAssetCount
		request.Diagnostics.SelectedAssetBytes = query.SelectedAssetBytes
		if request.Diagnostics.ProfileEnabled {
			query.Profile = request.Diagnostics
		}
	}
	metadata, err := candidateMetadata(request.Plan)
	if err != nil {
		return CopyResult{}, err
	}
	finishMaterialize := request.Diagnostics.start(tileStageMaterialize)
	candidates, err := materializeCandidates(ctx, request.Conn, query, request.MaxRows)
	finishMaterialize()
	if err != nil {
		return CopyResult{}, err
	}
	finishCopy := request.Diagnostics.start(tileStageCopy)
	copyResult, copyErr := candidates.copy(candidateCopyRequest{
		Context:    ctx,
		Conn:       request.Conn,
		OutputPath: request.OutputPath,
		Metadata:   metadata,
		MaxBytes:   request.MaxBytes,
	})
	finishCopy()
	dropErr := candidates.drop(ctx, request.Conn)
	if copyErr != nil {
		if dropErr != nil {
			return CopyResult{}, fmt.Errorf("%w; cleanup candidates: %w", copyErr, dropErr)
		}
		return CopyResult{}, copyErr
	}
	if dropErr != nil {
		return CopyResult{}, dropErr
	}
	return copyResult, nil
}

func materializeCandidates(ctx context.Context, conn *sql.Conn, query PreparedQuery, maxRows int64) (candidateRows, error) {
	if ctx == nil {
		return candidateRows{}, errors.New("materialize candidates: context is nil")
	}
	if conn == nil {
		return candidateRows{}, errors.New("materialize candidates: DuckDB connection is nil")
	}
	if strings.TrimSpace(query.SQL) == "" {
		return candidateRows{}, errors.New("materialize candidates: query is empty")
	}
	if _, err := CandidateRowLimit(maxRows); err != nil {
		return candidateRows{}, fmt.Errorf("materialize candidates: %w", err)
	}
	if err := dropCandidateTable(ctx, conn); err != nil {
		return candidateRows{}, err
	}
	createSQL := "CREATE TEMPORARY TABLE " + candidateTableName + " AS " + query.SQL
	if query.Profile != nil {
		var profileKey string
		if err := conn.QueryRowContext(ctx, "EXPLAIN ANALYZE "+createSQL, query.Args...).Scan(&profileKey, &query.Profile.MaterializeProfile); err != nil {
			return candidateRows{}, fmt.Errorf("profile materialized candidates: %w", err)
		}
	} else {
		if _, err := conn.ExecContext(ctx, createSQL, query.Args...); err != nil {
			return candidateRows{}, fmt.Errorf("materialize candidates: %w", err)
		}
	}
	var rowCount int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM "+candidateTableName).Scan(&rowCount); err != nil {
		return candidateRows{}, cleanupCandidateTable(ctx, conn, fmt.Errorf("count materialized candidates: %w", err))
	}
	if rowCount > maxRows {
		return candidateRows{}, cleanupCandidateTable(ctx, conn, &TooManyRowsError{ActualRows: rowCount, LimitRows: maxRows})
	}
	return candidateRows{rowCount: rowCount}, nil
}

func (rows candidateRows) copy(request candidateCopyRequest) (CopyResult, error) {
	if rows.rowCount < 0 {
		return CopyResult{}, errors.New("copy candidates: row count is negative")
	}
	copySQL, err := geoparquet.CopySQL("SELECT * FROM "+candidateTableName+" ORDER BY id", request.OutputPath, request.Metadata)
	if err != nil {
		return CopyResult{}, fmt.Errorf("copy candidates: %w", err)
	}
	return CopyWithOutputLimit(request.Context, CopyRequest{Conn: request.Conn, Query: copySQL, OutputPath: request.OutputPath, MaxBytes: request.MaxBytes})
}

func (rows candidateRows) drop(ctx context.Context, conn *sql.Conn) error {
	return dropCandidateTable(ctx, conn)
}

func candidateMetadata(plan QueryPlan) ([]byte, error) {
	bounds, err := plan.bounds()
	if err != nil {
		return nil, fmt.Errorf("build tile metadata bounds: %w", err)
	}
	if len(bounds.LongitudeIntervals) != 1 || bounds.IsGlobal() {
		return geoparquet.PointMetadata(-180, -90, 180, 90)
	}
	interval := bounds.LongitudeIntervals[0]
	return geoparquet.PointMetadata(interval.MinDeg, bounds.LatitudeMinDeg, interval.MaxDeg, bounds.LatitudeMaxDeg)
}

func dropCandidateTable(ctx context.Context, conn *sql.Conn) error {
	if ctx == nil {
		return errors.New("drop candidates: context is nil")
	}
	if conn == nil {
		return errors.New("drop candidates: DuckDB connection is nil")
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+candidateTableName); err != nil {
		return fmt.Errorf("drop candidates: %w", err)
	}
	return nil
}

func cleanupCandidateTable(ctx context.Context, conn *sql.Conn, cause error) error {
	if err := dropCandidateTable(ctx, conn); err != nil {
		return fmt.Errorf("%w; cleanup candidates: %w", cause, err)
	}
	return cause
}

func BuildQueryPlan(request TileRequest, fields []string, schema catalog.Schema) (QueryPlan, error) {
	if err := request.Validate(); err != nil {
		return QueryPlan{}, err
	}
	columns, projectionSQL, err := validateProjection(fields, schema)
	if err != nil {
		return QueryPlan{}, err
	}
	plan := QueryPlan{Columns: columns, ProjectionSQL: projectionSQL}
	if request.Square != nil {
		square := *request.Square
		plan.Square = &square
		return plan, nil
	}
	cell, err := ParseCanonicalCell(request.Cell)
	if err != nil {
		return QueryPlan{}, err
	}
	plan.Cell = cell
	plan.CellText = cell.String()
	plan.CellValue = uint64(cell)
	bounds, err := h3filter.CellBounds(cell)
	if err != nil {
		return QueryPlan{}, err
	}
	plan.Empty = bounds.LatitudeMinDeg > tilequery.LatitudeLimitDeg || bounds.LatitudeMaxDeg < -tilequery.LatitudeLimitDeg
	return plan, nil
}

func (plan QueryPlan) valid() bool {
	if plan.Square != nil {
		return plan.Square.Validate() == nil && !plan.Cell.IsValid()
	}
	return plan.Cell.IsValid()
}

func (plan QueryPlan) bounds() (h3filter.Bounds, error) {
	if plan.Square != nil {
		rect := plan.Square.Bounds()
		return h3filter.Bounds{
			LatitudeMinDeg: rect.SouthDeg, LatitudeMaxDeg: rect.NorthDeg,
			LongitudeIntervals: squareIntervals(*plan.Square, rect),
		}, nil
	}
	return h3filter.CellBounds(plan.Cell)
}

func squareIntervals(square tilequery.Square, rect tilequery.Rect) []h3filter.LongitudeInterval {
	intervals := []h3filter.LongitudeInterval{{MinDeg: rect.WestDeg, MaxDeg: rect.EastDeg}}
	if square.X == 0 && square.Z > 0 {
		intervals = append(intervals, h3filter.LongitudeInterval{MinDeg: tilequery.MaxLongitudeDeg, MaxDeg: tilequery.MaxLongitudeDeg})
	}
	return intervals
}

func (plan QueryPlan) membershipSQL() string {
	if plan.Square != nil {
		southComparison := ">"
		if plan.Square.Y == (1<<plan.Square.Z)-1 {
			southComparison = ">="
		}
		return fmt.Sprintf("(%s >= ? AND %s < ? AND %s <= ? AND %s %s ?)", normalizedLongitudeSQL(), normalizedLongitudeSQL(), latitudeSQL(), latitudeSQL(), southComparison)
	}
	return exactMembershipSQL() + " AND " + latitudeSQL() + " BETWEEN ? AND ?"
}

func (plan QueryPlan) membershipArgs() []any {
	if plan.Square != nil {
		bounds := plan.Square.Bounds()
		return []any{bounds.WestDeg, bounds.EastDeg, bounds.NorthDeg, bounds.SouthDeg}
	}
	return []any{plan.CellValue, -tilequery.LatitudeLimitDeg, tilequery.LatitudeLimitDeg}
}

func ParseCanonicalCell(value string) (h3.Cell, error) {
	cell := h3.CellFromString(value)
	if !cell.IsValid() {
		return 0, fmt.Errorf("%w: %q is not a valid H3 cell", ErrInvalidCell, value)
	}
	if cell.String() != value {
		return 0, fmt.Errorf("%w: %q is not canonical, want %q", ErrInvalidCell, value, cell)
	}
	return cell, nil
}

func pinnedAssetURLs(snapshot catalog.Snapshot, bounds h3filter.Bounds) (assetURLs []string, selectedBytes int64, err error) {
	if len(snapshot.Manifest) == 0 {
		return nil, 0, errors.New("build candidate query: catalog manifest is empty")
	}
	assetHost := snapshot.AssetHost
	if assetHost == "" {
		assetHost = catalog.DefaultAssetHost
	}
	prefix := fmt.Sprintf("/release/%s/theme=%s/type=%s/", snapshot.Release, placesTheme, placesType)
	assetURLs = make([]string, 0, len(snapshot.Manifest))
	seen := make(map[string]struct{}, len(snapshot.Manifest))
	for index, asset := range snapshot.Manifest {
		parsed, err := url.Parse(asset.Href)
		if err != nil {
			return nil, 0, fmt.Errorf("validate manifest asset %d: %w", index, err)
		}
		if parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, assetHost) || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, 0, fmt.Errorf("validate manifest asset %d: URL is not a trusted HTTPS asset", index)
		}
		assetName := path.Base(parsed.Path)
		if path.Clean(parsed.Path) != parsed.Path || !strings.HasPrefix(parsed.Path, prefix) || !strings.HasPrefix(assetName, "part-"+asset.PartitionID+"-") || !strings.HasSuffix(assetName, assetPathSuffix) {
			return nil, 0, fmt.Errorf("validate manifest asset %d: URL is outside the trusted places prefix", index)
		}
		if asset.PartitionID == "" || asset.SizeBytes <= 0 || asset.RowCount <= 0 || asset.RowGroupCount <= 0 {
			return nil, 0, fmt.Errorf("validate manifest asset %d: metadata is invalid", index)
		}
		if _, ok := seen[asset.Href]; ok {
			return nil, 0, fmt.Errorf("validate manifest asset %d: URL is repeated", index)
		}
		seen[asset.Href] = struct{}{}
		if bounds.IntersectsBBox(asset.BBox[0], asset.BBox[2], asset.BBox[1], asset.BBox[3]) {
			assetURLs = append(assetURLs, asset.Href)
			selectedBytes += asset.SizeBytes
		}
	}
	return assetURLs, selectedBytes, nil
}

func candidateProjectionSQL(columns []catalog.Column) string {
	selections := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedName := quoteIdentifier(column.Name)
		if column.Name == geoparquet.GeometryColumnName {
			selections = append(selections, fmt.Sprintf("ST_AsWKB(%s) AS %s", quotedName, quotedName))
			continue
		}
		selections = append(selections, quotedName)
	}
	return strings.Join(selections, ", ")
}

func coordinateSQL(axis string) string {
	geometry := quoteIdentifier(geoparquet.GeometryColumnName)
	return fmt.Sprintf("CASE WHEN %s IS NULL OR ST_GeometryType(%s) <> '%s' THEN %s ELSE ST_%s(%s) END", geometry, geometry, geometryPointType, geometryInvalidSQL, axis, geometry)
}

func latitudeSQL() string { return coordinateSQL("Y") }

func normalizedLongitudeSQL() string {
	longitude := coordinateSQL("X")
	return fmt.Sprintf("CASE WHEN (%s) = 180 THEN -180 ELSE (%s) END", longitude, longitude)
}

func exactMembershipSQL() string {
	return fmt.Sprintf("h3_cell_contains(%s, %s, CAST(? AS UBIGINT))", latitudeSQL(), coordinateSQL("X"))
}

func validateProjection(fields []string, schema catalog.Schema) ([]catalog.Column, string, error) {
	if schema.PrimaryGeometry != geoparquet.GeometryColumnName {
		return nil, "", fmt.Errorf("%w: primary geometry %q is unsupported", ErrInvalidProjection, schema.PrimaryGeometry)
	}

	schemaColumns := make(map[string]catalog.Column, len(schema.Columns))
	for _, column := range schema.Columns {
		if !isSimpleIdentifier(column.Name) {
			return nil, "", fmt.Errorf("%w: schema column %q is not a simple identifier", ErrInvalidProjection, column.Name)
		}
		if _, ok := schemaColumns[column.Name]; ok {
			return nil, "", fmt.Errorf("%w: schema column %q is repeated", ErrInvalidProjection, column.Name)
		}
		schemaColumns[column.Name] = column
	}
	for _, required := range []string{identifierID, geoparquet.GeometryColumnName} {
		if _, ok := schemaColumns[required]; !ok {
			return nil, "", fmt.Errorf("%w: schema does not contain required column %q", ErrInvalidProjection, required)
		}
	}

	if len(fields) == 0 {
		return nil, "", fmt.Errorf("%w: selected columns must not be empty", ErrInvalidProjection)
	}
	selected := make([]catalog.Column, 0, len(fields))
	quotedColumns := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if !isSimpleIdentifier(field) {
			return nil, "", fmt.Errorf("%w: selected column %q is not a simple identifier", ErrInvalidProjection, field)
		}
		if _, ok := seen[field]; ok {
			return nil, "", fmt.Errorf("%w: selected column %q is repeated", ErrInvalidProjection, field)
		}
		column, ok := schemaColumns[field]
		if !ok {
			return nil, "", fmt.Errorf("%w: selected column %q is not in the catalog schema", ErrInvalidProjection, field)
		}
		seen[field] = struct{}{}
		selected = append(selected, column)
		quotedColumns = append(quotedColumns, quoteIdentifier(field))
	}
	for _, required := range []string{identifierID, geoparquet.GeometryColumnName} {
		if _, ok := seen[required]; !ok {
			return nil, "", fmt.Errorf("%w: selected columns must include %q", ErrInvalidProjection, required)
		}
	}
	return selected, strings.Join(quotedColumns, ", "), nil
}

func isSimpleIdentifier(value string) bool {
	if value == "" || !isIdentifierStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !isIdentifierPart(value[index]) {
			return false
		}
	}
	return true
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9'
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func ValidateMinimum(query tilequery.Query, minResolution int) error {
	if query.Square != nil {
		minimum, err := tilequery.MinimumXYZZoom(minResolution)
		if err != nil {
			return err
		}
		if query.Square.Z < minimum {
			return &ZoomTooCoarseError{ActualZoom: query.Square.Z, MinZoom: minimum}
		}
		return nil
	}
	cell, err := ParseCanonicalCell(query.Cell)
	if err != nil {
		return err
	}
	if cell.Resolution() < minResolution {
		return &ResolutionTooCoarseError{ActualResolution: cell.Resolution(), MinResolution: minResolution}
	}
	return nil
}
