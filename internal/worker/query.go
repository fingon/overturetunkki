package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/fingon/overturetunkki/internal/catalog"
	"github.com/fingon/overturetunkki/internal/geoparquet"
	"github.com/fingon/overturetunkki/internal/h3filter"
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
	ErrInvalidCell       = errors.New("invalid H3 cell")
	ErrInvalidProjection = errors.New("invalid source projection")
	ErrTooManyRows       = errors.New("candidate rows exceed row limit")
)

type TileRequest struct {
	Cell string
}

type QueryPlan struct {
	Cell          h3.Cell
	CellText      string
	CellValue     uint64
	Columns       []catalog.Column
	ProjectionSQL string
}

type PreparedQuery struct {
	SQL  string
	Args []any
}

type TileBuildRequest struct {
	Conn       *sql.Conn
	Plan       QueryPlan
	Snapshot   catalog.Snapshot
	MaxRows    int64
	OutputPath string
	MaxBytes   int64
}

type candidateCopyRequest struct {
	Context    context.Context
	Conn       *sql.Conn
	OutputPath string
	Metadata   []byte
	MaxBytes   int64
}

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
	if !plan.Cell.IsValid() {
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
	assetURLs, err := pinnedAssetURLs(snapshot)
	if err != nil {
		return PreparedQuery{}, err
	}
	bounds, err := h3filter.CellBounds(plan.Cell)
	if err != nil {
		return PreparedQuery{}, fmt.Errorf("build candidate query bounds: %w", err)
	}
	bboxSQL, bboxArgs := bounds.DuckDBBBoxPredicate()
	projectionSQL := candidateProjectionSQL(plan.Columns)
	placeholders := make([]string, len(assetURLs))
	args := make([]any, 0, len(assetURLs)+len(bboxArgs)+2)
	for index, assetURL := range assetURLs {
		placeholders[index] = "?"
		args = append(args, assetURL)
	}
	args = append(args, bboxArgs...)
	args = append(args, plan.CellValue, rowLimit)
	return PreparedQuery{
		SQL: fmt.Sprintf(
			"SELECT %s FROM read_parquet([%s]) WHERE %s AND %s LIMIT ?",
			projectionSQL,
			strings.Join(placeholders, ", "),
			bboxSQL,
			exactMembershipSQL(),
		),
		Args: args,
	}, nil
}

func BuildTile(ctx context.Context, request TileBuildRequest) (CopyResult, error) {
	if ctx == nil {
		return CopyResult{}, errors.New("build tile: context is nil")
	}
	if request.Conn == nil {
		return CopyResult{}, errors.New("build tile: DuckDB connection is nil")
	}
	query, err := BuildCandidateQuery(request.Plan, request.Snapshot, request.MaxRows)
	if err != nil {
		return CopyResult{}, err
	}
	metadata, err := candidateMetadata(request.Plan)
	if err != nil {
		return CopyResult{}, err
	}
	candidates, err := materializeCandidates(ctx, request.Conn, query, request.MaxRows)
	if err != nil {
		return CopyResult{}, err
	}
	copyResult, copyErr := candidates.copy(candidateCopyRequest{
		Context:    ctx,
		Conn:       request.Conn,
		OutputPath: request.OutputPath,
		Metadata:   metadata,
		MaxBytes:   request.MaxBytes,
	})
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
	if _, err := conn.ExecContext(ctx, createSQL, query.Args...); err != nil {
		return candidateRows{}, fmt.Errorf("materialize candidates: %w", err)
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
	copySQL, err := geoparquet.CopySQL("SELECT * FROM "+candidateTableName, request.OutputPath, request.Metadata)
	if err != nil {
		return CopyResult{}, fmt.Errorf("copy candidates: %w", err)
	}
	return CopyWithOutputLimit(request.Context, CopyRequest{Conn: request.Conn, Query: copySQL, OutputPath: request.OutputPath, MaxBytes: request.MaxBytes})
}

func (rows candidateRows) drop(ctx context.Context, conn *sql.Conn) error {
	return dropCandidateTable(ctx, conn)
}

func candidateMetadata(plan QueryPlan) ([]byte, error) {
	bounds, err := h3filter.CellBounds(plan.Cell)
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
	cell, err := ParseCanonicalCell(request.Cell)
	if err != nil {
		return QueryPlan{}, err
	}
	columns, projectionSQL, err := validateProjection(fields, schema)
	if err != nil {
		return QueryPlan{}, err
	}
	return QueryPlan{
		Cell:          cell,
		CellText:      cell.String(),
		CellValue:     uint64(cell),
		Columns:       columns,
		ProjectionSQL: projectionSQL,
	}, nil
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

func pinnedAssetURLs(snapshot catalog.Snapshot) ([]string, error) {
	if len(snapshot.Manifest) == 0 {
		return nil, errors.New("build candidate query: catalog manifest is empty")
	}
	assetHost := snapshot.AssetHost
	if assetHost == "" {
		assetHost = catalog.DefaultAssetHost
	}
	prefix := fmt.Sprintf("/release/%s/theme=%s/type=%s/", snapshot.Release, placesTheme, placesType)
	assetURLs := make([]string, 0, len(snapshot.Manifest))
	seen := make(map[string]struct{}, len(snapshot.Manifest))
	for index, asset := range snapshot.Manifest {
		parsed, err := url.Parse(asset.Href)
		if err != nil {
			return nil, fmt.Errorf("validate manifest asset %d: %w", index, err)
		}
		if parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, assetHost) || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("validate manifest asset %d: URL is not a trusted HTTPS asset", index)
		}
		assetName := path.Base(parsed.Path)
		if path.Clean(parsed.Path) != parsed.Path || !strings.HasPrefix(parsed.Path, prefix) || !strings.HasPrefix(assetName, "part-"+asset.PartitionID+"-") || !strings.HasSuffix(assetName, assetPathSuffix) {
			return nil, fmt.Errorf("validate manifest asset %d: URL is outside the trusted places prefix", index)
		}
		if asset.PartitionID == "" || asset.SizeBytes <= 0 || asset.RowCount <= 0 || asset.RowGroupCount <= 0 {
			return nil, fmt.Errorf("validate manifest asset %d: metadata is invalid", index)
		}
		if _, ok := seen[asset.Href]; ok {
			return nil, fmt.Errorf("validate manifest asset %d: URL is repeated", index)
		}
		seen[asset.Href] = struct{}{}
		assetURLs = append(assetURLs, asset.Href)
	}
	return assetURLs, nil
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

func exactMembershipSQL() string {
	geometry := quoteIdentifier(geoparquet.GeometryColumnName)
	latitude := fmt.Sprintf(
		"CASE WHEN %s IS NULL OR ST_GeometryType(%s) <> '%s' THEN %s ELSE ST_Y(%s) END",
		geometry,
		geometry,
		geometryPointType,
		geometryInvalidSQL,
		geometry,
	)
	longitude := fmt.Sprintf(
		"CASE WHEN %s IS NULL OR ST_GeometryType(%s) <> '%s' THEN %s ELSE ST_X(%s) END",
		geometry,
		geometry,
		geometryPointType,
		geometryInvalidSQL,
		geometry,
	)
	return fmt.Sprintf(
		"h3_cell_contains(%s, %s, CAST(? AS UBIGINT))",
		latitude,
		longitude,
	)
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
