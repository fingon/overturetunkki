package worker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mstenber/overturetunkki/internal/catalog"
	"github.com/mstenber/overturetunkki/internal/geoparquet"
	"github.com/uber/h3-go/v4"
)

const identifierID = "id"

var (
	ErrInvalidCell       = errors.New("invalid H3 cell")
	ErrInvalidProjection = errors.New("invalid source projection")
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
