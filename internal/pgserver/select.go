package pgserver

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

type Select struct {
	session *Session
}

func (s *Select) exec(txnID int64, parsedSql *ast.SelectStatement) ExecutionResult {
	res := ExecutionResult{}

	tableSchema, err := s.extractTable(parsedSql)
	if err != nil {
		res.Err = err
		return res
	}

	filter, err := s.parseWhereConditions(parsedSql.Where)
	if err != nil {
		res.Err = err
		return res
	}

	targetCols := s.resolveTargetColumns(tableSchema, parsedSql.Columns)
	engine, err := s.session.db.Scan(txnID, nil, nil, filter)
	if err != nil {
		res.Err = err
		return res
	}

	res.Messages = append(res.Messages, s.buildRowDescription(targetCols))

	// 2. Add DataRows
	count := 0
	for _, entry := range entries {
		rowMap, _ := decodeRow(entry.Value, tableSchema)
		if filter(rowMap) {
			res.Messages = append(res.Messages, s.buildDataRow(targetCols, rowMap))
			count++
		}
	}

	// 3. Add CommandComplete
	res.Messages = append(res.Messages, &pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("SELECT %d", count)),
	})

	return res
}

func (s *Select) extractTable(parsedSql *ast.SelectStatement) (*schema.TableSchema, error) {
	if len(parsedSql.From) == 0 {
		return nil, fmt.Errorf("no table specified in SELECT statement")
	}

	tableRef := parsedSql.From[0]
	tableName := tableRef.Name

	tableSchema, err := s.session.db.GetTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", tableName, err)
	}

	// make sure columns from the query are valid for this table
	for _, col := range parsedSql.Columns {
		columnString := col.TokenLiteral()
		if columnString == "*" {
			continue
		}
		if _, exists := tableSchema.Columns[columnString]; !exists {
			return nil, fmt.Errorf("column %q does not exist in table %q", col, tableName)
		}
	}

	return tableSchema, nil
}

func (s *Select) parseWhereConditions(expr ast.Expression) (RowFilter, error) {
	// If WHERE is empty, return a filter that allows everything
	if expr == nil {
		return func(map[string]string) bool { return true }, nil
	}

	switch node := expr.(type) {
	case *ast.BinaryExpression:
		// Handles: col = 'val', col != 5, etc.
		return s.handleBinaryExpression(node)

	case *ast.UnaryExpression:

		// Handles "NOT (condition)"
		subFilter, err := s.parseWhereConditions(node.Expr)
		if err != nil {
			return nil, err
		}
		return func(row map[string]string) bool {
			return !subFilter(row)
		}, nil

	case *ast.InExpression:
		return s.handleInExpression(node)

	case *ast.BetweenExpression:
		return s.handleBetweenExpression(node)

	default:
		return nil, fmt.Errorf("unsupported WHERE node type: %T", expr)
	}
}

func (s *Select) handleBinaryExpression(node *ast.BinaryExpression) (RowFilter, error) {
	op := strings.ToUpper(node.Operator)

	// Handle Logical Operators (Recursion)
	if op == "AND" || op == "OR" {
		left, err := s.parseWhereConditions(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := s.parseWhereConditions(node.Right)
		if err != nil {
			return nil, err
		}

		if op == "AND" {
			return func(row map[string]string) bool { return left(row) && right(row) }, nil
		}
		return func(row map[string]string) bool { return left(row) || right(row) }, nil
	}

	// Handle Comparison Operators (Leaf Nodes)
	// Expects: Identifier (column) Op LiteralValue (value)
	leftIdent, okL := node.Left.(*ast.Identifier)
	rightLit, okR := node.Right.(*ast.LiteralValue)

	if !okL || !okR {
		return nil, fmt.Errorf("complex comparisons (col vs col) not yet supported")
	}

	colName := leftIdent.Name
	val := fmt.Sprintf("%v", rightLit.Value)

	switch op {
	case "=":
		return func(row map[string]string) bool { return row[colName] == val }, nil
	case "!=", "<>":
		return func(row map[string]string) bool { return row[colName] != val }, nil
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}
}

func (s *Select) handleInExpression(node *ast.InExpression) (RowFilter, error) {
	ident, ok := node.Expr.(*ast.Identifier)
	if !ok {
		return nil, fmt.Errorf("IN expression requires a column identifier")
	}

	// Convert the AST list into a map for O(1) lookup
	valMap := make(map[string]struct{})
	for _, expr := range node.List {
		if lit, ok := expr.(*ast.LiteralValue); ok {
			valMap[fmt.Sprintf("%v", lit.Value)] = struct{}{}
		}
	}

	colName := ident.Name
	return func(row map[string]string) bool {
		_, found := valMap[row[colName]]
		if node.Not {
			return !found
		}
		return found
	}, nil
}

func (s *Select) handleBetweenExpression(node *ast.BetweenExpression) (RowFilter, error) {
	// 1. Identify the target column
	ident, ok := node.Expr.(*ast.Identifier)
	if !ok {
		return nil, fmt.Errorf("BETWEEN expression requires a column identifier")
	}

	// 2. Extract Lower and Upper bounds
	lowerLit, okL := node.Lower.(*ast.LiteralValue)
	upperLit, okU := node.Upper.(*ast.LiteralValue)

	if !okL || !okU {
		return nil, fmt.Errorf("BETWEEN bounds must be literal values (e.g., 10 AND 20)")
	}

	colName := ident.Name
	// Convert interface values to strings for simple comparison
	lowerVal := fmt.Sprintf("%v", lowerLit.Value)
	upperVal := fmt.Sprintf("%v", upperLit.Value)

	return func(row map[string]string) bool {
		val, exists := row[colName]
		if !exists {
			return false
		}

		inRange := val >= lowerVal && val <= upperVal

		if node.Not {
			return !inRange
		}
		return inRange
	}, nil
}

func (s *Select) resolveTargetColumns(ts *schema.TableSchema, astCols []ast.Expression) []string {
	// If SELECT *, return all columns from schema
	if len(astCols) == 0 {
		return ts.ColumnList
	}

	var resolved []string
	for _, colExpr := range astCols {
		literal := colExpr.TokenLiteral()
		if literal == "*" {
			return ts.ColumnList
		}
		resolved = append(resolved, literal)
	}
	return resolved
}
