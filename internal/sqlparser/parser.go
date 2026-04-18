package sqlparser

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// Parse reads SQL and returns the parsed AST statement.
func Parse(sqlText string) (sqlparser.Statement, error) {
	raw := strings.TrimSpace(sqlText)
	raw = strings.TrimSuffix(raw, ";")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty SQL statement")
	}

	stmt, err := sqlparser.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return stmt, nil
}

// ExtractTables extracts table names from a SQL statement by simple string parsing.
// This is a fallback for cases where AST field access isn't available.
func ExtractTables(sqlText string) ([]string, error) {
	raw := strings.TrimSpace(sqlText)
	raw = strings.TrimSuffix(raw, ";")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty SQL statement")
	}

	seen := make(map[string]bool)
	var tables []string
	upper := strings.ToUpper(raw)

	// Simple keyword-based extraction
	if strings.HasPrefix(upper, "SELECT") {
		fromIdx := strings.Index(upper, " FROM ")
		if fromIdx > 0 {
			remainder := raw[fromIdx+6:]
			extractTableNames(&tables, seen, extractTableList(remainder))
		}
	} else if strings.HasPrefix(upper, "INSERT") {
		intoIdx := strings.Index(upper, " INTO ")
		if intoIdx >= 0 {
			remainder := raw[intoIdx+6:]
			tableName := extractFirstToken(remainder)
			addTable(&tables, seen, tableName)
		}
	} else if strings.HasPrefix(upper, "UPDATE") {
		remainder := raw[7:] // Skip "UPDATE "
		tableName := extractFirstToken(remainder)
		addTable(&tables, seen, tableName)
	} else if strings.HasPrefix(upper, "DELETE") {
		fromIdx := strings.Index(upper, " FROM ")
		if fromIdx >= 0 {
			remainder := raw[fromIdx+6:]
			tableName := extractFirstToken(remainder)
			addTable(&tables, seen, tableName)
		}
	} else if strings.HasPrefix(upper, "CREATE") || strings.HasPrefix(upper, "DROP") {
		// CREATE/DROP TABLE or INDEX
		for _, keyword := range []string{"TABLE ", "INDEX "} {
			idx := strings.Index(upper, keyword)
			if idx >= 0 {
				remainder := raw[idx+len(keyword):]
				tableName := extractFirstToken(remainder)
				addTable(&tables, seen, tableName)
				break
			}
		}
	}

	return tables, nil
}

// extractTableList extracts the table list portion from a FROM clause.
func extractTableList(remainder string) string {
	// Stop at WHERE, GROUP, ORDER, LIMIT, etc.
	endIdx := len(remainder)
	for _, keyword := range []string{" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING ", ";"} {
		idx := strings.Index(strings.ToUpper(remainder), keyword)
		if idx > 0 && idx < endIdx {
			endIdx = idx
		}
	}
	return strings.TrimSpace(remainder[:endIdx])
}

// extractFirstToken extracts the first SQL identifier token from a string.
func extractFirstToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	var token strings.Builder
	for i, r := range s {
		if i == 0 && (r == '`' || r == '"' || r == '[') {
			// Handle quoted identifiers
			quote := r
			for j := i + 1; j < len(s); j++ {
				if rune(s[j]) == quote {
					return s[i+1 : j]
				}
			}
			return s[i+1:]
		}

		// Stop at whitespace or special chars
		if r == ' ' || r == '\t' || r == '\n' || r == '(' || r == ')' || r == ',' || r == ';' {
			break
		}
		token.WriteRune(r)
	}

	return token.String()
}

// extractTableNames parses a comma-separated list of table names.
func extractTableNames(tables *[]string, seen map[string]bool, tableList string) {
	for _, part := range strings.Split(tableList, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Handle aliases: "table AS alias" or "table alias"
		tokens := strings.Fields(part)
		if len(tokens) > 0 {
			tableName := tokens[0]
			addTable(tables, seen, tableName)
		}
	}
}

// addTable adds a table name if not already seen.
func addTable(tables *[]string, seen map[string]bool, name string) {
	name = strings.Trim(name, "`\"[]")
	name = strings.TrimSpace(name)
	if name == "" || seen[name] {
		return
	}
	seen[name] = true
	*tables = append(*tables, name)
}


