package pgserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	applog "github.com/rodrigo0345/omag/pkg/log"
)

const (
	pgTextOID uint32 = 25
)

var (
	reCreateTable  = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+([^(\s]+)\s*\((.*)\)$`)
	reDropTable    = regexp.MustCompile(`(?is)^DROP\s+TABLE\s+([^(\s;]+)$`)
	reCreateIndex  = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?INDEX\s+([^(\s]+)\s+ON\s+([^(\s]+)\s*\((.*)\)$`)
	reDropIndex    = regexp.MustCompile(`(?is)^DROP\s+INDEX\s+([^(\s;]+)$`)
	reInsert       = regexp.MustCompile(`(?is)^INSERT\s+INTO\s+([^(\s]+)\s*(?:\((.*)\))?\s+VALUES\s*\((.*)\)$`)
	reUpdate       = regexp.MustCompile(`(?is)^UPDATE\s+([^(\s]+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$`)
	reDelete       = regexp.MustCompile(`(?is)^DELETE\s+FROM\s+([^(\s;]+)(?:\s+WHERE\s+(.*))?$`)
	reSelect       = regexp.MustCompile(`(?is)^SELECT\s+(.*)\s+FROM\s+([^(\s]+)(?:\s+WHERE\s+(.*))?$`)
	reBegin        = regexp.MustCompile(`(?is)^BEGIN(?:\s+TRANSACTION)?$`)
	reCommit       = regexp.MustCompile(`(?is)^COMMIT$`)
	reRollback     = regexp.MustCompile(`(?is)^ROLLBACK$`)
	reSet          = regexp.MustCompile(`(?is)^SET\s+([^(\s=]+)\s*=\s*(.+)$`)
	reShow         = regexp.MustCompile(`(?is)^SHOW\s+([^(\s;]+)$`)
	reSimpleSelect = regexp.MustCompile(`(?is)^SELECT\s+(.+)$`)
)

type Server struct {
	db database.Database
}

type connSession struct {
	inTransaction bool
	txnID         int64
	txnTableName  string
	txnTableSchema *schema.TableSchema
	status        byte
}

type statementResult struct {
	messages []pgproto3.BackendMessage
	status   byte
}

func New(db database.Database) *Server {
	return &Server{db: db}
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	applog.Info("[PGSERVER] listening on %s", addr)

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			return err
		}
		applog.Debug("[PGSERVER] accepted connection remote=%s", conn.RemoteAddr().String())

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.serveConn(conn); err != nil {
				applog.Warn("[PGSERVER] connection closed with error remote=%s err=%v", conn.RemoteAddr().String(), err)
			} else {
				applog.Debug("[PGSERVER] connection closed remote=%s", conn.RemoteAddr().String())
			}
		}()
	}
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	go func() {
		defer close(errCh)
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				errCh <- err
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = s.serveConn(conn)
			}()
		}
	}()

	select {
	case err := <-errCh:
		wg.Wait()
		return err
	case <-ctx.Done():
		wg.Wait()
		return ctx.Err()
	}
}

func (s *Server) serveConn(conn net.Conn) error {
	defer func() { _ = conn.Close() }()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	sess := &connSession{status: 'I'}

	if err := s.handshake(backend, conn); err != nil {
		return err
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "unexpected EOF") {
				return nil
			}
			return err
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			applog.Debug("[PGSERVER] query remote=%s sql=%q", conn.RemoteAddr().String(), strings.TrimSpace(m.String))
			res, err := s.handleQuery(sess, m.String)
			if err != nil {
				applog.Debug("[PGSERVER] query error remote=%s err=%v", conn.RemoteAddr().String(), err)
				if sendErr := s.sendError(backend, sess.status, err); sendErr != nil {
					return sendErr
				}
				continue
			}
			for _, message := range res.messages {
				if err := backend.Send(message); err != nil {
					return err
				}
			}
			if res.status != 0 {
				sess.status = res.status
			}
			if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: sess.status}); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Sync:
			if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: sess.status}); err != nil {
				return err
			}
		default:
			applog.Debug("[PGSERVER] unsupported frontend message remote=%s type=%T", conn.RemoteAddr().String(), msg)
			if err := s.sendError(backend, sess.status, fmt.Errorf("unsupported frontend message %T", msg)); err != nil {
				return err
			}
		}
	}
}

func (s *Server) handshake(backend *pgproto3.Backend, conn net.Conn) error {
	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		applog.Debug("[PGSERVER] startup message remote=%s type=%T", conn.RemoteAddr().String(), msg)

		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return err
			}
			continue
		case *pgproto3.CancelRequest:
			return nil
		case *pgproto3.StartupMessage:
			break
		default:
			return fmt.Errorf("unsupported startup message %T", msg)
		}
		break
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
		&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
		&pgproto3.BackendKeyData{ProcessID: uint32(time.Now().Unix()), SecretKey: 1},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		if err := backend.Send(msg); err != nil {
			return err
		}
	}
	applog.Debug("[PGSERVER] handshake complete remote=%s", conn.RemoteAddr().String())

	return nil
}

func (s *Server) handleQuery(sess *connSession, sqlText string) (statementResult, error) {
	stmts := splitStatements(sqlText)
	result := statementResult{status: sess.status}

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		res, err := s.executeStatement(sess, stmt)
		if err != nil {
			return statementResult{}, err
		}
		result.messages = append(result.messages, res.messages...)
		if res.status != 0 {
			result.status = res.status
		}
	}

	return result, nil
}

func (s *Server) executeStatement(sess *connSession, stmt string) (statementResult, error) {
	switch {
	case reBegin.MatchString(stmt):
		if sess.inTransaction {
			return statementResult{}, fmt.Errorf("transaction already in progress")
		}
		sess.inTransaction = true
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")}}, status: 'T'}, nil
	case reCommit.MatchString(stmt):
		if !sess.inTransaction {
			return statementResult{}, fmt.Errorf("no transaction is in progress")
		}
		if sess.txnID != 0 {
			if err := s.db.Commit(sess.txnID); err != nil {
				return statementResult{}, err
			}
		}
		sess.inTransaction = false
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")}}, status: 'I'}, nil
	case reRollback.MatchString(stmt):
		if !sess.inTransaction {
			return statementResult{}, fmt.Errorf("no transaction is in progress")
		}
		if sess.txnID != 0 {
			if err := s.db.Abort(sess.txnID); err != nil {
				return statementResult{}, err
			}
		}
		sess.inTransaction = false
		sess.txnID = 0
		sess.txnTableName = ""
		sess.txnTableSchema = nil
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")}}, status: 'I'}, nil
	case reSet.MatchString(stmt):
		return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("SET")}}}, nil
	case reShow.MatchString(stmt):
		return s.execShow(stmt)
	case reCreateTable.MatchString(stmt):
		return s.execCreateTable(stmt)
	case reDropTable.MatchString(stmt):
		return s.execDropTable(stmt)
	case reCreateIndex.MatchString(stmt):
		return s.execCreateIndex(stmt)
	case reDropIndex.MatchString(stmt):
		return s.execDropIndex(stmt)
	case reInsert.MatchString(stmt):
		return s.execInsert(sess, stmt)
	case reUpdate.MatchString(stmt):
		return s.execUpdate(sess, stmt)
	case reDelete.MatchString(stmt):
		return s.execDelete(sess, stmt)
	case reSelect.MatchString(stmt):
		return s.execSelect(stmt)
	case reSimpleSelect.MatchString(stmt):
		return s.execLiteralSelect(stmt)
	default:
		return statementResult{}, fmt.Errorf("unsupported SQL statement: %s", stmt)
	}
}

func (s *Server) beginMutationTxn(sess *connSession, tableName string, tableSchema *schema.TableSchema) (txnID int64, started bool, err error) {
	if sess == nil {
		return 0, false, fmt.Errorf("session is nil")
	}

	if sess.txnID != 0 {
		if sess.txnTableName != "" && tableName != "" && sess.txnTableName != tableName {
			return 0, false, fmt.Errorf("transaction already bound to table %q", sess.txnTableName)
		}
		if sess.txnTableName == "" && tableName != "" {
			sess.txnTableName = tableName
			sess.txnTableSchema = tableSchema
		}
		return sess.txnID, false, nil
	}

	txnID = s.db.BeginTransaction(txn_unit.READ_COMMITTED, tableName, tableSchema)
	if txnID <= 0 {
		return 0, false, fmt.Errorf("failed to begin transaction")
	}

	started = true
	if sess.inTransaction {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	return txnID, started, nil
}

func (s *Server) execCreateTable(stmt string) (statementResult, error) {
	m := reCreateTable.FindStringSubmatch(stmt)
	if len(m) != 3 {
		return statementResult{}, fmt.Errorf("invalid CREATE TABLE statement")
	}

	tableName := normalizeIdent(m[1])
	defs := splitTopLevel(m[2], ',')
	if len(defs) == 0 {
		return statementResult{}, fmt.Errorf("CREATE TABLE requires at least one column")
	}

	var primaryKey string
	tableSchema := schema.NewTableSchema(tableName, "")
	for _, def := range defs {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}

		upper := strings.ToUpper(def)
		if strings.HasPrefix(upper, "PRIMARY KEY") {
			cols, err := parseParenList(def)
			if err != nil {
				return statementResult{}, err
			}
			if len(cols) != 1 {
				return statementResult{}, fmt.Errorf("PRIMARY KEY must specify exactly one column")
			}
			primaryKey = normalizeIdent(cols[0])
			continue
		}

		parts := strings.Fields(def)
		if len(parts) < 2 {
			return statementResult{}, fmt.Errorf("invalid column definition %q", def)
		}

		colName := normalizeIdent(parts[0])
		dataType := parseDataType(parts[1])
		nullable := !strings.Contains(upper, "NOT NULL")
		if err := tableSchema.AddColumn(colName, dataType, nullable); err != nil {
			return statementResult{}, err
		}
		if strings.Contains(upper, "PRIMARY KEY") {
			primaryKey = colName
		}
	}

	if primaryKey == "" {
		return statementResult{}, fmt.Errorf("CREATE TABLE requires a primary key")
	}
	// Rebuild schema with the actual primary key value.
	tableSchema.PrimaryKey = primaryKey
	if pkCol, ok := tableSchema.Columns[primaryKey]; ok {
		pkCol.Nullable = false
	}
	if err := tableSchema.Validate(); err != nil {
		return statementResult{}, err
	}

	if err := s.db.CreateTable(tableSchema); err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("CREATE TABLE")}}}, nil
}

func (s *Server) execDropTable(stmt string) (statementResult, error) {
	m := reDropTable.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid DROP TABLE statement")
	}

	if err := s.db.DropTable(normalizeIdent(m[1])); err != nil {
		return statementResult{}, err
	}
	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP TABLE")}}}, nil
}

func (s *Server) execCreateIndex(stmt string) (statementResult, error) {
	m := reCreateIndex.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid CREATE INDEX statement")
	}

	isUnique := strings.TrimSpace(strings.ToUpper(m[1])) != ""
	indexName := normalizeIdent(m[2])
	tableName := normalizeIdent(m[3])
	cols, err := parseIdentList(m[4])
	if err != nil {
		return statementResult{}, err
	}
	if len(cols) != 1 {
		return statementResult{}, fmt.Errorf("only single-column indexes are supported")
	}

	indexType := schema.IndexTypeSecondary
	if isUnique {
		indexType = schema.IndexTypeUnique
	}
	if err := s.db.CreateIndex(tableName, indexName, indexType, cols, isUnique); err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("CREATE INDEX")}}, status: 'I'}, nil
}

func (s *Server) execDropIndex(stmt string) (statementResult, error) {
	m := reDropIndex.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid DROP INDEX statement")
	}

	indexName := normalizeIdent(m[1])
	tables := s.db.SchemaManager().ListTables()
	for _, tableName := range tables {
		if ts, err := s.db.GetTableSchema(tableName); err == nil {
			if _, exists := ts.Indexes[indexName]; exists {
				if err := s.db.DropIndex(tableName, indexName); err != nil {
					return statementResult{}, err
				}
				return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DROP INDEX")}}}, nil
			}
		}
	}
	return statementResult{}, fmt.Errorf("index %q not found", indexName)
}

func (s *Server) execInsert(sess *connSession, stmt string) (statementResult, error) {
	m := reInsert.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid INSERT statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.db.GetTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	cols := tableSchema.ColumnList
	if strings.TrimSpace(m[2]) != "" {
		cols, err = parseIdentList(m[2])
		if err != nil {
			return statementResult{}, err
		}
	}

	values, err := parseValueList(m[3])
	if err != nil {
		return statementResult{}, err
	}
	if len(cols) != len(values) {
		return statementResult{}, fmt.Errorf("column/value count mismatch")
	}

	row := make(map[string]string, len(cols))
	for i, col := range cols {
		row[normalizeIdent(col)] = values[i]
	}
	if _, ok := row[tableSchema.PrimaryKey]; !ok {
		return statementResult{}, fmt.Errorf("primary key %q must be included", tableSchema.PrimaryKey)
	}
	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return statementResult{}, fmt.Errorf("table %q not found", tableName)
	}
	if _, err := engine.Get([]byte(row[tableSchema.PrimaryKey])); err == nil {
		return statementResult{}, fmt.Errorf("duplicate primary key value %q", row[tableSchema.PrimaryKey])
	}

	if err := s.writeRow(sess, tableName, tableSchema, row); err != nil {
		return statementResult{}, err
	}
	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")}}}, nil
}

func (s *Server) execUpdate(sess *connSession, stmt string) (statementResult, error) {
	m := reUpdate.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid UPDATE statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.db.GetTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	setMap, err := parseAssignments(m[2])
	if err != nil {
		return statementResult{}, err
	}
	whereMap, err := parseWhereClause(m[3])
	if err != nil {
		return statementResult{}, err
	}

	count, err := s.updateRows(sess, tableName, tableSchema, setMap, whereMap)
	if err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("UPDATE %d", count))}}}, nil
}

func (s *Server) execDelete(sess *connSession, stmt string) (statementResult, error) {
	m := reDelete.FindStringSubmatch(stmt)
	if len(m) != 3 {
		return statementResult{}, fmt.Errorf("invalid DELETE statement")
	}

	tableName := normalizeIdent(m[1])
	tableSchema, err := s.db.GetTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	whereMap := map[string]string{}
	if strings.TrimSpace(m[2]) != "" {
		whereMap, err = parseWhereClause(m[2])
		if err != nil {
			return statementResult{}, err
		}
	}

	count, err := s.deleteRows(sess, tableName, tableSchema, whereMap)
	if err != nil {
		return statementResult{}, err
	}

	return statementResult{messages: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("DELETE %d", count))}}}, nil
}

func (s *Server) execSelect(stmt string) (statementResult, error) {
	m := reSelect.FindStringSubmatch(stmt)
	if len(m) != 4 {
		return statementResult{}, fmt.Errorf("invalid SELECT statement")
	}

	columnExpr := strings.TrimSpace(m[1])
	tableName := normalizeIdent(m[2])
	tableSchema, err := s.db.GetTableSchema(tableName)
	if err != nil {
		return statementResult{}, err
	}

	cols, err := s.resolveSelectColumns(tableSchema, columnExpr)
	if err != nil {
		return statementResult{}, err
	}

	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return statementResult{}, err
	}

	if strings.TrimSpace(m[3]) != "" {
		whereMap, err := parseWhereClause(m[3])
		if err != nil {
			return statementResult{}, err
		}
		rows = filterRows(rows, whereMap)
	}

	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, col := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(col),
			DataTypeOID:  pgTextOID,
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       pgproto3.TextFormat,
		}
	}

	messages := []pgproto3.BackendMessage{&pgproto3.RowDescription{Fields: fields}}
	for _, row := range rows {
		values := make([][]byte, len(cols))
		for i, col := range cols {
			values[i] = []byte(row[col])
		}
		messages = append(messages, &pgproto3.DataRow{Values: values})
	}
	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
	return statementResult{messages: messages}, nil
}

func (s *Server) execLiteralSelect(stmt string) (statementResult, error) {
	// Support simple expressions like `SELECT 1` or `SELECT 'ok'`.
	m := reSimpleSelect.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid SELECT statement")
	}

	expr := strings.TrimSpace(m[1])
	if strings.Contains(strings.ToUpper(expr), " FROM ") {
		return statementResult{}, fmt.Errorf("unsupported SELECT form")
	}

	value, err := parseSQLValue(expr)
	if err != nil {
		return statementResult{}, err
	}

	messages := []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("?column?"), DataTypeOID: pgTextOID, DataTypeSize: -1, TypeModifier: -1, Format: pgproto3.TextFormat}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(value)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
	}
	return statementResult{messages: messages}, nil
}

func (s *Server) execShow(stmt string) (statementResult, error) {
	m := reShow.FindStringSubmatch(stmt)
	if len(m) != 2 {
		return statementResult{}, fmt.Errorf("invalid SHOW statement")
	}

	name := normalizeIdent(m[1])
	value := map[string]string{
		"client_encoding":             "UTF8",
		"server_encoding":             "UTF8",
		"server_version":              "15.0",
		"standard_conforming_strings": "on",
		"application_name":            "omag",
		"search_path":                 "public",
		"transaction_isolation":       "read committed",
		"transaction_read_only":       "off",
		"datestyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
	}[name]
	if value == "" {
		value = ""
	}

	messages := []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte(name), DataTypeOID: pgTextOID, DataTypeSize: -1, TypeModifier: -1, Format: pgproto3.TextFormat}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(value)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
	}
	return statementResult{messages: messages}, nil
}

func (s *Server) resolveSelectColumns(tableSchema *schema.TableSchema, expr string) ([]string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "*" {
		return append([]string(nil), tableSchema.ColumnList...), nil
	}
	parts, err := parseIdentList(expr)
	if err != nil {
		return nil, err
	}
	for _, col := range parts {
		if _, ok := tableSchema.Columns[normalizeIdent(col)]; !ok {
			return nil, fmt.Errorf("column %q does not exist", col)
		}
	}
	return normalizeIdents(parts), nil
}

func (s *Server) readRows(tableName string, tableSchema *schema.TableSchema) ([]map[string]string, error) {
	engine := s.db.TableStorageEngine(tableName)
	if engine == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	entries, err := engine.Scan(nil, nil)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]string, 0, len(entries))
	for _, entry := range entries {
		row, err := decodeRow(entry.Value)
		if err != nil {
			return nil, err
		}
		if _, ok := row[tableSchema.PrimaryKey]; !ok {
			row[tableSchema.PrimaryKey] = string(entry.Key)
		}
		rows = append(rows, row)
	}
	pkType := schema.DataTypeString
	if colType, err := tableSchema.ColumnDataType(tableSchema.PrimaryKey); err == nil {
		pkType = colType
	}
	sort.Slice(rows, func(i, j int) bool {
		return compareRowValues(rows[i][tableSchema.PrimaryKey], rows[j][tableSchema.PrimaryKey], pkType) < 0
	})
	return rows, nil
}

func (s *Server) writeRow(sess *connSession, tableName string, tableSchema *schema.TableSchema, row map[string]string) (err error) {
	if tableSchema == nil {
		return fmt.Errorf("table schema is nil")
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	payload, marshalErr := json.Marshal(row)
	if marshalErr != nil {
		return marshalErr
	}
	if err = s.db.Write(txnID, []byte(row[tableSchema.PrimaryKey]), payload); err != nil {
		return err
	}
	return nil
}

func (s *Server) updateRows(sess *connSession, tableName string, tableSchema *schema.TableSchema, setMap, whereMap map[string]string) (count int, err error) {
	if _, updatesPrimaryKey := setMap[tableSchema.PrimaryKey]; updatesPrimaryKey {
		return 0, fmt.Errorf("updating primary key %q is not supported", tableSchema.PrimaryKey)
	}

	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return 0, err
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()

	for _, row := range rows {
		if !rowMatches(row, whereMap) {
			continue
		}
		for col, val := range setMap {
			if _, ok := tableSchema.Columns[col]; !ok {
				return 0, fmt.Errorf("column %q does not exist", col)
			}
			row[col] = val
		}
		payload, marshalErr := json.Marshal(row)
		if marshalErr != nil {
			return 0, marshalErr
		}
		if err = s.db.Write(txnID, []byte(row[tableSchema.PrimaryKey]), payload); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

func (s *Server) deleteRows(sess *connSession, tableName string, tableSchema *schema.TableSchema, whereMap map[string]string) (count int, err error) {
	rows, err := s.readRows(tableName, tableSchema)
	if err != nil {
		return 0, err
	}

	txnID, started, err := s.beginMutationTxn(sess, tableName, tableSchema)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			if abortErr := s.db.Abort(txnID); abortErr != nil {
				err = errors.Join(err, abortErr)
			}
			if sess != nil && sess.inTransaction {
				sess.inTransaction = false
				sess.txnID = 0
				sess.txnTableName = ""
				sess.txnTableSchema = nil
			}
			return
		}

		if started && (sess == nil || !sess.inTransaction) {
			if commitErr := s.db.Commit(txnID); commitErr != nil {
				if abortErr := s.db.Abort(txnID); abortErr != nil {
					err = errors.Join(commitErr, abortErr)
					return
				}
				err = commitErr
			}
		}
	}()
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}

	for _, row := range rows {
		if !rowMatches(row, whereMap) {
			continue
		}
		if err = s.db.Delete(txnID, []byte(row[tableSchema.PrimaryKey])); err != nil {
			return 0, err
		}
		count++
	}
	if sess != nil && sess.inTransaction && sess.txnID == 0 {
		sess.txnID = txnID
		sess.txnTableName = tableName
		sess.txnTableSchema = tableSchema
	}
	return count, nil
}

func (s *Server) sendError(backend *pgproto3.Backend, status byte, err error) error {
	if sendErr := backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: err.Error()}); sendErr != nil {
		return sendErr
	}
	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func splitStatements(sqlText string) []string {
	var parts []string
	var buf strings.Builder
	var inSingle, inDouble bool

	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					buf.WriteByte(sqlText[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(sqlText) && sqlText[i+1] == '"' {
					buf.WriteByte(sqlText[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		case ';':
			if inSingle || inDouble {
				buf.WriteByte(c)
				continue
			}
			part := strings.TrimSpace(buf.String())
			if part != "" {
				parts = append(parts, part)
			}
			buf.Reset()
			continue
		}
		buf.WriteByte(c)
	}

	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func splitTopLevel(s string, sep rune) []string {
	var parts []string
	var buf strings.Builder
	depth := 0
	var inSingle, inDouble bool
	sepByte := byte(sep)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(s) && s[i+1] == '\'' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(s) && s[i+1] == '"' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		default:
			if c == sepByte && !inSingle && !inDouble && depth == 0 {
				part := strings.TrimSpace(buf.String())
				if part != "" {
					parts = append(parts, part)
				}
				buf.Reset()
				continue
			}
		}
		buf.WriteByte(c)
	}
	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseParenList(s string) ([]string, error) {
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid parenthesized list: %s", s)
	}
	inner := s[start+1 : end]
	return parseIdentList(inner)
}

func parseIdentList(s string) ([]string, error) {
	parts := splitTopLevel(s, ',')
	idents := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idents = append(idents, normalizeIdent(part))
	}
	return idents, nil
}

func normalizeIdents(idents []string) []string {
	out := make([]string, len(idents))
	for i, ident := range idents {
		out[i] = normalizeIdent(ident)
	}
	return out
}

func parseAssignments(s string) (map[string]string, error) {
	assignments := make(map[string]string)
	for _, part := range splitTopLevel(s, ',') {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid assignment %q", part)
		}
		key := normalizeIdent(kv[0])
		val, err := parseSQLValue(kv[1])
		if err != nil {
			return nil, err
		}
		assignments[key] = val
	}
	return assignments, nil
}

func parseWhereClause(s string) (map[string]string, error) {
	conds := make(map[string]string)
	for _, part := range splitOnAnd(s) {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("unsupported WHERE clause %q", part)
		}
		key := normalizeIdent(kv[0])
		val, err := parseSQLValue(kv[1])
		if err != nil {
			return nil, err
		}
		conds[key] = val
	}
	return conds, nil
}

func splitOnAnd(s string) []string {
	var parts []string
	var buf strings.Builder
	var inSingle, inDouble bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			if !inDouble {
				buf.WriteByte(c)
				if inSingle && i+1 < len(s) && s[i+1] == '\'' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				buf.WriteByte(c)
				if inDouble && i+1 < len(s) && s[i+1] == '"' {
					buf.WriteByte(s[i+1])
					i++
					continue
				}
				inDouble = !inDouble
				continue
			}
		}

		if !inSingle && !inDouble && i+3 <= len(s) && strings.EqualFold(s[i:i+3], "AND") {
			prevOK := i == 0 || isWhitespaceOrParen(s[i-1])
			nextOK := i+3 == len(s) || isWhitespaceOrParen(s[i+3])
			if prevOK && nextOK {
				part := strings.TrimSpace(buf.String())
				if part != "" {
					parts = append(parts, part)
				}
				buf.Reset()
				i += 2
				continue
			}
		}

		buf.WriteByte(c)
	}
	if part := strings.TrimSpace(buf.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func isWhitespaceOrParen(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '(', ')':
		return true
	default:
		return false
	}
}

func rowMatches(row map[string]string, whereMap map[string]string) bool {
	for col, val := range whereMap {
		if row[col] != val {
			return false
		}
	}
	return true
}

func filterRows(rows []map[string]string, whereMap map[string]string) []map[string]string {
	if len(whereMap) == 0 {
		return rows
	}
	filtered := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		if rowMatches(row, whereMap) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func parseSQLValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "NULL" || raw == "null" {
		return "", nil
	}
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return unescapeSQLString(raw[1 : len(raw)-1]), nil
		}
	}
	return raw, nil
}

func parseValueList(s string) ([]string, error) {
	parts := splitTopLevel(s, ',')
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		val, err := parseSQLValue(part)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func unescapeSQLString(s string) string {
	s = strings.ReplaceAll(s, "''", "'")
	s = strings.ReplaceAll(s, `""`, `"`)
	return s
}

func compareRowValues(a, b string, dataType schema.DataType) int {
	switch dataType {
	case schema.DataTypeInt64:
		if ai, err := strconv.ParseInt(strings.TrimSpace(a), 10, 64); err == nil {
			if bi, err := strconv.ParseInt(strings.TrimSpace(b), 10, 64); err == nil {
				switch {
				case ai < bi:
					return -1
				case ai > bi:
					return 1
				default:
					return 0
				}
			}
		}
	case schema.DataTypeFloat64:
		if af, err := strconv.ParseFloat(strings.TrimSpace(a), 64); err == nil {
			if bf, err := strconv.ParseFloat(strings.TrimSpace(b), 64); err == nil {
				switch {
				case af < bf:
					return -1
				case af > bf:
					return 1
				default:
					return 0
				}
			}
		}
	case schema.DataTypeBool:
		if ab, err := strconv.ParseBool(strings.TrimSpace(a)); err == nil {
			if bb, err := strconv.ParseBool(strings.TrimSpace(b)); err == nil {
				switch {
				case !ab && bb:
					return -1
				case ab && !bb:
					return 1
				default:
					return 0
				}
			}
		}
	}
	return strings.Compare(a, b)
}

func normalizeIdent(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "`\"[]")
	return strings.ToLower(s)
}

func parseDataType(token string) schema.DataType {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "INT", "INT4", "INTEGER", "BIGINT", "SMALLINT", "SERIAL":
		return schema.DataTypeInt64
	case "BOOL", "BOOLEAN":
		return schema.DataTypeBool
	case "FLOAT", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL":
		return schema.DataTypeFloat64
	case "BYTEA":
		return schema.DataTypeBytes
	default:
		return schema.DataTypeString
	}
}


func decodeRow(data []byte) (map[string]string, error) {
	var row map[string]string
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	if row == nil {
		row = make(map[string]string)
	}
	return row, nil
}

