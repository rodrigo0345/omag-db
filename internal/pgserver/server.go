package pgserver

import (
	"net"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/pkg/log"
)

type ExecutionResult struct {
	Messages []pgproto3.BackendMessage
	Err      error
}

type Session struct {
	conn     net.Conn
	be       *pgproto3.Backend
	db       database.Database
	analyzer *log.Analyzer // easy debugging for operations
}

func NewSession(conn net.Conn, db database.Database) *Session {

	chunkReader := pgproto3.NewChunkReader(conn)
	ioWriter := conn

	be := pgproto3.NewBackend(chunkReader, ioWriter)
	return &Session{
		conn:     conn,
		be:       be,
		db:       db,
		analyzer: log.NewAnalyzer(),
	}
}

func (s *Session) Run() error {
	for {
		msg, err := s.be.Receive()
		if err != nil {
			return err
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			s.handleQuery(m.String)
			break
		case *pgproto3.Terminate:
			return nil
		}
	}
}

func (s *Session) handleQuery(sqlQuery string) {
	stmts, err := gosqlx.Parse(sqlQuery)

	if err != nil {
		s.sendError(err)
		return
	}

	for _, stmt := range stmts.Statements {
		switch q := stmt.(type) {
		case *ast.SelectStatement:
			selectStmt := &Select{
				session: s,
			}
			selectStmt.exec(q)
		}
	}
}

func (s *Session) sendError(err error) {
	_ = s.be.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  err.Error(),
	})
}
