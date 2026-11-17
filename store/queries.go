package store

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/random"
)

// QueriesTableName is the name of the SQLite table which backs the registered
// query API. Registered queries are held in SQLite, rather than in memory, so
// that registering and deregistering a query is written through Raft like any
// other change to the database. Registrations therefore reach every node and
// are captured by snapshots.
const QueriesTableName = "__rqlite_queries"

// SQL statements used by the registered query API. Exported so callers can
// reference them by name rather than duplicating string literals.
const (
	QueriesCreateTableSQL = `CREATE TABLE IF NOT EXISTS __rqlite_queries (
		id TEXT PRIMARY KEY NOT NULL,
		sql TEXT NOT NULL
	) WITHOUT ROWID`

	QueriesInsertSQL = `INSERT INTO __rqlite_queries (id, sql) VALUES (?, ?)`

	QueriesSelectSQL = `SELECT id, sql FROM __rqlite_queries WHERE id = ?`

	QueriesSelectAllSQL = `SELECT id, sql FROM __rqlite_queries ORDER BY id`

	QueriesDeleteSQL = `DELETE FROM __rqlite_queries WHERE id = ?`
)

// RegisteredQuery is a query held in the registry, together with the ID under
// which it was registered.
type RegisteredQuery struct {
	ID  string `json:"id"`
	SQL string `json:"sql"`
}

// NewRegisteredQueryID returns a new, randomly-generated ID for a registered
// query.
func NewRegisteredQueryID() string {
	b := random.Bytes(16)
	// Convert the random bytes into the format of a UUID.
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// RegisterQueryStatements returns the statements which add queries to the
// registry, creating the registry table if it does not already exist. The
// statements must be executed through Raft, so that every node learns of the
// registrations.
func RegisterQueryStatements(queries []RegisteredQuery) []*proto.Statement {
	stmts := make([]*proto.Statement, 0, len(queries)+1)
	stmts = append(stmts, &proto.Statement{Sql: QueriesCreateTableSQL})
	for _, q := range queries {
		stmts = append(stmts, &proto.Statement{
			Sql: QueriesInsertSQL,
			Parameters: []*proto.Parameter{
				{Value: &proto.Parameter_S{S: q.ID}},
				{Value: &proto.Parameter_S{S: q.SQL}},
			},
		})
	}
	return stmts
}

// DeregisterQueryStatement returns the statement which removes the query
// registered under id from the registry. The statement must be executed
// through Raft.
func DeregisterQueryStatement(id string) *proto.Statement {
	return &proto.Statement{
		Sql: QueriesDeleteSQL,
		Parameters: []*proto.Parameter{
			{Value: &proto.Parameter_S{S: id}},
		},
	}
}

// RegisteredQueryStatement returns the statement which reads the query
// registered under id. The statement is read-only.
func RegisteredQueryStatement(id string) *proto.Statement {
	return &proto.Statement{
		Sql: QueriesSelectSQL,
		Parameters: []*proto.Parameter{
			{Value: &proto.Parameter_S{S: id}},
		},
	}
}

// RegisteredQueriesStatement returns the statement which reads every query in
// the registry. The statement is read-only.
func RegisteredQueriesStatement() *proto.Statement {
	return &proto.Statement{Sql: QueriesSelectAllSQL}
}

// RegisteredQueriesFromRows converts the rows returned by a registry read into
// the registered queries they describe. A registry which has never been
// written to has no table, which reads as an empty registry rather than an
// error.
func RegisteredQueriesFromRows(rows []*proto.QueryRows) ([]RegisteredQuery, error) {
	queries := make([]RegisteredQuery, 0)
	if len(rows) == 0 {
		return queries, nil
	}
	if rows[0].Error != "" {
		if strings.Contains(rows[0].Error, "no such table") {
			return queries, nil
		}
		return nil, errors.New(rows[0].Error)
	}
	for _, v := range rows[0].Values {
		if len(v.Parameters) != 2 {
			return nil, fmt.Errorf("registered query row has %d columns, want 2", len(v.Parameters))
		}
		queries = append(queries, RegisteredQuery{
			ID:  v.Parameters[0].GetS(),
			SQL: v.Parameters[1].GetS(),
		})
	}
	return queries, nil
}
