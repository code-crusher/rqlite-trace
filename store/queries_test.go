package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Test_NewRegisteredQueryID tests that generated IDs are unique and formatted
// like a UUID.
func Test_NewRegisteredQueryID(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		id := NewRegisteredQueryID()
		if seen[id] {
			t.Fatalf("duplicate registered query ID %s", id)
		}
		seen[id] = true

		parts := strings.Split(id, "-")
		if len(parts) != 5 {
			t.Fatalf("registered query ID %s is not UUID-shaped", id)
		}
		if exp := []int{8, 4, 4, 4, 12}; len(parts[4]) != exp[4] ||
			len(parts[0]) != exp[0] || len(parts[1]) != exp[1] ||
			len(parts[2]) != exp[2] || len(parts[3]) != exp[3] {
			t.Fatalf("registered query ID %s has wrong group lengths: %v", id, parts)
		}
	}
}

// Test_RegisterQueryStatements tests the statements built for the registry.
func Test_RegisterQueryStatements(t *testing.T) {
	stmts := RegisterQueryStatements([]RegisteredQuery{
		{ID: "id1", SQL: "SELECT * FROM foo"},
		{ID: "id2", SQL: "SELECT * FROM bar"},
	})
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}
	if stmts[0].Sql != QueriesCreateTableSQL {
		t.Fatalf("first statement should create the registry table, got %s", stmts[0].Sql)
	}
	for i, q := range []struct{ id, sql string }{
		{"id1", "SELECT * FROM foo"},
		{"id2", "SELECT * FROM bar"},
	} {
		stmt := stmts[i+1]
		if stmt.Sql != QueriesInsertSQL {
			t.Fatalf("statement %d should insert, got %s", i+1, stmt.Sql)
		}
		if len(stmt.Parameters) != 2 {
			t.Fatalf("statement %d should have 2 parameters, got %d", i+1, len(stmt.Parameters))
		}
		if got := stmt.Parameters[0].GetS(); got != q.id {
			t.Fatalf("statement %d has ID %s, want %s", i+1, got, q.id)
		}
		if got := stmt.Parameters[1].GetS(); got != q.sql {
			t.Fatalf("statement %d has SQL %s, want %s", i+1, got, q.sql)
		}
	}
}

// Test_RegisteredQueryStatement tests the statements built to read one query.
func Test_RegisteredQueryStatement(t *testing.T) {
	stmt := RegisteredQueryStatement("abc")
	if stmt.Sql != QueriesSelectSQL {
		t.Fatalf("unexpected SQL: %s", stmt.Sql)
	}
	if len(stmt.Parameters) != 1 || stmt.Parameters[0].GetS() != "abc" {
		t.Fatalf("unexpected parameters: %v", stmt.Parameters)
	}

	stmt = DeregisterQueryStatement("abc")
	if stmt.Sql != QueriesDeleteSQL {
		t.Fatalf("unexpected SQL: %s", stmt.Sql)
	}
	if len(stmt.Parameters) != 1 || stmt.Parameters[0].GetS() != "abc" {
		t.Fatalf("unexpected parameters: %v", stmt.Parameters)
	}

	if stmt := RegisteredQueriesStatement(); stmt.Sql != QueriesSelectAllSQL {
		t.Fatalf("unexpected SQL: %s", stmt.Sql)
	}
}

// Test_RegisteredQueriesFromRows tests conversion of registry rows.
func Test_RegisteredQueriesFromRows(t *testing.T) {
	if qs, err := RegisteredQueriesFromRows(nil); err != nil || len(qs) != 0 {
		t.Fatalf("expected empty registry, got %v, %v", qs, err)
	}

	// A missing registry table reads as an empty registry.
	rows := []*proto.QueryRows{{Error: "no such table: __rqlite_queries"}}
	if qs, err := RegisteredQueriesFromRows(rows); err != nil || len(qs) != 0 {
		t.Fatalf("expected empty registry, got %v, %v", qs, err)
	}

	rows = []*proto.QueryRows{{
		Columns: []string{"id", "sql"},
		Values: []*proto.Values{
			{Parameters: []*proto.Parameter{
				{Value: &proto.Parameter_S{S: "id1"}},
				{Value: &proto.Parameter_S{S: "SELECT 1"}},
			}},
			{Parameters: []*proto.Parameter{
				{Value: &proto.Parameter_S{S: "id2"}},
				{Value: &proto.Parameter_S{S: "SELECT 2"}},
			}},
		},
	}}
	qs, err := RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert rows: %s", err)
	}
	if len(qs) != 2 || qs[0].ID != "id1" || qs[0].SQL != "SELECT 1" ||
		qs[1].ID != "id2" || qs[1].SQL != "SELECT 2" {
		t.Fatalf("unexpected registered queries: %v", qs)
	}

	// Any other error is reported.
	rows = []*proto.QueryRows{{Error: "disk I/O error"}}
	if _, err := RegisteredQueriesFromRows(rows); err == nil {
		t.Fatalf("expected error for registry read failure")
	}
}

// Test_SingleNodeRegisteredQueries tests that a registered query is written
// through Raft, and can be read back, invoked, and deregistered.
func Test_SingleNodeRegisteredQueries(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}

	// Reading an empty registry is not an error, even though the registry
	// table does not exist yet.
	rows, _, _, err := s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{RegisteredQueriesStatement()},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to read empty registry: %s", err)
	}
	qs, err := RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert empty registry: %s", err)
	}
	if len(qs) != 0 {
		t.Fatalf("expected empty registry, got %v", qs)
	}

	// Create a table and register a query which reads it.
	_, _, err = s.Execute(context.Background(), executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "declan")`,
	}, false, false))
	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}

	regID := NewRegisteredQueryID()
	_, idx, err := s.Execute(context.Background(), &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction:     true,
			RollbackOnError: true,
			Statements: RegisterQueryStatements([]RegisteredQuery{
				{ID: regID, SQL: `SELECT name FROM foo WHERE id = ?`},
			}),
		},
	})
	if err != nil {
		t.Fatalf("failed to register query: %s", err)
	}
	if idx == 0 {
		t.Fatalf("registration did not return a Raft index")
	}

	// The registration is in the registry.
	rows, _, _, err = s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{RegisteredQueriesStatement()},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to list registry: %s", err)
	}
	qs, err = RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert registry: %s", err)
	}
	if len(qs) != 1 || qs[0].ID != regID || qs[0].SQL != `SELECT name FROM foo WHERE id = ?` {
		t.Fatalf("unexpected registry contents: %v", qs)
	}

	// Read it back by ID.
	rows, _, _, err = s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{RegisteredQueryStatement(regID)},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to read registered query: %s", err)
	}
	qs, err = RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert registered query: %s", err)
	}
	if len(qs) != 1 || qs[0].ID != regID {
		t.Fatalf("unexpected registered query: %v", qs)
	}

	// Invoke it with a parameter.
	stmt := RegisteredQueryStatement(regID)
	rows, _, _, err = s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{stmt},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to read registered query: %s", err)
	}
	qs, err = RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert registered query: %s", err)
	}
	results, _, _, err := s.Request(context.Background(), &proto.ExecuteQueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: qs[0].SQL,
					Parameters: []*proto.Parameter{
						{Value: &proto.Parameter_I{I: 2}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to invoke registered query: %s", err)
	}
	if len(results) != 1 || results[0].GetQ() == nil {
		t.Fatalf("unexpected invoke result: %v", results)
	}
	if exp, got := `[["declan"]]`, asJSON(results[0].GetQ().Values); exp != got {
		t.Fatalf("unexpected invoke result\nexp: %s\ngot: %s", exp, got)
	}

	// Deregistering removes it.
	_, _, err = s.Execute(context.Background(), &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{DeregisterQueryStatement(regID)},
		},
	})
	if err != nil {
		t.Fatalf("failed to deregister query: %s", err)
	}
	rows, _, _, err = s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{RegisteredQueriesStatement()},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to list registry: %s", err)
	}
	qs, err = RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert registry: %s", err)
	}
	if len(qs) != 0 {
		t.Fatalf("expected empty registry after deregistration, got %v", qs)
	}
}

// Test_SingleNodeRegisteredQueriesSnapshot tests that a registered query
// survives a snapshot and the log truncation that follows it.
func Test_SingleNodeRegisteredQueriesSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}

	regID := NewRegisteredQueryID()
	_, _, err := s.Execute(context.Background(), &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction:     true,
			RollbackOnError: true,
			Statements: RegisterQueryStatements([]RegisteredQuery{
				{ID: regID, SQL: `SELECT 1`},
			}),
		},
	})
	if err != nil {
		t.Fatalf("failed to register query: %s", err)
	}

	// Snapshot, then restart without the clean-snapshot marker so the node
	// cannot fast-restart on the existing SQLite file and must restore from
	// the Raft snapshot instead. The registration is carried in that snapshot.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot: %s", err)
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err)
	}
	if err := fsutil.Remove(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to remove clean snapshot marker: %s", err)
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to reopen single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}

	rows, _, _, err := s.Query(context.Background(), &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{RegisteredQueryStatement(regID)},
		},
		Level: proto.ConsistencyLevel_NONE,
	})
	if err != nil {
		t.Fatalf("failed to read registered query after snapshot: %s", err)
	}
	qs, err := RegisteredQueriesFromRows(rows)
	if err != nil {
		t.Fatalf("failed to convert registered query: %s", err)
	}
	if len(qs) != 1 || qs[0].ID != regID || qs[0].SQL != "SELECT 1" {
		t.Fatalf("registered query did not survive snapshot: %v", qs)
	}
}
