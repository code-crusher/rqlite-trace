package sql

import (
	"regexp"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func regexpMatch(pattern, s string) bool {
	return regexp.MustCompile(pattern).MatchString(s)
}

func Test_ContainsRaftIndex(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		{
			name:     "Contains placeholder",
			stmt:     "INSERT INTO foo VALUES (1, __RQLITE_RAFT_INDEX__)",
			expected: true,
		},
		{
			name:     "Contains placeholder in WHERE clause",
			stmt:     "SELECT * FROM foo WHERE log_idx <= __RQLITE_RAFT_INDEX__",
			expected: true,
		},
		{
			name:     "No placeholder - unrelated statement",
			stmt:     "SELECT * FROM foo",
			expected: false,
		},
		{
			name:     "No placeholder - empty statement",
			stmt:     "",
			expected: false,
		},
		{
			name:     "No placeholder - similar but not exact match",
			stmt:     "SELECT * FROM foo WHERE log_idx <= RQLITE_RAFT_INDEX",
			expected: false,
		},
		{
			name:     "Placeholder is case-sensitive",
			stmt:     "INSERT INTO foo VALUES (1, __rqlite_raft_index__)",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsRaftIndex(tt.stmt); got != tt.expected {
				t.Errorf("ContainsRaftIndex(%q) = %v; want %v", tt.stmt, got, tt.expected)
			}
		})
	}
}

func Test_InterpolateRaftIndex(t *testing.T) {
	t.Run("no placeholder", func(t *testing.T) {
		stmts := []*proto.Statement{
			{Sql: `INSERT INTO foo VALUES (1, 'bob')`},
			{Sql: `SELECT * FROM foo`},
		}
		InterpolateRaftIndex(stmts, 42)
		if exp, got := `INSERT INTO foo VALUES (1, 'bob')`, stmts[0].Sql; exp != got {
			t.Fatalf("SQL is modified: %s", got)
		}
		if exp, got := `SELECT * FROM foo`, stmts[1].Sql; exp != got {
			t.Fatalf("SQL is modified: %s", got)
		}
	})

	t.Run("single placeholder", func(t *testing.T) {
		stmts := []*proto.Statement{
			{Sql: `INSERT INTO foo VALUES (1, __RQLITE_RAFT_INDEX__)`},
		}
		InterpolateRaftIndex(stmts, 42)
		if exp, got := `INSERT INTO foo VALUES (1, 42)`, stmts[0].Sql; exp != got {
			t.Fatalf("interpolated %q, want %q", got, exp)
		}
	})

	t.Run("multiple occurrences and statements", func(t *testing.T) {
		stmts := []*proto.Statement{
			{Sql: `INSERT INTO foo VALUES (__RQLITE_RAFT_INDEX__, __RQLITE_RAFT_INDEX__)`},
			{Sql: `SELECT * FROM foo WHERE idx <= __RQLITE_RAFT_INDEX__ AND idx >= __RQLITE_RAFT_INDEX__`},
		}
		InterpolateRaftIndex(stmts, 7)
		if exp, got := `INSERT INTO foo VALUES (7, 7)`, stmts[0].Sql; exp != got {
			t.Fatalf("interpolated %q, want %q", got, exp)
		}
		if exp, got := `SELECT * FROM foo WHERE idx <= 7 AND idx >= 7`, stmts[1].Sql; exp != got {
			t.Fatalf("interpolated %q, want %q", got, exp)
		}
	})

	t.Run("mixed statements", func(t *testing.T) {
		stmts := []*proto.Statement{
			{Sql: `INSERT INTO foo VALUES (1, 'bob')`},
			{Sql: `INSERT INTO foo VALUES (2, __RQLITE_RAFT_INDEX__)`},
		}
		InterpolateRaftIndex(stmts, 12345)
		if exp, got := `INSERT INTO foo VALUES (1, 'bob')`, stmts[0].Sql; exp != got {
			t.Fatalf("SQL is modified: %s", got)
		}
		if exp, got := `INSERT INTO foo VALUES (2, 12345)`, stmts[1].Sql; exp != got {
			t.Fatalf("interpolated %q, want %q", got, exp)
		}
	})

	t.Run("nil-safe", func(t *testing.T) {
		stmts := []*proto.Statement{
			nil,
			{Sql: `INSERT INTO foo VALUES (1, __RQLITE_RAFT_INDEX__)`},
		}
		InterpolateRaftIndex(stmts, 3)
		if exp, got := `INSERT INTO foo VALUES (1, 3)`, stmts[1].Sql; exp != got {
			t.Fatalf("interpolated %q, want %q", got, exp)
		}
	})
}

// Test_RaftIndexToken_Preserved verifies that SQL processing does not
// mangle statements containing the Raft index placeholder, since the
// placeholder must survive intact until it is interpolated at apply time.
func Test_RaftIndexToken_Preserved(t *testing.T) {
	t.Run("no rewriting enabled", func(t *testing.T) {
		sqlStr := `INSERT INTO foo VALUES (__RQLITE_RAFT_INDEX__, 'bob')`
		stmts := []*proto.Statement{{Sql: sqlStr}}
		if err := Process(stmts, false, false); err != nil {
			t.Fatalf("failed to process: %s", err)
		}
		if exp, got := sqlStr, stmts[0].Sql; exp != got {
			t.Fatalf("SQL is modified: %s", got)
		}
	})

	t.Run("with random rewriting", func(t *testing.T) {
		stmts := []*proto.Statement{
			{Sql: `INSERT INTO foo VALUES (RANDOM(), __RQLITE_RAFT_INDEX__)`},
		}
		if err := Process(stmts, true, false); err != nil {
			t.Fatalf("failed to process: %s", err)
		}
		if !ContainsRaftIndex(stmts[0].Sql) {
			t.Fatalf("Raft index token is missing after processing: %s", stmts[0].Sql)
		}
		if !regexpMatch(`INSERT INTO "foo" VALUES \(-?[0-9]+, "__RQLITE_RAFT_INDEX__"\)`, stmts[0].Sql) {
			t.Fatalf("unexpected rewrite result: %s", stmts[0].Sql)
		}
	})
}
