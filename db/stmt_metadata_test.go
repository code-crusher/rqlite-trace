package db

import (
	"slices"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_DB_StmtMetadata(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()

	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	tests := []struct {
		name      string
		sql       string
		readOnly  bool
		params    []string
		errString string
	}{
		{
			name:     "select without parameters",
			sql:      `SELECT * FROM foo`,
			readOnly: true,
		},
		{
			name:     "select with anonymous parameters",
			sql:      `SELECT * FROM foo WHERE id = ? AND name = ?`,
			readOnly: true,
			params:   []string{"", ""},
		},
		{
			name:     "select with named parameters",
			sql:      `SELECT * FROM foo WHERE id = :id AND name = @name`,
			readOnly: true,
			params:   []string{":id", "@name"},
		},
		{
			name:     "parameter numbers which leave a gap",
			sql:      `SELECT * FROM foo WHERE id = ?3 AND name = $name`,
			readOnly: true,
			params:   []string{"", "", "?3", "$name"},
		},
		{
			name:     "repeated named parameter shares a number",
			sql:      `UPDATE foo SET name = :name WHERE id = :name`,
			readOnly: false,
			params:   []string{":name"},
		},
		{
			name:     "insert",
			sql:      `INSERT INTO foo(id, name) VALUES(?, ?)`,
			readOnly: false,
			params:   []string{"", ""},
		},
		{
			name:     "update with explicit parameter number",
			sql:      `UPDATE foo SET name = ?2 WHERE id = ?`,
			readOnly: false,
			params:   []string{"", "?2", ""},
		},
		{
			name:     "bare parameter after numbered parameter",
			sql:      `SELECT * FROM foo WHERE id = ?2 AND name = ?`,
			readOnly: true,
			params:   []string{"", "?2", ""},
		},
		{
			name:     "bare parameter before named parameter",
			sql:      `SELECT ?, :name, ?`,
			readOnly: true,
			params:   []string{"", ":name", ""},
		},
		{
			name:     "named parameter referenced later by number keeps its name",
			sql:      `SELECT :id, ?1`,
			readOnly: true,
			params:   []string{":id"},
		},
		{
			name:     "numbered parameter referenced before a named one",
			sql:      `SELECT ?3, ?1`,
			readOnly: true,
			params:   []string{"?1", "", "?3"},
		},
		{
			name:     "parameters which differ only by prefix are distinct",
			sql:      `SELECT * FROM foo WHERE id = :id AND name = @id`,
			readOnly: true,
			params:   []string{":id", "@id"},
		},
		{
			name:     "parameter names are case-sensitive",
			sql:      `SELECT * FROM foo WHERE id = :id AND name = :ID`,
			readOnly: true,
			params:   []string{":id", ":ID"},
		},
		{
			name:     "numbered parameter with leading zero",
			sql:      `SELECT * FROM foo WHERE id = ?01`,
			readOnly: true,
			params:   []string{"?01"},
		},
		{
			name:     "parameters are numbered in the order they appear in the SQL",
			sql:      `WITH c AS (SELECT :b) SELECT :a FROM c WHERE 1 = ?`,
			readOnly: true,
			params:   []string{":b", ":a", ""},
		},
		{
			name:      "invalid SQL",
			sql:       `SLECT * FORM foo`,
			errString: "syntax error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := db.StmtMetadata(tt.sql)
			if tt.errString != "" {
				if err == nil {
					t.Fatalf("expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("failed to get statement metadata: %s", err.Error())
			}
			if md.ReadOnly != tt.readOnly {
				t.Fatalf("expected read-only %v, got %v", tt.readOnly, md.ReadOnly)
			}
			if !slices.Equal(md.Parameters, tt.params) {
				t.Fatalf("expected parameters %v, got %v", tt.params, md.Parameters)
			}
		})
	}
}

func Test_DB_StmtMetadata_DoesNotExecute(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()

	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	if _, err := db.StmtMetadata(`INSERT INTO foo(id, name) VALUES(1, 'fiona')`); err != nil {
		t.Fatalf("failed to get statement metadata: %s", err.Error())
	}

	rows, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := int64(0), rows[0].Values[0].Parameters[0].GetI(); exp != got {
		t.Fatalf("expected %d rows, got %d", exp, got)
	}
}
