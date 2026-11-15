package db

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// tableHeader encodes the changeset table header for a table of the given
// name and primary-key flags, per the CHANGESET FORMAT comments in the
// SQLite source.
func tableHeader(table string, pk []bool) []byte {
	b := []byte{'T', byte(len(pk))}
	for _, isPK := range pk {
		if isPK {
			b = append(b, 0x01)
		} else {
			b = append(b, 0x00)
		}
	}
	b = append(b, table...)
	return append(b, 0x00)
}

// appendSQLiteVarint appends n as a SQLite varint. The values encoded by
// these tests are small enough to need a single byte, so that is all that
// is supported here.
func appendSQLiteVarint(b []byte, n int) []byte {
	if n < 0 || n >= 0x80 {
		panic(fmt.Sprintf("unsupported fixture varint %d", n))
	}
	return append(b, byte(n))
}

// appendField appends the changeset encoding of one record field. The
// field lengths of the fixtures fit in a single-byte SQLite varint.
func appendField(b []byte, v any) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, changesetFieldNull)
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		return append(append(b, changesetFieldInteger), buf[:]...)
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
		return append(append(b, changesetFieldFloat), buf[:]...)
	case string:
		b = append(b, changesetFieldText)
		b = appendSQLiteVarint(b, len(v))
		return append(b, v...)
	case []byte:
		b = append(b, changesetFieldBlob)
		b = appendSQLiteVarint(b, len(v))
		return append(b, v...)
	}
	panic(fmt.Sprintf("unsupported fixture value %T", v))
}

// reflectDeepEqual reports deep equality, treating nil and empty byte
// slices as equal.
func reflectDeepEqual(a, b []any) bool {
	normalize := func(v []any) []any {
		out := make([]any, len(v))
		for i, x := range v {
			if bs, ok := x.([]byte); ok && len(bs) == 0 {
				x = []byte{}
			}
			out[i] = x
		}
		return out
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// The tests in this file cover ReadChangeset and ChangesetStatements, the
// Go-native changeset reader and replayer added for issue #169. The
// session-extension investigation behind them found that the go-sqlite3
// fork's ChangesetIterator crashes on any UPDATE change with unchanged
// columns (it passes the NULL sqlite3_value standing for an undefined field
// to sqlite3_value_type), that a session cannot coexist with the preupdate
// hook rqlite's CDC installs, and that tables without an explicit PRIMARY
// KEY are silently ignored. See the comment atop changeset.go.

// Test_ReadChangeset_Empty checks that the empty changeset produced when
// a session records no changes decodes to no changes.
func Test_ReadChangeset_Empty(t *testing.T) {
	changes, err := ReadChangeset(nil)
	if err != nil {
		t.Fatalf("error reading nil changeset: %s", err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected no changes, got %d", len(changes))
	}
	changes, err = ReadChangeset([]byte{})
	if err != nil {
		t.Fatalf("error reading empty changeset: %s", err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected no changes, got %d", len(changes))
	}
}

// Test_ReadChangeset_Malformed checks that corrupt input is rejected with
// ErrMalformedChangeset rather than panicking.
func Test_ReadChangeset_Malformed(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
	}{
		{"operation before table header", []byte{changesetOpInsert, 0x00}},
		{"unknown operation", append(tableHeader("foo", []bool{true}), 0xEE, 0x00)},
		{"truncated table header", []byte{'T', 0x02, 0x01}},
		{"truncated record", append(tableHeader("foo", []bool{true}), changesetOpInsert, 0x00, changesetFieldInteger, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)},
		{"unknown field type", append(tableHeader("foo", []bool{true}), changesetOpInsert, 0x00, 0xEE)},
		{"implausible column count", []byte{'T', 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0x01}},
		{"PK flag out of range", []byte{'T', 0x01, 0x02, 'f', 'o', 'o', 0x00}},
		{"unterminated table name", []byte{'T', 0x01, 0x01, 'f', 'o', 'o'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := ReadChangeset(tt.b)
			if err == nil {
				t.Fatalf("expected error, got %v", changes)
			}
			if !strings.Contains(err.Error(), "malformed changeset") {
				t.Fatalf("expected malformed changeset error, got %s", err)
			}
		})
	}
}

// Test_ReadChangeset_Values decodes a changeset covering every field type
// and both record shapes. The fixture is hand-encoded from the CHANGESET
// FORMAT comments in the SQLite source, independently of the decoder, so
// the test pins the format itself.
func Test_ReadChangeset_Values(t *testing.T) {
	// Table: 4 columns, the first is the primary key.
	b := tableHeader("tbl", []bool{true, false, false, false})

	// INSERT of a row holding one value of each storage type.
	b = append(b, changesetOpInsert, 0x00)
	b = appendField(b, int64(1))        // PK
	b = appendField(b, "fiona")         // TEXT
	b = appendField(b, 3.5)             // REAL
	b = appendField(b, []byte{0x00, 1}) // BLOB

	// UPDATE that changed the TEXT column only. The old record carries the
	// PK and the changed column; every other field is undefined. The new
	// record carries the new value of the changed column only.
	b = append(b, changesetOpUpdate, 0x00)
	b = appendField(b, int64(1)) // PK
	b = appendField(b, "fiona")  // old TEXT
	b = append(b, 0x00)          // unchanged REAL: undefined
	b = append(b, 0x00)          // unchanged BLOB: undefined
	b = append(b, 0x00)          // PK in the new record: unchanged
	b = appendField(b, "declan") // new TEXT
	b = append(b, 0x00, 0x00)    // unchanged REAL and BLOB

	// UPDATE that set the REAL column to NULL. A SQL NULL is a value: it
	// must not be conflated with the undefined fields around it.
	b = append(b, changesetOpUpdate, 0x00)
	b = appendField(b, int64(1))      // PK
	b = append(b, 0x00)               // TEXT: unchanged
	b = appendField(b, 3.5)           // old REAL
	b = append(b, 0x00)               // BLOB: unchanged
	b = append(b, 0x00)               // PK: unchanged
	b = append(b, 0x00)               // TEXT: unchanged
	b = append(b, changesetFieldNull) // new REAL: SQL NULL
	b = append(b, 0x00)               // BLOB: unchanged

	// DELETE of the row.
	b = append(b, changesetOpDelete, 0x00)
	b = appendField(b, int64(1))
	b = appendField(b, "declan")
	b = appendField(b, nil)
	b = appendField(b, []byte{0x00, 1})

	changes, err := ReadChangeset(b)
	if err != nil {
		t.Fatalf("error reading changeset: %s", err)
	}
	if len(changes) != 4 {
		t.Fatalf("expected 4 changes, got %d", len(changes))
	}

	ins, upd, updNull, del := changes[0], changes[1], changes[2], changes[3]
	if ins.Op != changesetOpInsert || upd.Op != changesetOpUpdate ||
		updNull.Op != changesetOpUpdate || del.Op != changesetOpDelete {
		t.Fatalf("unexpected operations: %d %d %d %d", ins.Op, upd.Op, updNull.Op, del.Op)
	}
	for i, c := range changes {
		if c.Table != "tbl" {
			t.Fatalf("change %d: table %q, want tbl", i, c.Table)
		}
		if len(c.PK) != 4 || !c.PK[0] || c.PK[1] || c.PK[2] || c.PK[3] {
			t.Fatalf("change %d: unexpected PK flags %v", i, c.PK)
		}
	}

	// The INSERT carries a full row.
	want := []any{int64(1), "fiona", 3.5, []byte{0x00, 1}}
	if !reflectDeepEqual(ins.New, want) {
		t.Fatalf("INSERT new: got %v, want %v", ins.New, want)
	}

	// The first UPDATE changed only the TEXT column.
	if _, undefined := upd.New[0].(changesetUndefined); !undefined {
		t.Fatalf("UPDATE new PK: got %v, want undefined", upd.New[0])
	}
	if upd.New[1] != "declan" {
		t.Fatalf("UPDATE new TEXT: got %v, want declan", upd.New[1])
	}
	if _, undefined := upd.New[2].(changesetUndefined); !undefined {
		t.Fatalf("UPDATE new REAL: got %v, want undefined", upd.New[2])
	}
	if upd.Old[1] != "fiona" {
		t.Fatalf("UPDATE old TEXT: got %v, want fiona", upd.Old[1])
	}

	// The second UPDATE set the REAL column to SQL NULL: a value, not an
	// undefined field.
	if v, undefined := updNull.New[2].(changesetUndefined); undefined {
		_ = v
		t.Fatalf("UPDATE new REAL: got undefined, want SQL NULL")
	}
	if updNull.New[2] != nil {
		t.Fatalf("UPDATE new REAL: got %v, want SQL NULL", updNull.New[2])
	}
	if updNull.Old[2] != 3.5 {
		t.Fatalf("UPDATE old REAL: got %v, want 3.5", updNull.Old[2])
	}

	// The DELETE carries the full old row.
	wantOld := []any{int64(1), "declan", nil, []byte{0x00, 1}}
	if !reflectDeepEqual(del.Old, wantOld) {
		t.Fatalf("DELETE old: got %v, want %v", del.Old, wantOld)
	}
	if del.New != nil {
		t.Fatalf("DELETE new: got %v, want nil", del.New)
	}
}

// Test_ReadChangeset_MultipleTables checks that a changeset with changes
// to several tables decodes each group against its own header.
func Test_ReadChangeset_MultipleTables(t *testing.T) {
	b := tableHeader("t1", []bool{true})
	b = append(b, changesetOpInsert, 0x00)
	b = appendField(b, int64(7))
	b = append(b, changesetOpDelete, 0x00)
	b = appendField(b, int64(7))

	b = append(b, tableHeader("t2", []bool{false, true})...)
	b = append(b, changesetOpInsert, 0x00)
	b = appendField(b, "x")
	b = appendField(b, int64(8))

	changes, err := ReadChangeset(b)
	if err != nil {
		t.Fatalf("error reading changeset: %s", err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}
	if changes[0].Table != "t1" || changes[1].Table != "t1" || changes[2].Table != "t2" {
		t.Fatalf("unexpected tables: %s %s %s", changes[0].Table, changes[1].Table, changes[2].Table)
	}
	if changes[2].PK[0] || !changes[2].PK[1] {
		t.Fatalf("unexpected PK flags for t2: %v", changes[2].PK)
	}
	if !reflectDeepEqual(changes[2].New, []any{"x", int64(8)}) {
		t.Fatalf("t2 INSERT new: got %v", changes[2].New)
	}
}

// Test_ReadChangeset_Patchset decodes a patchset: a DELETE that encodes
// only its PK fields, and an UPDATE whose single record holds the PK and
// the modified columns.
func Test_ReadChangeset_Patchset(t *testing.T) {
	b := tableHeader("tbl", []bool{true, false, false})
	b[0] = 'P'

	// DELETE: only the PK field is encoded, the other columns contribute
	// no bytes at all.
	b = append(b, changesetOpDelete, 0x00)
	b = appendField(b, int64(3))

	// UPDATE: one record, carrying the PK and the modified column, with
	// the unmodified column undefined.
	b = append(b, changesetOpUpdate, 0x00)
	b = appendField(b, int64(3))
	b = appendField(b, "new")
	b = append(b, 0x00)

	// INSERT: a full record, as in a changeset.
	b = append(b, changesetOpInsert, 0x00)
	b = appendField(b, int64(4))
	b = appendField(b, "y")
	b = appendField(b, "z")

	changes, err := ReadChangeset(b)
	if err != nil {
		t.Fatalf("error reading patchset: %s", err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}

	del := changes[0]
	if _, undefined := del.Old[1].(changesetUndefined); !undefined {
		t.Fatalf("patchset DELETE old[1]: got %v, want undefined", del.Old[1])
	}
	if del.Old[0] != int64(3) {
		t.Fatalf("patchset DELETE old[0]: got %v, want 3", del.Old[0])
	}

	upd := changes[1]
	if upd.Old[0] != int64(3) {
		t.Fatalf("patchset UPDATE old PK: got %v, want 3", upd.Old[0])
	}
	if upd.New[1] != "new" {
		t.Fatalf("patchset UPDATE new[1]: got %v, want new", upd.New[1])
	}
	if _, undefined := upd.Old[1].(changesetUndefined); !undefined {
		t.Fatalf("patchset UPDATE old[1]: got %v, want undefined", upd.Old[1])
	}

	ins := changes[2]
	if !reflectDeepEqual(ins.New, []any{int64(4), "y", "z"}) {
		t.Fatalf("patchset INSERT new: got %v", ins.New)
	}
}

// Test_ChangesetStatements_Generate checks the statements generated for
// each kind of change.
func Test_ChangesetStatements_Generate(t *testing.T) {
	names := func(table string) ([]string, error) {
		switch table {
		case "tbl":
			return []string{"id", "name", "score"}, nil
		}
		return nil, nil
	}

	// INSERT of a full row.
	insert := &ChangesetChange{
		Table: "tbl",
		PK:    []bool{true, false, false},
		Op:    changesetOpInsert,
		New:   []any{int64(1), "fiona", 3.5},
	}
	// UPDATE of the name column, with the surrounding columns undefined.
	update := &ChangesetChange{
		Table: "tbl",
		PK:    []bool{true, false, false},
		Op:    changesetOpUpdate,
		Old:   []any{int64(1), "fiona", changesetUndefined{}},
		New:   []any{changesetUndefined{}, "declan", changesetUndefined{}},
	}
	// UPDATE setting the score to SQL NULL.
	updateNull := &ChangesetChange{
		Table: "tbl",
		PK:    []bool{true, false, false},
		Op:    changesetOpUpdate,
		Old:   []any{int64(1), changesetUndefined{}, 3.5},
		New:   []any{changesetUndefined{}, changesetUndefined{}, nil},
	}
	// DELETE by primary key.
	del := &ChangesetChange{
		Table: "tbl",
		PK:    []bool{true, false, false},
		Op:    changesetOpDelete,
		Old:   []any{int64(1), "fiona", 3.5},
	}

	stmts, args, err := ChangesetStatements([]*ChangesetChange{insert, update, updateNull, del}, names)
	if err != nil {
		t.Fatalf("error generating statements: %s", err)
	}
	if len(stmts) != 4 || len(args) != 4 {
		t.Fatalf("expected 4 statements, got %d", len(stmts))
	}

	if stmts[0] != `INSERT INTO "tbl" VALUES (?,?,?)` {
		t.Fatalf("INSERT statement: got %s", stmts[0])
	}
	if !reflectDeepEqual(args[0], []any{int64(1), "fiona", 3.5}) {
		t.Fatalf("INSERT args: got %v", args[0])
	}

	if stmts[1] != `UPDATE "tbl" SET "name" = ? WHERE "id" IS ?` {
		t.Fatalf("UPDATE statement: got %s", stmts[1])
	}
	if !reflectDeepEqual(args[1], []any{"declan", int64(1)}) {
		t.Fatalf("UPDATE args: got %v", args[1])
	}

	// A SQL NULL must be bound as a value, not skipped.
	if stmts[2] != `UPDATE "tbl" SET "score" = ? WHERE "id" IS ?` {
		t.Fatalf("UPDATE NULL statement: got %s", stmts[2])
	}
	if !reflectDeepEqual(args[2], []any{nil, int64(1)}) {
		t.Fatalf("UPDATE NULL args: got %v", args[2])
	}

	if stmts[3] != `DELETE FROM "tbl" WHERE "id" IS ?` {
		t.Fatalf("DELETE statement: got %s", stmts[3])
	}
	if !reflectDeepEqual(args[3], []any{int64(1)}) {
		t.Fatalf("DELETE args: got %v", args[3])
	}
}

// Test_ChangesetStatements_Errors checks the error paths of statement
// generation.
func Test_ChangesetStatements_Errors(t *testing.T) {
	names := func(table string) ([]string, error) {
		return []string{"id", "name"}, nil
	}
	noSuchTable := func(table string) ([]string, error) {
		return nil, nil
	}

	// No changes means no statements.
	stmts, args, err := ChangesetStatements(nil, names)
	if err != nil || stmts != nil || args != nil {
		t.Fatalf("empty changeset: got %v %v %v", stmts, args, err)
	}

	// A table missing from the target database.
	c := &ChangesetChange{Table: "gone", Op: changesetOpInsert, New: []any{int64(1)}}
	if _, _, err := ChangesetStatements([]*ChangesetChange{c}, noSuchTable); err == nil ||
		!strings.Contains(err.Error(), "no such table") {
		t.Fatalf("missing table: got %v", err)
	}

	// A changeset whose width disagrees with the table.
	c = &ChangesetChange{Table: "tbl", Op: changesetOpInsert, New: []any{int64(1)}}
	if _, _, err := ChangesetStatements([]*ChangesetChange{c}, names); err == nil ||
		!strings.Contains(err.Error(), "malformed changeset") {
		t.Fatalf("width mismatch: got %v", err)
	}

	// A DELETE on a table without a primary key: the row cannot be
	// identified, so replay refuses.
	c = &ChangesetChange{
		Table: "tbl", Op: changesetOpDelete,
		PK: []bool{false, false}, Old: []any{int64(1), "x"},
	}
	if _, _, err := ChangesetStatements([]*ChangesetChange{c}, names); err == nil ||
		!strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("DELETE without PK: got %v", err)
	}
}

// Test_ChangesetReplay replays a changeset against a second database and
// checks that both databases then hold the same rows. The changes are the
// ones the session extension records for the statements run against the
// source database, spelled out here so the test does not depend on the
// fork's changeset iterator, which cannot read UPDATE changes (see the
// comment atop changeset.go). The half of the round trip exercised here
// is the apply half of what issue #169 proposed.
func Test_ChangesetReplay(t *testing.T) {
	// The source database runs the statements a session would record. It
	// is the ground truth for the replay.
	src, _ := mustOpenRawSQLite(t, "file:source.db?mode=memory",
		`CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT, score REAL)`,
		`INSERT INTO foo VALUES (1, 'fiona', 3.5)`,
		`INSERT INTO foo VALUES (2, 'declan', 2.5)`,
	)
	mustExecOnSQL(t, src, `INSERT INTO foo VALUES (3, 'ole', 1.5)`)
	mustExecOnSQL(t, src, `UPDATE foo SET score = 4.5 WHERE id = 2`)
	mustExecOnSQL(t, src, `UPDATE foo SET score = NULL WHERE id = 1`)
	mustExecOnSQL(t, src, `DELETE FROM foo WHERE id = 1`)

	// The target database starts at the state the session would have
	// started from, and receives the changeset.
	dst, dstPath := mustCreateOnDiskDatabase()
	defer fsutil.Remove(dstPath)
	defer dst.Close()
	mustExecute(dst, `CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT, score REAL)`)
	mustExecute(dst, `INSERT INTO foo VALUES (1, 'fiona', 3.5)`)
	mustExecute(dst, `INSERT INTO foo VALUES (2, 'declan', 2.5)`)

	// The changes the session records for the statements above.
	changes := []*ChangesetChange{
		{
			Table: "foo", Op: changesetOpInsert, PK: []bool{true, false, false},
			New: []any{int64(3), "ole", 1.5},
		},
		{
			Table: "foo", Op: changesetOpUpdate, PK: []bool{true, false, false},
			Old: []any{int64(2), changesetUndefined{}, 2.5},
			New: []any{changesetUndefined{}, changesetUndefined{}, 4.5},
		},
		{
			Table: "foo", Op: changesetOpUpdate, PK: []bool{true, false, false},
			Old: []any{int64(1), changesetUndefined{}, 3.5},
			New: []any{changesetUndefined{}, changesetUndefined{}, nil},
		},
		{
			Table: "foo", Op: changesetOpDelete, PK: []bool{true, false, false},
			Old: []any{int64(1), "fiona", nil},
		},
	}

	stmts, args, err := ChangesetStatements(changes, dst.ColumnNames)
	if err != nil {
		t.Fatalf("failed to generate statements: %s", err)
	}
	for i, stmt := range stmts {
		if err := executeWithParamsOnDB(dst, stmt, args[i]); err != nil {
			t.Fatalf("failed to execute %q: %s", stmt, err)
		}
	}

	// The replayed database must hold exactly the source database's rows.
	want := queryAsJSONOnSQL(t, src, `SELECT * FROM foo ORDER BY id`)
	got := queryAsJSONOnDB(t, dst, `SELECT * FROM foo ORDER BY id`)
	if want != got {
		t.Fatalf("rows after replay:\ngot  %s\nwant %s", got, want)
	}
}

// Test_SessionCapture verifies the capture half of what issue #169
// proposed: that the session extension, compiled into rqlite's SQLite
// fork, records the changes rqlite-style statements make. It reads the
// changeset back through the fork's iterator, which is safe as long as
// the changeset holds no UPDATE changes; the changes here are arranged
// so the session records a net INSERT per row, which never trips the
// iterator's crash on undefined fields.
func Test_SessionCapture(t *testing.T) {
	src, raw := mustOpenRawSQLite(t, "file:capture.db?mode=memory",
		`CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo VALUES (2, 'declan')`,
	)
	session, err := raw.CreateSession("main")
	if err != nil {
		t.Fatalf("failed to create session: %s", err)
	}
	defer session.DeleteSession()
	if err := session.AttachSession(""); err != nil {
		t.Fatalf("failed to attach session: %s", err)
	}

	mustExecOnSQL(t, src, `INSERT INTO foo VALUES (1, 'fiona')`)
	mustExecOnSQL(t, src, `DELETE FROM foo WHERE id = 2`)

	cs, err := sqlite3.NewChangeset(session)
	if err != nil {
		t.Fatalf("failed to create changeset: %s", err)
	}
	iter, err := sqlite3.NewChangesetIterator(cs)
	if err != nil {
		t.Fatalf("failed to create iterator: %s", err)
	}
	defer iter.Finalize()

	type captured struct {
		table string
		op    int
		row   []any
	}
	var got []captured
	for {
		more, err := iter.Next()
		if err != nil {
			t.Fatalf("failed to iterate changeset: %s", err)
		}
		if !more {
			break
		}
		table, nCol, op, _, err := iter.Op()
		if err != nil {
			t.Fatalf("failed to read operation: %s", err)
		}
		c := captured{table: table, op: op, row: make([]any, nCol)}
		if op == sqlite3.SQLITE_INSERT {
			if err := iter.New(c.row); err != nil {
				t.Fatalf("failed to read inserted row: %s", err)
			}
		} else if op == sqlite3.SQLITE_DELETE {
			if err := iter.Old(c.row); err != nil {
				t.Fatalf("failed to read deleted row: %s", err)
			}
		} else {
			t.Fatalf("unexpected UPDATE in changeset: the iterator cannot read it")
		}
		got = append(got, c)
	}

	want := []captured{
		{"foo", sqlite3.SQLITE_INSERT, []any{int64(1), "fiona"}},
		{"foo", sqlite3.SQLITE_DELETE, []any{int64(2), "declan"}},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d changes, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i].table != want[i].table || got[i].op != want[i].op ||
			!reflectDeepEqual(got[i].row, want[i].row) {
			t.Fatalf("change %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

// Test_SessionCapture_Issues documents the findings of the issue #169
// investigation, as behaviors of the session extension that any use of it
// for replication must account for. Each subtest names the finding it
// covers.
func Test_SessionCapture_Issues(t *testing.T) {
	t.Run("no primary key means no changes recorded", func(t *testing.T) {
		src, raw := mustOpenRawSQLite(t, "file:nopk.db?mode=memory",
			`CREATE TABLE foo (id INTEGER, name TEXT)`,
		)
		session, err := raw.CreateSession("main")
		if err != nil {
			t.Fatalf("failed to create session: %s", err)
		}
		defer session.DeleteSession()
		if err := session.AttachSession(""); err != nil {
			t.Fatalf("failed to attach session: %s", err)
		}
		mustExecOnSQL(t, src, `INSERT INTO foo VALUES (1, 'fiona')`)

		// The row is in the database, but the changeset is empty: without
		// a primary key a table's rows cannot be identified, and the
		// session ignores the table entirely. SQLITE_SESSION_OBJCONFIG_
		// ROWID would treat the rowid as the key, but the fork's Go
		// binding does not expose that setting.
		cs, err := sqlite3.NewChangeset(session)
		if err != nil {
			t.Fatalf("failed to create changeset: %s", err)
		}
		iter, err := sqlite3.NewChangesetIterator(cs)
		if err != nil {
			t.Fatalf("failed to create iterator: %s", err)
		}
		defer iter.Finalize()
		if more, err := iter.Next(); err != nil {
			t.Fatalf("failed to iterate changeset: %s", err)
		} else if more {
			t.Fatalf("expected empty changeset for a table without a primary key")
		}
		if n := queryCount(t, src, `SELECT COUNT(*) FROM foo`); n != 1 {
			t.Fatalf("the row itself must still be in the database")
		}
	})
}

// mustOpenRawSQLite opens a database through the SQLite driver directly,
// outside the DB type, runs the given statements, and returns the
// database/sql connection together with the raw driver connection. All
// statements issued on the returned connection hit the same underlying
// SQLite connection, which matters for in-memory databases (each pooled
// connection would be a separate database) and for the session extension,
// reachable only through the raw driver.
func mustOpenRawSQLite(t *testing.T, path string, stmts ...string) (*sql.Conn, *sqlite3.SQLiteConn) {
	t.Helper()
	sqldb, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	t.Cleanup(func() { sqldb.Close() })
	conn, err := sqldb.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get connection: %s", err)
	}
	t.Cleanup(func() { conn.Close() })
	var raw *sqlite3.SQLiteConn
	if err := conn.Raw(func(driverConn any) error {
		raw = driverConn.(*sqlite3.SQLiteConn)
		return nil
	}); err != nil {
		t.Fatalf("failed to get raw connection: %s", err)
	}
	for _, stmt := range stmts {
		mustExecOnSQL(t, conn, stmt)
	}
	return conn, raw
}

// mustExecOnSQL executes a statement on a database/sql connection.
func mustExecOnSQL(t *testing.T, conn *sql.Conn, stmt string) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("failed to execute %q: %s", stmt, err)
	}
}

// executeWithParamsOnDB executes a parameterized statement on a DB, the
// way the replay of a changeset would.
func executeWithParamsOnDB(db *DB, stmt string, args []any) error {
	params := make([]*command.Parameter, len(args))
	for i, arg := range args {
		switch v := arg.(type) {
		case nil:
			// A database NULL.
		case int64:
			params[i] = &command.Parameter{Value: &command.Parameter_I{I: v}}
		case float64:
			params[i] = &command.Parameter{Value: &command.Parameter_D{D: v}}
		case string:
			params[i] = &command.Parameter{Value: &command.Parameter_S{S: v}}
		case []byte:
			params[i] = &command.Parameter{Value: &command.Parameter_Y{Y: v}}
		default:
			return fmt.Errorf("unsupported replay value %T", arg)
		}
	}
	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{{Sql: stmt, Parameters: params}},
	}, false)
	return err
}

// queryAsJSONOnSQL runs a query on a database/sql connection and renders
// the rows as a list of column-name-to-value maps, for comparison against
// another database.
func queryAsJSONOnSQL(t *testing.T, conn *sql.Conn, query string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("failed to query %q: %s", query, err)
	}
	defer rows.Close()
	return rowsAsJSON(t, rows)
}

// rowsAsJSON renders database/sql rows as a list of column-name-to-value
// maps, for comparison against another database. TEXT columns scan as
// bytes and are rendered as strings.
func rowsAsJSON(t *testing.T, rows *sql.Rows) string {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to read columns: %s", err)
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("failed to scan row: %s", err)
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[c] = v
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to iterate rows: %s", err)
	}
	return asJSON(out)
}

// queryAsJSONOnDB runs a query on a DB and renders the rows the same way
// queryAsJSONOnSQL does, so two databases can be compared.
func queryAsJSONOnDB(t *testing.T, db *DB, query string) string {
	t.Helper()
	rows, err := db.QueryStringStmt(query)
	if err != nil {
		t.Fatalf("failed to query %q: %s", query, err)
	}
	if len(rows) != 1 || rows[0].GetError() != "" {
		t.Fatalf("query %q failed: %v", query, rows)
	}
	var out []map[string]any
	vals := rows[0].GetValues()
	for _, v := range vals {
		params := v.GetParameters()
		row := make(map[string]any, len(params))
		for i, p := range params {
			var value any
			switch pv := p.GetValue().(type) {
			case *command.Parameter_I:
				value = pv.I
			case *command.Parameter_D:
				value = pv.D
			case *command.Parameter_S:
				value = pv.S
			case *command.Parameter_Y:
				value = string(pv.Y)
			case *command.Parameter_B:
				value = pv.B
			case nil:
				value = nil
			}
			row[rows[0].GetColumns()[i]] = value
		}
		out = append(out, row)
	}
	return asJSON(out)
}

// queryCount runs a query expected to return a single integer.
func queryCount(t *testing.T, conn *sql.Conn, query string) int {
	t.Helper()
	var n int
	if err := conn.QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("failed to query %q: %s", query, err)
	}
	return n
}
