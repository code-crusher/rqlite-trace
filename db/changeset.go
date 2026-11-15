package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

// This file implements a reader for SQLite session-extension changesets,
// the format produced by sqlite3session_changeset() and documented in the
// SQLite source under "CHANGESET FORMAT" (see the comments in
// sqlite3-binding.c of the go-sqlite3 fork). Issue #169 asked whether the
// session extension could be used to replicate rqlite writes as changesets,
// and to apply changesets to other stores.
//
// The rqlite SQLite fork compiles the extension in and exposes capturing a
// changeset through SQLiteConn.CreateSession/NewChangeset. Reading a
// changeset back is implemented here in Go rather than through the fork's
// ChangesetIterator because that iterator passes the NULL sqlite3_value
// pointers, which stand for the undefined (unchanged) fields of an UPDATE
// record, straight to sqlite3_value_type, which crashes the process. A
// Go decoder is also all that is needed to apply a changeset to another
// database: replaying it as parameterized INSERT, UPDATE, and DELETE
// statements is equivalent to sqlite3changeset_apply for the
// no-conflict case, which is the only case that can arise when
// replicating rqlite's own serialized writes.

// Changeset operation types, as stored in a changeset.
const (
	changesetOpDelete byte = 0x09 // SQLITE_DELETE
	changesetOpInsert byte = 0x12 // SQLITE_INSERT
	changesetOpUpdate byte = 0x17 // SQLITE_UPDATE

	// Record field type bytes, as stored in a changeset record.
	changesetFieldUndefined byte = 0x00
	changesetFieldInteger   byte = 0x01
	changesetFieldFloat     byte = 0x02
	changesetFieldText      byte = 0x03
	changesetFieldBlob      byte = 0x04
	changesetFieldNull      byte = 0x05
)

// ErrMalformedChangeset is returned when a changeset cannot be parsed.
var ErrMalformedChangeset = errors.New("malformed changeset")

// changesetUndefined stands for an undefined record field: one whose value
// the change neither carries nor depends on, because an UPDATE did not
// modify the column. It is distinct from a database NULL, which is a real
// value and is represented by nil.
type changesetUndefined struct{}

// ChangesetChange is a single row change read from a changeset. Exactly one
// of Old and New carries values for an INSERT or DELETE; both do for an
// UPDATE. The values follow the column order of the table's CREATE TABLE
// statement. A nil value is a database NULL. The zero value of this type,
// changesetUndefined, marks a field the change does not carry: for an
// UPDATE it means the column was not modified, and the entry in the other
// record holds the value that applied throughout.
type ChangesetChange struct {
	// Table is the unqualified name of the table the change applies to.
	Table string

	// PK is a slice of as many booleans as there are columns, true for
	// PRIMARY KEY columns.
	PK []bool

	// Op is one of changesetOpDelete, changesetOpInsert, changesetOpUpdate.
	Op byte

	// Old and New hold the row values before and after the change.
	Old []any
	New []any
}

// String returns a rendering of the change for logging.
func (c *ChangesetChange) String() string {
	var op string
	switch c.Op {
	case changesetOpInsert:
		op = "INSERT"
	case changesetOpUpdate:
		op = "UPDATE"
	case changesetOpDelete:
		op = "DELETE"
	default:
		op = fmt.Sprintf("op(%d)", c.Op)
	}
	return fmt.Sprintf("%s %s old=%v new=%v", op, c.Table, c.Old, c.New)
}

// ReadChangeset decodes the changeset in b, returning the changes in the
// order they appear. An empty or nil slice decodes to an empty (non-nil)
// slice, which is what sqlite3session_changeset produces when no changes
// were recorded. See the comment at the top of this file for why this is
// implemented in Go.
func ReadChangeset(b []byte) ([]*ChangesetChange, error) {
	changes := make([]*ChangesetChange, 0, len(b)/16)
	r := &changesetReader{b: b}
	for {
		change, err := r.next()
		if err == io.EOF {
			return changes, nil
		}
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
}

// changesetReader reads the changeset wire format. All multi-byte integers
// are big-endian; see the format comments in the SQLite source.
type changesetReader struct {
	b []byte
	i int

	patchset bool // Last table header read was patchset format.

	// Fields of the table header most recently read.
	table string
	pk    []bool
	nCol  int
}

func (r *changesetReader) readByte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *changesetReader) readBytes(n int) ([]byte, error) {
	if n < 0 || r.i+n > len(r.b) {
		return nil, fmt.Errorf("%w: truncated at offset %d", ErrMalformedChangeset, r.i)
	}
	v := r.b[r.i : r.i+n]
	r.i += n
	return v, nil
}

// next returns the next change, or io.EOF once the changeset is exhausted.
func (r *changesetReader) next() (*ChangesetChange, error) {
	// Skip any number of table headers. 'T' introduces a changeset-format
	// header, 'P' a patchset-format one.
	for {
		save := r.i
		c, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if c == 'T' || c == 'P' {
			if err := r.readTableHeader(c == 'P'); err != nil {
				return nil, err
			}
			continue
		}
		// Not a table header: it is an operation byte, and the reader
		// must have read a header before it.
		r.i = save
		if r.table == "" {
			return nil, fmt.Errorf("%w: change before any table header", ErrMalformedChangeset)
		}
		break
	}

	op, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedChangeset, err)
	}
	if op != changesetOpDelete && op != changesetOpInsert && op != changesetOpUpdate {
		return nil, fmt.Errorf("%w: unknown operation 0x%02x", ErrMalformedChangeset, op)
	}
	if _, err := r.readByte(); err != nil { // The indirect-change flag.
		return nil, fmt.Errorf("%w: %v", ErrMalformedChangeset, err)
	}

	ch := &ChangesetChange{Table: r.table, Op: op, PK: r.pk}
	if r.patchset {
		if err := r.readPatchsetChange(ch); err != nil {
			return nil, err
		}
	} else {
		// A changeset record carries one field per column, so the reader
		// can decode it without reference to the primary key.
		if op != changesetOpInsert {
			if ch.Old, err = r.readRecord(); err != nil {
				return nil, err
			}
		}
		if op != changesetOpDelete {
			if ch.New, err = r.readRecord(); err != nil {
				return nil, err
			}
		}
	}
	return ch, nil
}

// readPatchsetChange reads the single record of a patchset change, which
// leaves out every field that is neither a primary key nor modified. A
// DELETE carries just the PK fields, with no bytes at all for the others;
// an INSERT or UPDATE carries a full-width record in which the unmodified
// fields are undefined. The PK fields identify an UPDATE's row before and
// after the change, so they are copied into Old.
func (r *changesetReader) readPatchsetChange(ch *ChangesetChange) error {
	switch ch.Op {
	case changesetOpDelete:
		old, err := r.readSparseRecord(ch.PK)
		if err != nil {
			return err
		}
		ch.Old = old
		ch.New = nil
		return nil

	case changesetOpInsert:
		new, err := r.readRecord()
		if err != nil {
			return err
		}
		ch.Old = nil
		ch.New = new
		return nil

	case changesetOpUpdate:
		new, err := r.readRecord()
		if err != nil {
			return err
		}
		ch.New = new
		ch.Old = make([]any, r.nCol)
		for i, isPK := range ch.PK {
			if isPK {
				ch.Old[i] = new[i]
			} else {
				ch.Old[i] = changesetUndefined{}
			}
		}
		return nil
	}
	return fmt.Errorf("%w: unknown operation 0x%02x", ErrMalformedChangeset, ch.Op)
}

// readSparseRecord reads a record in which only the fields selected by
// present are encoded; the other fields do not appear at all, not even as
// a type byte, and come back as changesetUndefined. A nil present selects
// every field.
func (r *changesetReader) readSparseRecord(present []bool) ([]any, error) {
	values := make([]any, r.nCol)
	for i := 0; i < r.nCol; i++ {
		if present != nil && !present[i] {
			values[i] = changesetUndefined{}
			continue
		}
		if err := r.readField(values, i); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// readTableHeader reads the table header introduced by the byte T or P that
// the caller has already consumed. patchset is true for 'P'.
func (r *changesetReader) readTableHeader(patchset bool) error {
	r.patchset = patchset
	nCol, err := r.readVarint()
	if err != nil {
		return fmt.Errorf("%w: reading column count: %v", ErrMalformedChangeset, err)
	}
	if nCol <= 0 || nCol > 0x7FFFFFFF {
		return fmt.Errorf("%w: implausible column count %d", ErrMalformedChangeset, nCol)
	}
	pk, err := r.readBytes(nCol)
	if err != nil {
		return fmt.Errorf("%w: reading PK flags: %v", ErrMalformedChangeset, err)
	}
	flags := make([]bool, nCol)
	for i, f := range pk {
		if f > 1 {
			return fmt.Errorf("%w: PK flag %d out of range", ErrMalformedChangeset, f)
		}
		flags[i] = f == 1
	}
	// The table name is NUL-terminated.
	end := r.i
	for end < len(r.b) && r.b[end] != 0 {
		end++
	}
	if end == len(r.b) {
		return fmt.Errorf("%w: unterminated table name", ErrMalformedChangeset)
	}
	r.table = string(r.b[r.i:end])
	r.i = end + 1
	r.pk = flags
	r.nCol = nCol
	return nil
}

// readRecord reads one full-width record: one field per column of the
// table, each introduced by its type byte. Every entry of the returned
// slice carries a value: changesetUndefined where the record marks the
// field undefined, nil for a database NULL.
func (r *changesetReader) readRecord() ([]any, error) {
	values := make([]any, r.nCol)
	for i := 0; i < r.nCol; i++ {
		if err := r.readField(values, i); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// readField reads the field for column i into values.
func (r *changesetReader) readField(values []any, i int) error {
	t, err := r.readByte()
	if err != nil {
		return fmt.Errorf("%w: reading field %d: %v", ErrMalformedChangeset, i, err)
	}
	switch t {
	case changesetFieldUndefined:
		values[i] = changesetUndefined{}
	case changesetFieldNull:
		// Keep nil: a database NULL.
	case changesetFieldInteger:
		b, err := r.readBytes(8)
		if err != nil {
			return fmt.Errorf("%w: field %d: %v", ErrMalformedChangeset, i, err)
		}
		values[i] = int64(binary.BigEndian.Uint64(b))
	case changesetFieldFloat:
		b, err := r.readBytes(8)
		if err != nil {
			return fmt.Errorf("%w: field %d: %v", ErrMalformedChangeset, i, err)
		}
		values[i] = math.Float64frombits(binary.BigEndian.Uint64(b))
	case changesetFieldText, changesetFieldBlob:
		n, err := r.readVarint()
		if err != nil {
			return fmt.Errorf("%w: field %d length: %v", ErrMalformedChangeset, i, err)
		}
		if n < 0 || n > len(r.b)-r.i {
			return fmt.Errorf("%w: field %d length %d out of range", ErrMalformedChangeset, i, n)
		}
		b, err := r.readBytes(n)
		if err != nil {
			return fmt.Errorf("%w: field %d: %v", ErrMalformedChangeset, i, err)
		}
		if t == changesetFieldText {
			values[i] = string(b)
		} else {
			v := make([]byte, n)
			copy(v, b)
			values[i] = v
		}
	default:
		return fmt.Errorf("%w: field %d has unknown type 0x%02x", ErrMalformedChangeset, i, t)
	}
	return nil
}

// readVarint reads a SQLite varint: up to 9 bytes, big-endian. In the
// first 8 bytes only the low 7 bits carry the value; a 9th byte, if
// present, carries 8 full bits. See the "Variable-length Integers" section
// of the SQLite file format documentation.
func (r *changesetReader) readVarint() (int, error) {
	var v uint64
	for i := 0; i < 8; i++ {
		b, err := r.readByte()
		if err != nil {
			return 0, fmt.Errorf("%w: %v", ErrMalformedChangeset, err)
		}
		if b&0x80 == 0 {
			return int(v | uint64(b)), nil
		}
		v = v<<7 | uint64(b&0x7F)
	}
	// Ninth byte: all 8 bits are used.
	b, err := r.readByte()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrMalformedChangeset, err)
	}
	return int(v<<8 | uint64(b)), nil
}

// opName returns the name of a changeset operation byte.
func opName(op byte) string {
	switch op {
	case changesetOpDelete:
		return "DELETE"
	case changesetOpInsert:
		return "INSERT"
	case changesetOpUpdate:
		return "UPDATE"
	}
	return fmt.Sprintf("op(%d)", op)
}

// ChangesetStatements renders the changes in cs as parameterized SQL
// statements, in order, suitable for replaying against a database whose
// schema matches the one the changeset was recorded against. The statements
// are those that sqlite3changeset_apply would run: an INSERT of the new
// row, a DELETE of the row identified by its primary key, and, for an
// UPDATE, an UPDATE of the columns the change modified. As with
// sqlite3changeset_apply, rows are identified by primary key and compared
// with IS, which matches SQL NULL keys. The schema of the target database
// supplies the column names, since a changeset identifies columns only by
// position.
//
// The returned statements and arguments are parallel slices: args[i] holds
// the values for the ? placeholders of stmts[i]. The caller is expected to
// apply them within a single transaction if the whole changeset is to be
// applied atomically.
func ChangesetStatements(cs []*ChangesetChange, columnNames func(table string) ([]string, error)) (stmts []string, args [][]any, err error) {
	if len(cs) == 0 {
		return nil, nil, nil
	}

	// A changeset identifies columns by position, so column names are
	// needed from the target database, one slice per table. The C
	// changeset-apply implementation fetches the same thing through its
	// xFilter callback.
	cols := make(map[string][]string)
	for _, c := range cs {
		if _, ok := cols[c.Table]; !ok {
			names, err := columnNames(c.Table)
			if err != nil {
				return nil, nil, err
			}
			if len(names) == 0 {
				return nil, nil, fmt.Errorf("no such table: %s", c.Table)
			}
			cols[c.Table] = names
		}
	}

	for _, c := range cs {
		names := cols[c.Table]
		if len(c.Old) != 0 && len(c.Old) != len(names) ||
			len(c.New) != 0 && len(c.New) != len(names) {
			return nil, nil, fmt.Errorf("%w: table %s has %d columns, changeset has %d",
				ErrMalformedChangeset, c.Table, len(names), max(len(c.Old), len(c.New)))
		}

		switch c.Op {
		case changesetOpInsert:
			s, a := insertStatement(c.Table, c.New)
			stmts = append(stmts, s)
			args = append(args, a)

		case changesetOpDelete:
			s, a, err := deleteStatement(c.Table, names, c.Old, c.PK)
			if err != nil {
				return nil, nil, err
			}
			stmts = append(stmts, s)
			args = append(args, a)

		case changesetOpUpdate:
			s, a, err := updateStatement(c.Table, names, c)
			if err != nil {
				return nil, nil, err
			}
			stmts = append(stmts, s)
			args = append(args, a)

		default:
			return nil, nil, fmt.Errorf("%w: unknown operation 0x%02x", ErrMalformedChangeset, c.Op)
		}
	}
	return stmts, args, nil
}

// insertStatement returns a parameterized INSERT of one full row. The
// values are in column order, so the statement needs no column list.
func insertStatement(table string, values []any) (string, []any) {
	args := make([]any, len(values))
	for i, v := range values {
		if _, undefined := v.(changesetUndefined); undefined {
			// An INSERT record has no undefined fields; a changeset with
			// one is malformed, and binding NULL is the least-surprise
			// handling.
			v = nil
		}
		args[i] = v
	}
	return fmt.Sprintf("INSERT INTO %s VALUES (%s)",
		quoteIdent(table), strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")), args
}

// deleteStatement returns a parameterized DELETE of the row identified by
// its primary key.
func deleteStatement(table string, names []string, values []any, pk []bool) (string, []any, error) {
	conds := make([]string, 0, len(pk))
	args := make([]any, 0, len(pk))
	for i, isPK := range pk {
		if !isPK {
			continue
		}
		conds = append(conds, fmt.Sprintf("%s IS ?", quoteIdent(names[i])))
		args = append(args, values[i])
	}
	if len(conds) == 0 {
		return "", nil, fmt.Errorf("%w: DELETE on %s, which has no primary key",
			ErrMalformedChangeset, table)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteIdent(table), strings.Join(conds, " AND ")), args, nil
}

// updateStatement returns a parameterized UPDATE of the columns the change
// modified. Within an UPDATE change the primary-key fields of the old and
// new records are equal, so either record identifies the row.
func updateStatement(table string, names []string, c *ChangesetChange) (string, []any, error) {
	var set []string
	var setArgs []any
	for i, v := range c.New {
		if _, undefined := v.(changesetUndefined); undefined {
			// The change did not modify this column.
			continue
		}
		set = append(set, fmt.Sprintf("%s = ?", quoteIdent(names[i])))
		setArgs = append(setArgs, v)
	}
	conds := make([]string, 0, len(c.PK))
	var whereArgs []any
	for i, isPK := range c.PK {
		if !isPK {
			continue
		}
		if _, undefined := c.Old[i].(changesetUndefined); undefined {
			return "", nil, fmt.Errorf("%w: UPDATE on %s lacks a primary key value",
				ErrMalformedChangeset, table)
		}
		conds = append(conds, fmt.Sprintf("%s IS ?", quoteIdent(names[i])))
		whereArgs = append(whereArgs, c.Old[i])
	}
	if len(conds) == 0 {
		return "", nil, fmt.Errorf("%w: UPDATE on %s, which has no primary key",
			ErrMalformedChangeset, table)
	}
	if len(set) == 0 {
		// A no-op UPDATE. Return a valid statement so the caller's
		// transaction does not have to special-case it.
		set = []string{quoteIdent(names[0])}
		setArgs = []any{c.Old[0]}
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			quoteIdent(table), strings.Join(set, ", "), strings.Join(conds, " AND ")),
		append(setArgs, whereArgs...), nil
}

// quoteIdent quotes an SQL identifier, doubling any embedded quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
