package db

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/rqlite/sql"
)

// StmtMetadata describes a SQL statement, without executing it.
type StmtMetadata struct {
	// ReadOnly reports whether the statement only reads the database.
	ReadOnly bool `json:"read_only"`

	// Parameters holds one entry per bind parameter, in the order in which
	// SQLite numbers the parameters. Each entry is the parameter as it
	// appears in the SQL, e.g. ":id", "@name", "$value" or "?2", or the empty
	// string for a parameter which the SQL does not name, such as a bare "?".
	Parameters []string `json:"parameters"`
}

// stmtParameters returns the parameters of the given SQL statement, in
// parameter-number order. n is the number of parameters as reported by SQLite,
// and is authoritative; the parser is used only to recover the parameter
// names, so any names which cannot be recovered are left empty.
//
// SQLite numbers parameters as follows: "?NNN" is parameter NNN, a bare "?"
// is one more than the largest number assigned so far, and a named parameter
// such as ":id" is the number assigned to the first parameter with that name,
// or one more than the largest number assigned so far if it has not been seen
// before. See https://www.sqlite.org/lang_expr.html#parameters.
func stmtParameters(stmtSQL string, n int) []string {
	params := make([]string, n)
	if n == 0 {
		return params
	}

	// SQLite has already accepted the statement, so use whatever the parser
	// managed to parse, even if it reported an error. The parser can lag
	// SQLite's syntax, and a partial parse still recovers most names.
	parsed, _ := sql.NewParser(strings.NewReader(stmtSQL)).ParseStatement()
	if parsed == nil {
		return params
	}
	v := &bindExprVisitor{}
	if _, err := sql.Walk(v, parsed); err != nil {
		return params
	}
	// SQLite numbers parameters in the order in which they appear in the SQL,
	// so sort by position rather than relying on the walk order.
	slices.SortFunc(v.exprs, func(a, b *sql.BindExpr) int {
		return cmp.Compare(a.NamePos.Offset, b.NamePos.Offset)
	})

	named := make(map[string]int)
	next := 0
	for _, expr := range v.exprs {
		name := expr.Name
		var idx int
		switch {
		case name == "?":
			// A bare "?" is an anonymous parameter, which SQLite does not
			// name, and which takes the next parameter number.
			next++
			idx = next
			name = ""
		case strings.HasPrefix(name, "?"):
			nnn, err := strconv.Atoi(name[1:])
			if err != nil || nnn < 1 {
				continue
			}
			idx = nnn
			if idx > next {
				next = idx
			}
		default:
			if i, ok := named[name]; ok {
				idx = i
				break
			}
			next++
			idx = next
			named[name] = idx
		}
		// The first name assigned to a parameter slot wins, so a later
		// reference by number does not rename a slot which already has a name.
		if idx <= n && name != "" && params[idx-1] == "" {
			params[idx-1] = name
		}
	}
	return params
}

// bindExprVisitor collects the bind expressions of a statement.
type bindExprVisitor struct {
	exprs []*sql.BindExpr
}

func (v *bindExprVisitor) Visit(node sql.Node) (sql.Visitor, sql.Node, error) {
	if be, ok := node.(*sql.BindExpr); ok {
		v.exprs = append(v.exprs, be)
	}
	return v, node, nil
}

func (v *bindExprVisitor) VisitEnd(node sql.Node) (sql.Node, error) {
	return node, nil
}
