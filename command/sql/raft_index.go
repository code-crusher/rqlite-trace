package sql

import (
	"strconv"
	"strings"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// RaftIndexToken is a placeholder that may appear anywhere a numeric
// literal is valid in a SQL statement. It is replaced with the Raft log
// index the statement is applied under, so statements can record, and
// query by, the log position they were committed at. For example, after
//
//	INSERT INTO facts (subject, relation, object, log_idx)
//	    VALUES (?, ?, ?, __RQLITE_RAFT_INDEX__)
//
// sequentially-consistent reads are possible by selecting rows with
// log_idx less than or equal to the latest Raft index a node has applied.
// See https://github.com/rqlite/rqlite/issues/829.
const RaftIndexToken = "__RQLITE_RAFT_INDEX__"

// ContainsRaftIndex returns true if the statement contains the Raft index
// placeholder. The function performs a case-sensitive comparison, since
// the placeholder must appear verbatim to be interpolated.
func ContainsRaftIndex(stmt string) bool {
	return strings.Contains(stmt, RaftIndexToken)
}

// InterpolateRaftIndex replaces any occurrence of RaftIndexToken in the
// given statements with idx, modifying the statements in place. It is a
// no-op if no statement contains the placeholder.
func InterpolateRaftIndex(stmts []*proto.Statement, idx uint64) {
	contained := false
	for i := range stmts {
		if stmts[i] != nil && ContainsRaftIndex(stmts[i].Sql) {
			contained = true
			break
		}
	}
	if !contained {
		return
	}

	index := strconv.FormatUint(idx, 10)
	for i := range stmts {
		if stmts[i] != nil && ContainsRaftIndex(stmts[i].Sql) {
			stmts[i].Sql = strings.ReplaceAll(stmts[i].Sql, RaftIndexToken, index)
		}
	}
}
