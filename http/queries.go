package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/command/sql"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

// queriesPath is the path of the registered query API. A request to this path
// registers or lists queries; a request to this path followed by a query ID
// reads, invokes, or deregisters that query.
const queriesPath = "/db/queries"

// registeredQueriesResponse is the response to a request which reads or
// changes the registry of registered queries.
type registeredQueriesResponse struct {
	Results   []store.RegisteredQuery `json:"results"`
	Error     string                  `json:"error,omitempty"`
	RaftIndex uint64                  `json:"raft_index,omitempty"`
	Time      float64                 `json:"time,omitempty"`

	start time.Time
	end   time.Time
}

// SetTime sets the Time attribute of the response. This way it will be present
// in the serialized JSON version.
func (r *registeredQueriesResponse) SetTime() {
	r.Time = r.end.Sub(r.start).Seconds()
}

func newRegisteredQueriesResponse() *registeredQueriesResponse {
	return &registeredQueriesResponse{
		Results: make([]store.RegisteredQuery, 0),
		start:   time.Now(),
	}
}

// handleQueries serves the registered query API. Registering a query writes it
// through Raft, so it reaches every node and is captured by snapshots.
func (s *Service) handleQueries(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// The path is either the registry itself or the registry followed by a
	// query ID. Anything else is not part of this API.
	var id string
	if r.URL.Path != queriesPath {
		if !strings.HasPrefix(r.URL.Path, queriesPath+"/") {
			http.NotFound(w, r)
			return
		}
		id = strings.TrimPrefix(r.URL.Path, queriesPath+"/")
	}
	if id == "" {
		switch r.Method {
		case http.MethodGet:
			if !s.CheckRequestPerm(r, auth.PermQuery) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			s.listRegisteredQueries(w, r, qp)
		case http.MethodPost:
			if !s.CheckRequestPerm(r, auth.PermExecute) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			s.registerQueries(w, r, qp)
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	// An ID is generated as a single path element, so anything else is not a
	// valid ID.
	if strings.Contains(id, "/") {
		http.Error(w, "invalid query ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if !s.CheckRequestPerm(r, auth.PermQuery) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		s.getRegisteredQuery(w, r, qp, id)
	case http.MethodPost:
		// A registered query may modify the database, so invoking one needs
		// the same permissions as a request which may do both.
		if !s.CheckRequestPermAll(r, auth.PermQuery, auth.PermExecute) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		s.invokeRegisteredQuery(w, r, qp, id)
	case http.MethodDelete:
		if !s.CheckRequestPerm(r, auth.PermExecute) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		s.deregisterQuery(w, r, qp, id)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// registerQueries stores the statements in the request body in the registry,
// under freshly-generated IDs, and returns those IDs.
func (s *Service) registerQueries(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	stmts, err := ParseRequest(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queries := make([]store.RegisteredQuery, len(stmts))
	for i, stmt := range stmts {
		if strings.TrimSpace(stmt.Sql) == "" {
			http.Error(w, "registered query cannot be empty", http.StatusBadRequest)
			return
		}
		if len(stmt.Parameters) != 0 {
			http.Error(w, "registered query cannot have parameters", http.StatusBadRequest)
			return
		}
		queries[i] = store.RegisteredQuery{
			ID:  store.NewRegisteredQueryID(),
			SQL: stmt.Sql,
		}
	}

	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction:     true,
			RollbackOnError: true,
			Statements:      store.RegisterQueryStatements(queries),
		},
	}
	results, raftIndex, addr, resultsErr := s.proxy.Execute(r.Context(), er, makeCredentials(r),
		qp.Timeout(defaultTimeout), qp.Retries(0), qp.Redirect())
	if resultsErr != nil {
		if s.handleQueriesProxyErr(w, r, qp, resultsErr) {
			return
		}
	}

	resp := newRegisteredQueriesResponse()
	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else if err := executeResponseError(results); err != nil {
		resp.Error = err.Error()
	} else {
		stats.Add(numQueriesRegistered, int64(len(queries)))
		w.Header().Set(ServedByHTTPHeader, addr)
		resp.Results = queries
		if qp.RaftIndex() {
			resp.RaftIndex = raftIndex
		}
	}
	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

// listRegisteredQueries returns every query in the registry.
func (s *Service) listRegisteredQueries(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	queries, _, addr, err := s.readRegistry(r, qp, store.RegisteredQueriesStatement())
	if err != nil {
		if s.handleQueriesProxyErr(w, r, qp, err) {
			return
		}
		resp := newRegisteredQueriesResponse()
		resp.Error = err.Error()
		resp.end = time.Now()
		s.writeResponse(w, qp, resp)
		return
	}

	w.Header().Set(ServedByHTTPHeader, addr)
	resp := newRegisteredQueriesResponse()
	resp.Results = queries
	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

// getRegisteredQuery returns the query registered under id.
func (s *Service) getRegisteredQuery(w http.ResponseWriter, r *http.Request, qp QueryParams, id string) {
	queries, _, addr, err := s.readRegistry(r, qp, store.RegisteredQueryStatement(id))
	if err != nil {
		if s.handleQueriesProxyErr(w, r, qp, err) {
			return
		}
		resp := newRegisteredQueriesResponse()
		resp.Error = err.Error()
		resp.end = time.Now()
		s.writeResponse(w, qp, resp)
		return
	}
	if len(queries) == 0 {
		stats.Add(numQueriesNotFound, 1)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set(ServedByHTTPHeader, addr)
	resp := newRegisteredQueriesResponse()
	resp.Results = queries
	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

// invokeRegisteredQuery runs the query registered under id, supplying it with
// the parameters in the request body. The body is a JSON array of positional
// parameters, a JSON object of named parameters, or empty for no parameters.
func (s *Service) invokeRegisteredQuery(w http.ResponseWriter, r *http.Request, qp QueryParams, id string) {
	parameters, err := parseRegisteredQueryParameters(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queries, _, _, err := s.readRegistry(r, qp, store.RegisteredQueryStatement(id))
	if err != nil {
		if s.handleQueriesProxyErr(w, r, qp, err) {
			return
		}
		resp := NewResponse()
		resp.Error = err.Error()
		resp.end = time.Now()
		s.writeResponse(w, qp, resp)
		return
	}
	if len(queries) == 0 {
		stats.Add(numQueriesNotFound, 1)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	stmts := []*proto.Statement{
		{
			Sql:        queries[0].SQL,
			Parameters: parameters,
		},
	}
	if !qp.NoParse() {
		if err := sql.Process(stmts, !qp.NoRewriteRandom(), !qp.NoRewriteTime()); err != nil {
			http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
			return
		}
	}
	stats.Add(numQueriesInvoked, 1)

	resp := NewResponse()
	resp.Results.AssociativeJSON = qp.Associative()
	resp.Results.BlobsAsArrays = qp.BlobArray()
	resp.Results.Errors = qp.Errors()

	eqr := &proto.ExecuteQueryRequest{
		Request: &proto.Request{
			Transaction:    qp.Tx(),
			Statements:     stmts,
			DbTimeout:      int64(qp.DBTimeout(0)),
			QualifyColumns: qp.QualifyColumns(),
		},
		Timings:             qp.Timings(),
		Level:               qp.Level(),
		Freshness:           qp.Freshness().Nanoseconds(),
		FreshnessStrict:     qp.FreshnessStrict(),
		LinearizableTimeout: qp.LinearizableTimeout(defaultLinearTimeout).Nanoseconds(),
	}

	results, _, raftIndex, addr, resultsErr := s.proxy.Request(r.Context(), eqr, makeCredentials(r),
		qp.Timeout(defaultTimeout), qp.Retries(0), qp.Redirect())
	if resultsErr != nil {
		if s.handleQueriesProxyErr(w, r, qp, resultsErr) {
			return
		}
		resp.Error = resultsErr.Error()
	} else {
		w.Header().Set(ServedByHTTPHeader, addr)
		resp.Results.ExecuteQueryResponse = results
		if qp.RaftIndex() {
			resp.RaftIndex = raftIndex
		}
	}
	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

// deregisterQuery removes the query registered under id.
func (s *Service) deregisterQuery(w http.ResponseWriter, r *http.Request, qp QueryParams, id string) {
	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{store.DeregisterQueryStatement(id)},
		},
	}
	results, raftIndex, addr, resultsErr := s.proxy.Execute(r.Context(), er, makeCredentials(r),
		qp.Timeout(defaultTimeout), qp.Retries(0), qp.Redirect())
	if resultsErr != nil {
		if s.handleQueriesProxyErr(w, r, qp, resultsErr) {
			return
		}
	}

	resp := newRegisteredQueriesResponse()
	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else if err := executeResponseError(results); err != nil {
		if strings.Contains(err.Error(), "no such table") {
			stats.Add(numQueriesNotFound, 1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		resp.Error = err.Error()
	} else if rowsAffected(results) == 0 {
		stats.Add(numQueriesNotFound, 1)
		w.WriteHeader(http.StatusNotFound)
		return
	} else {
		stats.Add(numQueriesDeregistered, 1)
		w.Header().Set(ServedByHTTPHeader, addr)
		if qp.RaftIndex() {
			resp.RaftIndex = raftIndex
		}
	}
	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

// readRegistry reads the queries returned by stmt from the registry. A
// registry which has never been written to has no table, which reads as an
// empty registry rather than an error.
func (s *Service) readRegistry(r *http.Request, qp QueryParams, stmt *proto.Statement) ([]store.RegisteredQuery, uint64, string, error) {
	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{stmt},
		},
		Timings:             qp.Timings(),
		Level:               qp.Level(),
		Freshness:           qp.Freshness().Nanoseconds(),
		FreshnessStrict:     qp.FreshnessStrict(),
		LinearizableTimeout: qp.LinearizableTimeout(defaultLinearTimeout).Nanoseconds(),
	}
	rows, raftIndex, addr, err := s.proxy.Query(r.Context(), qr, makeCredentials(r),
		qp.Timeout(defaultTimeout), qp.Retries(0), qp.Redirect())
	if err != nil {
		return nil, 0, "", err
	}
	queries, err := store.RegisteredQueriesFromRows(rows)
	return queries, raftIndex, addr, err
}

// handleQueriesProxyErr writes a response for err if it is an error the proxy
// layer returns for a request this node cannot serve. It reports whether a
// response was written.
func (s *Service) handleQueriesProxyErr(w http.ResponseWriter, r *http.Request, qp QueryParams, err error) bool {
	switch {
	case errors.Is(err, proxy.ErrNotLeader):
		s.DoRedirect(w, r, qp)
		return true
	case errors.Is(err, proxy.ErrLeaderNotFound):
		stats.Add(numLeaderNotFound, 1)
		http.Error(w, proxy.ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
		return true
	case errors.Is(err, proxy.ErrUnauthorized):
		http.Error(w, "remote registered query request not authorized", http.StatusUnauthorized)
		return true
	}
	return false
}

// parseRegisteredQueryParameters parses the parameters for a registered query.
// The body is a JSON array of positional parameters, a JSON object of named
// parameters, or empty.
func parseRegisteredQueryParameters(r io.Reader) ([]*proto.Parameter, error) {
	if r == nil {
		return nil, nil
	}
	dec := json.NewDecoder(r)
	dec.UseNumber()
	t, err := dec.Token()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, ErrInvalidJSON
	}

	var params []*proto.Parameter
	switch t {
	case json.Delim('['):
		for dec.More() {
			var v any
			if err := dec.Decode(&v); err != nil {
				return nil, ErrInvalidJSON
			}
			p, err := makeParameter("", v)
			if err != nil {
				return nil, err
			}
			params = append(params, p)
		}
	case json.Delim('{'):
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, ErrInvalidJSON
			}
			var v any
			if err := dec.Decode(&v); err != nil {
				return nil, ErrInvalidJSON
			}
			p, err := makeParameter(key.(string), v)
			if err != nil {
				return nil, err
			}
			params = append(params, p)
		}
	default:
		return nil, ErrInvalidRequest
	}

	// Consume the closing bracket or brace.
	if _, err := dec.Token(); err != nil {
		return nil, ErrInvalidJSON
	}
	// The array or object must be the only value in the body.
	if _, err := dec.Token(); err != io.EOF {
		return nil, ErrInvalidRequest
	}
	return params, nil
}

// executeResponseError returns the first error reported by the statements of
// an execute response.
func executeResponseError(results []*proto.ExecuteQueryResponse) error {
	for _, r := range results {
		if errStr := r.GetError(); errStr != "" {
			return errors.New(errStr)
		}
		if e := r.GetE(); e != nil && e.Error != "" {
			return errors.New(e.Error)
		}
	}
	return nil
}

// rowsAffected returns the number of rows changed by the first statement of an
// execute response.
func rowsAffected(results []*proto.ExecuteQueryResponse) int64 {
	if len(results) == 0 {
		return 0
	}
	return results[0].GetE().GetRowsAffected()
}
