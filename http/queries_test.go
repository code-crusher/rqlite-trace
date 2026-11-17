package http

import (
	"encoding/json"
	"expvar"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

// registryRows returns the rows a registry read of the given queries would
// return.
func registryRows(queries ...store.RegisteredQuery) []*command.QueryRows {
	rows := &command.QueryRows{
		Columns: []string{"id", "sql"},
	}
	for _, q := range queries {
		rows.Values = append(rows.Values, &command.Values{
			Parameters: []*command.Parameter{
				{Value: &command.Parameter_S{S: q.ID}},
				{Value: &command.Parameter_S{S: q.SQL}},
			},
		})
	}
	return []*command.QueryRows{rows}
}

// executeOK returns an execute response reporting a single changed row.
func executeOK() []*command.ExecuteQueryResponse {
	return []*command.ExecuteQueryResponse{{
		Result: &command.ExecuteQueryResponse_E{
			E: &command.ExecuteResult{RowsAffected: 1},
		},
	}}
}

// mustNewQueriesService returns a started service backed by the given mocks.
func mustNewQueriesService(t *testing.T, m *MockStore, c *mockClusterService, cred CredentialStore) (*Service, string) {
	t.Helper()
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), cred)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	return s, "http://" + s.Addr().String()
}

// doQueriesRequest performs an HTTP request against the registered query API
// and returns the response and its body.
func doQueriesRequest(t *testing.T, method, url, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create request: %s", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to make request: %s", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %s", err)
	}
	return resp, string(b)
}

// Test_Queries_Register tests that registering queries writes them through
// Raft and returns the generated IDs.
func Test_Queries_Register(t *testing.T) {
	var gotStmts []*command.Statement
	var gotTx bool
	m := &MockStore{leaderAddr: "foo:1234"}
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		gotStmts = er.Request.Statements
		gotTx = er.Request.Transaction
		return executeOK(), 7, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodPost, host+"/db/queries",
		`["SELECT * FROM foo", "SELECT * FROM bar"]`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var r struct {
		Results   []store.RegisteredQuery `json:"results"`
		RaftIndex uint64                  `json:"raft_index"`
	}
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatalf("failed to unmarshal response: %s", err)
	}
	if len(r.Results) != 2 {
		t.Fatalf("expected 2 registered queries, got %d", len(r.Results))
	}
	if r.Results[0].SQL != "SELECT * FROM foo" || r.Results[1].SQL != "SELECT * FROM bar" {
		t.Fatalf("unexpected registered queries: %v", r.Results)
	}
	if r.Results[0].ID == "" || r.Results[0].ID == r.Results[1].ID {
		t.Fatalf("expected distinct non-empty IDs, got %v", r.Results)
	}
	if !gotTx {
		t.Fatalf("registration was not written as a transaction")
	}
	if len(gotStmts) != 3 {
		t.Fatalf("expected 3 statements (create + 2 inserts), got %d", len(gotStmts))
	}
	if gotStmts[0].Sql != store.QueriesCreateTableSQL {
		t.Fatalf("first statement should create the registry table, got %s", gotStmts[0].Sql)
	}
	if got := gotStmts[1].Parameters[0].GetS(); got != r.Results[0].ID {
		t.Fatalf("insert used ID %s, response reported %s", got, r.Results[0].ID)
	}
}

// Test_Queries_RegisterWithParams tests that a query which already contains
// parameters is rejected, since its parameters could not be supplied at
// invocation time.
func Test_Queries_RegisterWithParams(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodPost, host+"/db/queries",
		`[["SELECT * FROM foo WHERE id = ?", 1]]`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}

	// A blank query would register nothing useful.
	resp, body = doQueriesRequest(t, http.MethodPost, host+"/db/queries", `["  "]`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for blank query, got %d: %s", resp.StatusCode, body)
	}
}

// Test_Queries_List tests that the registry is listed, and that an empty
// registry, which has no table, lists as empty rather than failing.
func Test_Queries_List(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		if qr.Request.Statements[0].Sql != store.QueriesSelectAllSQL {
			t.Fatalf("unexpected registry read: %s", qr.Request.Statements[0].Sql)
		}
		return registryRows(
			store.RegisteredQuery{ID: "id1", SQL: "SELECT 1"},
			store.RegisteredQuery{ID: "id2", SQL: "SELECT 2"},
		), 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	if exp, got := `{"results":[{"id":"id1","sql":"SELECT 1"},{"id":"id2","sql":"SELECT 2"}]}`, body; exp != got {
		t.Fatalf("unexpected response\nexp: %s\ngot: %s", exp, got)
	}

	// A registry which has never been written to has no table.
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return []*command.QueryRows{{Error: "no such table: __rqlite_queries"}}, 0, nil
	}
	resp, body = doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for empty registry, got %d: %s", resp.StatusCode, body)
	}
	if exp, got := `{"results":[]}`, body; exp != got {
		t.Fatalf("unexpected response\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_Queries_Get tests reading a single registered query by ID.
func Test_Queries_Get(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		stmt := qr.Request.Statements[0]
		if stmt.Sql != store.QueriesSelectSQL {
			t.Fatalf("unexpected registry read: %s", stmt.Sql)
		}
		if stmt.Parameters[0].GetS() != "id1" {
			t.Fatalf("unexpected ID parameter: %s", stmt.Parameters[0].GetS())
		}
		return registryRows(store.RegisteredQuery{ID: "id1", SQL: "SELECT 1"}), 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodGet, host+"/db/queries/id1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	if exp, got := `{"results":[{"id":"id1","sql":"SELECT 1"}]}`, body; exp != got {
		t.Fatalf("unexpected response\nexp: %s\ngot: %s", exp, got)
	}

	// An unknown ID is not found.
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return registryRows(), 0, nil
	}
	resp, body = doQueriesRequest(t, http.MethodGet, host+"/db/queries/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown ID, got %d: %s", resp.StatusCode, body)
	}
}

// Test_Queries_Invoke tests invoking a registered query with positional and
// named parameters.
func Test_Queries_Invoke(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		expParam func(p *command.Parameter) bool
	}{
		{
			name: "positional",
			body: `[42]`,
			expParam: func(p *command.Parameter) bool {
				return p.GetName() == "" && p.GetI() == 42
			},
		},
		{
			name: "named",
			body: `{"id": 42}`,
			expParam: func(p *command.Parameter) bool {
				return p.GetName() == "id" && p.GetI() == 42
			},
		},
		{
			name:     "none",
			body:     "",
			expParam: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MockStore{leaderAddr: "foo:1234"}
			m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
				return registryRows(store.RegisteredQuery{
					ID:  "id1",
					SQL: "SELECT name FROM foo WHERE id = :id",
				}), 0, nil
			}
			var gotStmt *command.Statement
			m.requestFn = func(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
				gotStmt = eqr.Request.Statements[0]
				return []*command.ExecuteQueryResponse{{
					Result: &command.ExecuteQueryResponse_Q{
						Q: &command.QueryRows{
							Columns: []string{"name"},
							Types:   []string{"text"},
							Values: []*command.Values{
								{Parameters: []*command.Parameter{
									{Value: &command.Parameter_S{S: "fiona"}},
								}},
							},
						},
					},
				}}, 0, 11, nil
			}
			s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
			defer s.Close()

			resp, body := doQueriesRequest(t, http.MethodPost, host+"/db/queries/id1", tt.body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
			}
			if gotStmt == nil {
				t.Fatalf("registered query was not invoked")
			}
			if gotStmt.Sql != "SELECT name FROM foo WHERE id = :id" {
				t.Fatalf("unexpected SQL: %s", gotStmt.Sql)
			}
			if tt.expParam == nil {
				if len(gotStmt.Parameters) != 0 {
					t.Fatalf("expected no parameters, got %v", gotStmt.Parameters)
				}
			} else {
				if len(gotStmt.Parameters) != 1 || !tt.expParam(gotStmt.Parameters[0]) {
					t.Fatalf("unexpected parameters: %v", gotStmt.Parameters)
				}
			}
			if exp, got := `{"results":[{"columns":["name"],"types":["text"],"values":[["fiona"]]}]}`, body; exp != got {
				t.Fatalf("unexpected response\nexp: %s\ngot: %s", exp, got)
			}
		})
	}
}

// Test_Queries_InvokeUnknown tests that invoking an unknown query is not found.
func Test_Queries_InvokeUnknown(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return registryRows(), 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodPost, host+"/db/queries/nope", `[1]`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
	}
}

// Test_Queries_InvokeBadParams tests that malformed parameters are rejected.
func Test_Queries_InvokeBadParams(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	for _, body := range []string{`"hello"`, `[1] [2]`, `{`} {
		resp, respBody := doQueriesRequest(t, http.MethodPost, host+"/db/queries/id1", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for body %q, got %d: %s", body, resp.StatusCode, respBody)
		}
	}
}

// Test_Queries_Delete tests deregistering a query.
func Test_Queries_Delete(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	var gotStmt *command.Statement
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		gotStmt = er.Request.Statements[0]
		return executeOK(), 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodDelete, host+"/db/queries/id1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	if gotStmt.Sql != store.QueriesDeleteSQL || gotStmt.Parameters[0].GetS() != "id1" {
		t.Fatalf("unexpected deregistration statement: %s %v", gotStmt.Sql, gotStmt.Parameters)
	}

	// A delete which matches no row is not found.
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return []*command.ExecuteQueryResponse{{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{RowsAffected: 0},
			},
		}}, 0, nil
	}
	resp, body = doQueriesRequest(t, http.MethodDelete, host+"/db/queries/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
	}

	// A delete against a registry which has never been written to is also not
	// found.
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return []*command.ExecuteQueryResponse{{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{Error: "no such table: __rqlite_queries"},
			},
		}}, 0, nil
	}
	resp, body = doQueriesRequest(t, http.MethodDelete, host+"/db/queries/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing table, got %d: %s", resp.StatusCode, body)
	}
}

// Test_Queries_MethodNotAllowed tests that unsupported methods are rejected.
func Test_Queries_MethodNotAllowed(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, _ := doQueriesRequest(t, http.MethodDelete, host+"/db/queries", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for DELETE on registry, got %d", resp.StatusCode)
	}
	resp, _ = doQueriesRequest(t, http.MethodPut, host+"/db/queries/id1", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for PUT on query, got %d", resp.StatusCode)
	}
}

// Test_Queries_Auth tests that each operation requires the permission it needs.
func Test_Queries_Auth(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	cred := &mockCredentialStore{}
	s := New("127.0.0.1:0", m, &mockClusterService{}, proxy.New(m, &mockClusterService{}), cred)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	defer s.Close()
	host := "http://" + s.Addr().String()

	// No permissions at all.
	resp, _ := doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for GET without perm, got %d", resp.StatusCode)
	}
	resp, _ = doQueriesRequest(t, http.MethodPost, host+"/db/queries", `["SELECT 1"]`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for POST without perm, got %d", resp.StatusCode)
	}

	// A query-only user may read but not register.
	cred.aaFunc = func(username, password, perm string) bool {
		return perm == "query"
	}
	resp, _ = doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for GET with query perm, got %d", resp.StatusCode)
	}
	resp, _ = doQueriesRequest(t, http.MethodPost, host+"/db/queries", `["SELECT 1"]`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for POST with query perm only, got %d", resp.StatusCode)
	}

	// An execute-only user may register but not read.
	cred.aaFunc = func(username, password, perm string) bool {
		return perm == "execute"
	}
	resp, _ = doQueriesRequest(t, http.MethodPost, host+"/db/queries", `["SELECT 1"]`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for POST with execute perm, got %d", resp.StatusCode)
	}
	resp, _ = doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for GET with execute perm only, got %d", resp.StatusCode)
	}
}

// Test_Queries_Redirect tests that a follower which is asked to redirect does
// so for the registered query API.
func Test_Queries_Redirect(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return nil, 0, store.ErrNotLeader
	}
	c := &mockClusterService{apiAddr: "http://bar:5678"}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	defer s.Close()
	host := "http://" + s.Addr().String()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(host + "/db/queries?redirect")
	if err != nil {
		t.Fatalf("failed to make request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", resp.StatusCode)
	}
	if exp, got := "http://bar:5678/db/queries?redirect", resp.Header.Get("Location"); exp != got {
		t.Fatalf("unexpected redirect location\nexp: %s\ngot: %s", exp, got)
	}

	// Without the redirect flag, the request is forwarded to the leader.
	c.queryFn = func(qr *command.QueryRequest, addr string, timeout time.Duration) ([]*command.QueryRows, uint64, error) {
		return registryRows(), 0, nil
	}
	resp, body := doQueriesRequest(t, http.MethodGet, host+"/db/queries", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	if exp, got := `{"results":[]}`, body; exp != got {
		t.Fatalf("unexpected response\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_Queries_RequestForwarded tests that a request forwarded to the leader
// carries the registered query and its parameters.
func Test_Queries_RequestForwarded(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return nil, 0, store.ErrNotLeader
	}
	c := &mockClusterService{apiAddr: "http://bar:5678"}
	c.queryFn = func(qr *command.QueryRequest, addr string, timeout time.Duration) ([]*command.QueryRows, uint64, error) {
		return registryRows(store.RegisteredQuery{ID: "id1", SQL: "SELECT 1"}), 0, nil
	}
	m.requestFn = func(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
		return nil, 0, 0, store.ErrNotLeader
	}
	c.requestFn = func(eqr *command.ExecuteQueryRequest, nodeAddr string, timeout time.Duration) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
		if nodeAddr != "foo:1234" {
			t.Fatalf("request forwarded to %s, want leader foo:1234", nodeAddr)
		}
		return []*command.ExecuteQueryResponse{{
			Result: &command.ExecuteQueryResponse_Q{
				Q: &command.QueryRows{
					Columns: []string{"1"},
					Types:   []string{"integer"},
					Values:  []*command.Values{{}},
				},
			},
		}}, 0, 0, nil
	}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	defer s.Close()
	host := "http://" + s.Addr().String()

	resp, body := doQueriesRequest(t, http.MethodPost, host+"/db/queries/id1", `[]`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get(ServedByHTTPHeader); got != "foo:1234" {
		t.Fatalf("unexpected served-by header: %s", got)
	}
}

// Test_Queries_PathRouting tests that the registered query API does not
// shadow the existing database endpoints.
func Test_Queries_PathRouting(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	var gotSQL string
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		gotSQL = qr.Request.Statements[0].Sql
		return registryRows(), 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	// /db/query must still be served by the query endpoint, which passes the
	// requested statement through rather than reading the registry.
	if _, body := doQueriesRequest(t, http.MethodGet, host+"/db/query?q=SELECT+1", ""); body == "" {
		t.Fatalf("query endpoint returned no response")
	}
	if gotSQL != "SELECT 1" {
		t.Fatalf("registered query API handled a /db/query request, got SQL %q", gotSQL)
	}
}

// Test_Queries_InvalidID tests that a path with more than one element after
// the API prefix is rejected.
func Test_Queries_InvalidID(t *testing.T) {
	m := &MockStore{leaderAddr: "foo:1234"}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	resp, body := doQueriesRequest(t, http.MethodGet, host+"/db/queries/a/b", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
}

// Test_Queries_Stats tests that the registered query stats are tracked.
func Test_Queries_Stats(t *testing.T) {
	before := map[string]int64{
		numQueriesRegistered:   stats.Get(numQueriesRegistered).(*expvar.Int).Value(),
		numQueriesInvoked:      stats.Get(numQueriesInvoked).(*expvar.Int).Value(),
		numQueriesDeregistered: stats.Get(numQueriesDeregistered).(*expvar.Int).Value(),
	}
	m := &MockStore{leaderAddr: "foo:1234"}
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return executeOK(), 0, nil
	}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return registryRows(store.RegisteredQuery{ID: "id1", SQL: "SELECT 1"}), 0, nil
	}
	m.requestFn = func(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
		return nil, 0, 0, nil
	}
	s, host := mustNewQueriesService(t, m, &mockClusterService{}, nil)
	defer s.Close()

	doQueriesRequest(t, http.MethodPost, host+"/db/queries", `["SELECT 1"]`)
	doQueriesRequest(t, http.MethodPost, host+"/db/queries/id1", "")
	doQueriesRequest(t, http.MethodDelete, host+"/db/queries/id1", "")

	for k := range before {
		if got := stats.Get(k).(*expvar.Int).Value() - before[k]; got != 1 {
			t.Fatalf("stat %s advanced by %d, want 1", k, got)
		}
	}
}

// Test_ParseRegisteredQueryParameters tests parameter parsing directly.
func Test_ParseRegisteredQueryParameters(t *testing.T) {
	tests := []struct {
		body string
		exp  func([]*command.Parameter) bool
		err  error
	}{
		{
			body: "",
			exp:  func(p []*command.Parameter) bool { return len(p) == 0 },
		},
		{
			body: `[]`,
			exp:  func(p []*command.Parameter) bool { return len(p) == 0 },
		},
		{
			body: `[1, "two", 3.5, true, null]`,
			exp: func(p []*command.Parameter) bool {
				return len(p) == 5 &&
					p[0].GetI() == 1 &&
					p[1].GetS() == "two" &&
					p[2].GetD() == 3.5 &&
					p[3].GetB() &&
					p[4].GetValue() == nil
			},
		},
		{
			body: `{"a": 1, "b": "two"}`,
			exp: func(p []*command.Parameter) bool {
				if len(p) != 2 {
					return false
				}
				names := map[string]bool{}
				for _, pp := range p {
					names[pp.GetName()] = true
				}
				return names["a"] && names["b"]
			},
		},
		{
			body: `"hello"`,
			err:  ErrInvalidRequest,
		},
		{
			body: `[1] junk`,
			err:  ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		params, err := parseRegisteredQueryParameters(strings.NewReader(tt.body))
		if tt.err != nil {
			if err != tt.err {
				t.Fatalf("body %q: expected error %v, got %v", tt.body, tt.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("body %q: unexpected error %v", tt.body, err)
		}
		if !tt.exp(params) {
			t.Fatalf("body %q: unexpected parameters %v", tt.body, params)
		}
	}
}
