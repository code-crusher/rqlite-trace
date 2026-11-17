package system

import (
	"testing"
)

// Test_SingleNodeRegisteredQueries tests that a query can be registered, read
// back, invoked with parameters, listed, and deregistered on a single node.
func Test_SingleNodeRegisteredQueries(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	if _, err := node.Execute(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	if _, err := node.ExecuteMulti([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "declan")`,
	}); err != nil {
		t.Fatalf("failed to insert data: %s", err)
	}

	// An empty registry reads as empty.
	qs, err := node.RegisteredQueries()
	if err != nil {
		t.Fatalf("failed to list empty registry: %s", err)
	}
	if len(qs) != 0 {
		t.Fatalf("expected empty registry, got %v", qs)
	}

	// Register two queries in one request.
	qs, err = node.RegisterQueries([]string{
		`SELECT name FROM foo WHERE id = :id`,
		`SELECT COUNT(*) FROM foo`,
	})
	if err != nil {
		t.Fatalf("failed to register queries: %s", err)
	}
	if len(qs) != 2 {
		t.Fatalf("expected 2 registered queries, got %d", len(qs))
	}
	if qs[0].ID == "" || qs[0].ID == qs[1].ID {
		t.Fatalf("expected distinct non-empty IDs, got %v", qs)
	}
	if qs[0].SQL != `SELECT name FROM foo WHERE id = :id` || qs[1].SQL != `SELECT COUNT(*) FROM foo` {
		t.Fatalf("unexpected registered queries: %v", qs)
	}

	// The registry now holds them.
	listed, err := node.RegisteredQueries()
	if err != nil {
		t.Fatalf("failed to list registry: %s", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 registered queries, got %d", len(listed))
	}

	// Invoke the parameterized query with a named parameter.
	body, err := node.InvokeRegisteredQuery(qs[0].ID, map[string]any{"id": 2})
	if err != nil {
		t.Fatalf("failed to invoke registered query: %s", err)
	}
	if exp, got := `{"results":[{"columns":["name"],"types":["text"],"values":[["declan"]]}]}`, body; exp != got {
		t.Fatalf("unexpected invoke result\nexp: %s\ngot: %s", exp, got)
	}

	// Invoke the same query with a positional parameter.
	body, err = node.InvokeRegisteredQuery(qs[0].ID, []any{1})
	if err != nil {
		t.Fatalf("failed to invoke registered query: %s", err)
	}
	if exp, got := `{"results":[{"columns":["name"],"types":["text"],"values":[["fiona"]]}]}`, body; exp != got {
		t.Fatalf("unexpected invoke result\nexp: %s\ngot: %s", exp, got)
	}

	// Invoke the query which takes no parameters.
	body, err = node.InvokeRegisteredQuery(qs[1].ID, nil)
	if err != nil {
		t.Fatalf("failed to invoke registered query: %s", err)
	}
	if exp, got := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]}`, body; exp != got {
		t.Fatalf("unexpected invoke result\nexp: %s\ngot: %s", exp, got)
	}

	// A registered query may modify the database, in which case the change is
	// applied through Raft like any other write.
	writes, err := node.RegisterQueries([]string{`INSERT INTO foo(id, name) VALUES(?, ?)`})
	if err != nil {
		t.Fatalf("failed to register write query: %s", err)
	}
	body, err = node.InvokeRegisteredQuery(writes[0].ID, []any{3, "shane"})
	if err != nil {
		t.Fatalf("failed to invoke write query: %s", err)
	}
	if exp, got := `{"results":[{"last_insert_id":3,"rows_affected":1}]}`, body; exp != got {
		t.Fatalf("unexpected write result\nexp: %s\ngot: %s", exp, got)
	}
	if resp, err := node.Query(`SELECT name FROM foo WHERE id = 3`); err != nil {
		t.Fatalf("failed to query written row: %s", err)
	} else if exp := `{"results":[{"columns":["name"],"types":["text"],"values":[["shane"]]}]}`; exp != resp {
		t.Fatalf("write query did not insert row\nexp: %s\ngot: %s", exp, resp)
	}
	if err := node.DeregisterQuery(writes[0].ID); err != nil {
		t.Fatalf("failed to deregister write query: %s", err)
	}

	// Deregistering a query removes it, and invoking it is then not found.
	if err := node.DeregisterQuery(qs[0].ID); err != nil {
		t.Fatalf("failed to deregister query: %s", err)
	}
	listed, err = node.RegisteredQueries()
	if err != nil {
		t.Fatalf("failed to list registry: %s", err)
	}
	if len(listed) != 1 || listed[0].ID != qs[1].ID {
		t.Fatalf("unexpected registry after deregistration: %v", listed)
	}
	if _, err := node.InvokeRegisteredQuery(qs[0].ID, []any{1}); err == nil {
		t.Fatalf("expected error invoking deregistered query")
	}
}

// Test_MultiNodeRegisteredQueries tests that a registered query is written
// through Raft, so every node in the cluster learns of it and can serve it.
func Test_MultiNodeRegisteredQueries(t *testing.T) {
	node1 := mustNewLeaderNode("node1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err)
	}
	if _, err := node2.WaitForLeader(); err != nil {
		t.Fatalf("failed waiting for leader: %s", err)
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err)
	}
	if _, err := node3.WaitForLeader(); err != nil {
		t.Fatalf("failed waiting for leader: %s", err)
	}

	c := Cluster{node1, node2, node3}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err)
	}

	if _, err := leader.Execute(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	if _, err := leader.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
		t.Fatalf("failed to insert data: %s", err)
	}

	qs, err := leader.RegisterQueries([]string{`SELECT name FROM foo WHERE id = ?`})
	if err != nil {
		t.Fatalf("failed to register query: %s", err)
	}
	if len(qs) != 1 {
		t.Fatalf("expected 1 registered query, got %d", len(qs))
	}

	// Every node holds the registration, since it was written through Raft.
	for _, node := range c {
		listed, err := node.RegisteredQueries()
		if err != nil {
			t.Fatalf("failed to list registry on node %s: %s", node.ID, err)
		}
		if len(listed) != 1 || listed[0].ID != qs[0].ID || listed[0].SQL != `SELECT name FROM foo WHERE id = ?` {
			t.Fatalf("node %s has unexpected registry: %v", node.ID, listed)
		}
	}

	// A follower forwards the invocation to the leader, which serves it.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err)
	}
	if len(followers) != 2 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}
	body, err := followers[0].InvokeRegisteredQuery(qs[0].ID, []any{1})
	if err != nil {
		t.Fatalf("failed to invoke registered query via follower: %s", err)
	}
	if exp, got := `{"results":[{"columns":["name"],"types":["text"],"values":[["fiona"]]}]}`, body; exp != got {
		t.Fatalf("unexpected invoke result\nexp: %s\ngot: %s", exp, got)
	}

	// Deregistering through the leader removes the query on every node.
	if err := leader.DeregisterQuery(qs[0].ID); err != nil {
		t.Fatalf("failed to deregister query: %s", err)
	}
	for _, node := range c {
		listed, err := node.RegisteredQueries()
		if err != nil {
			t.Fatalf("failed to list registry on node %s: %s", node.ID, err)
		}
		if len(listed) != 0 {
			t.Fatalf("node %s has unexpected registry after deregistration: %v", node.ID, listed)
		}
	}
}
