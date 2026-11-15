package notify

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/store"
)

// mockLayer is a store.Layer over a plain TCP listener.
type mockLayer struct {
	ln net.Listener
}

func (m *mockLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockLayer) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockLayer) Close() error { return m.ln.Close() }

func (m *mockLayer) Addr() net.Addr { return m.ln.Addr() }

// Test_ServiceStoreIntegration tests the notification Service end-to-end with
// a real single-node store: when the store becomes Leader, a notification is
// sent to the configured HTTP endpoint.
func Test_ServiceStoreIntegration(t *testing.T) {
	ResetStats()

	var mu sync.Mutex
	var payloads []Payload
	srv := newTestServer(func(b []byte) {
		var p Payload
		if err := json.Unmarshal(b, &p); err != nil {
			t.Errorf("failed to unmarshal payload: %s", err.Error())
			return
		}
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
	})
	defer srv.Close()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %s", err.Error())
	}
	ly := &mockLayer{ln: ln}

	s := store.New(&store.Config{
		DBConf: store.NewDBConfig(),
		Dir:    t.TempDir(),
		ID:     ln.Addr().String(),
	}, ly)
	if s == nil {
		t.Fatalf("failed to create store")
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	cfg := DefaultConfig()
	cfg.Endpoint = srv.URL
	svc, err := NewService(s.ID(), s, cfg)
	if err != nil {
		t.Fatalf("failed to create service: %s", err.Error())
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	defer svc.Stop()

	// Bootstrap the single-node cluster, which will make this node Leader.
	if err := s.Bootstrap(store.NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	if !testPoll(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads) == 1
	}, 10*time.Second) {
		t.Fatalf("timeout waiting for leadership-change notification")
	}

	mu.Lock()
	p := payloads[0]
	mu.Unlock()
	if p.NodeID != s.ID() {
		t.Fatalf("unexpected node ID %s, expected %s", p.NodeID, s.ID())
	}
	if !p.Leader {
		t.Fatalf("expected leader=true")
	}
	if p.Timestamp.IsZero() {
		t.Fatalf("expected non-zero timestamp")
	}
}
