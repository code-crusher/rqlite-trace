package notify

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// mockCluster is a Cluster which simply records registered leader-change channels.
type mockCluster struct {
	mu             sync.Mutex
	chanRegistered bool
}

func (m *mockCluster) RegisterLeaderChange(c chan<- bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chanRegistered = true
}

func testPoll(t *testing.T, fn func() bool, dur time.Duration) bool {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(dur)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if fn() {
				return true
			}
		case <-timer.C:
			return fn()
		}
	}
}

func Test_ServiceNotify(t *testing.T) {
	ResetStats()

	// Endpoint which records payloads.
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

	cl := &mockCluster{}
	cfg := DefaultConfig()
	cfg.Endpoint = srv.URL

	s, err := NewService("node1", cl, cfg)
	if err != nil {
		t.Fatalf("failed to create service: %s", err.Error())
	}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	defer s.Stop()

	if !cl.chanRegistered {
		t.Fatalf("service did not register leader-change channel with cluster")
	}

	// Become leader.
	s.SetLeader(true)
	if !testPoll(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads) == 1
	}, 5*time.Second) {
		t.Fatalf("timeout waiting for first notification")
	}
	mu.Lock()
	p := payloads[0]
	mu.Unlock()
	if p.NodeID != "node1" {
		t.Fatalf("unexpected node ID %s", p.NodeID)
	}
	if !p.Leader {
		t.Fatalf("expected leader=true")
	}
	if p.Timestamp.IsZero() {
		t.Fatalf("expected non-zero timestamp")
	}

	// No longer leader.
	s.SetLeader(false)
	if !testPoll(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads) == 2
	}, 5*time.Second) {
		t.Fatalf("timeout waiting for second notification")
	}
	mu.Lock()
	p = payloads[1]
	mu.Unlock()
	if p.Leader {
		t.Fatalf("expected leader=false")
	}

	// Stats should reflect successful transmissions.
	if exp, got := "2", stats.Get(numNotificationsTxOK).String(); exp != got {
		t.Fatalf("expected %s successful transmissions, got %s", exp, got)
	}
}

func Test_ServiceNotifyEndpointFail(t *testing.T) {
	ResetStats()

	srv := newFailingTestServer()
	defer srv.Close()

	cl := &mockCluster{}
	cfg := DefaultConfig()
	cfg.Endpoint = srv.URL

	s, err := NewService("node1", cl, cfg)
	if err != nil {
		t.Fatalf("failed to create service: %s", err.Error())
	}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	defer s.Stop()

	s.SetLeader(true)
	if !testPoll(t, func() bool {
		return stats.Get(numNotificationsTxFailed).String() != "0"
	}, 5*time.Second) {
		t.Fatalf("timeout waiting for failed transmission to be recorded")
	}
}

func Test_ServiceNoConfig(t *testing.T) {
	if _, err := NewService("node1", &mockCluster{}, nil); err != ErrNoConfig {
		t.Fatalf("expected %s, got %v", ErrNoConfig, err)
	}
}

func Test_ServiceDoubleStart(t *testing.T) {
	srv := newTestServer(nil)
	defer srv.Close()
	cfg := DefaultConfig()
	cfg.Endpoint = srv.URL
	s, err := NewService("node1", &mockCluster{}, cfg)
	if err != nil {
		t.Fatalf("failed to create service: %s", err.Error())
	}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	defer s.Stop()
	if err := s.Start(); err != ErrAlreadyStarted {
		t.Fatalf("expected %s, got %v", ErrAlreadyStarted, err)
	}
}
