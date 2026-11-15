// Package notify implements notifications for Raft-related events, such as
// leadership changes. When the leadership status of the node running the
// service changes, a small JSON payload is POSTed to a configured HTTP(S)
// endpoint. This allows external systems to learn about, and react to,
// leadership changes without needing to poll the cluster. See DESIGN.md for
// details.
package notify

import (
	"bytes"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	httpurl "github.com/rqlite/rqlite/v10/http/url"
	"github.com/rqlite/rqlite/v10/internal/rsync"
)

// Cluster is the interface cluster-management systems must implement.
type Cluster interface {
	// RegisterLeaderChange registers the given channel which will receive
	// a boolean when the leader changes. The boolean indicates whether this
	// node is the new leader or not.
	RegisterLeaderChange(c chan<- bool)
}

var (
	// stats captures stats for the notification Service.
	stats *expvar.Map
)

const (
	numNotificationsTxOK     = "num_notifications_tx_ok"
	numNotificationsTxFailed = "num_notifications_tx_failed"

	leaderChanLen = 5 // Support any fast back-to-back leadership changes.
)

func init() {
	stats = expvar.NewMap("notify")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numNotificationsTxOK, 0)
	stats.Add(numNotificationsTxFailed, 0)
}

// Service is a notification Service. It notifies a configured endpoint about
// Raft-related events, such as leadership changes.
type Service struct {
	nodeID string
	clstr  Cluster
	cfg    *Config

	// httpClient is the client used to POST notifications to the endpoint.
	httpClient *http.Client

	// leaderObCh receives leadership-change notifications from the cluster.
	leaderObCh chan bool

	// isLeader tracks the last-known leadership state of this node.
	isLeader rsync.AtomicBool

	// For Service shutdown.
	done    chan struct{}
	started rsync.AtomicBool
	wg      sync.WaitGroup

	logger *log.Logger
}

// NewService creates a new notification Service, which will notify the
// endpoint, as given by the configuration, when leadership changes occur.
func NewService(nodeID string, clstr Cluster, cfg *Config) (*Service, error) {
	if cfg == nil {
		return nil, ErrNoConfig
	}
	tlsConfig, err := cfg.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	return &Service{
		nodeID:     nodeID,
		clstr:      clstr,
		cfg:        cfg,
		httpClient: &http.Client{Timeout: cfg.EndpointTimeout, Transport: &http.Transport{TLSClientConfig: tlsConfig}},
		leaderObCh: make(chan bool, leaderChanLen),
		done:       make(chan struct{}),
		logger:     log.New(os.Stderr, "[notify] ", log.LstdFlags),
	}, nil
}

// Start starts the notification Service.
func (s *Service) Start() error {
	if s.started.Is() {
		return ErrAlreadyStarted
	}
	s.clstr.RegisterLeaderChange(s.leaderObCh)
	s.wg.Add(1)
	go s.main()
	s.started.SetBool(true)
	s.logger.Printf("service started, notifying %s on leadership changes", s.sanitizedEndpoint())
	return nil
}

// Stop stops the notification Service.
func (s *Service) Stop() {
	if s.started.IsNot() {
		return
	}
	close(s.done)
	s.wg.Wait()
	s.httpClient.CloseIdleConnections()
	s.started.Unset()
	s.logger.Printf("service stopped")
}

// Stats returns statistics about the notification Service.
func (s *Service) Stats() (map[string]any, error) {
	return map[string]any{
		"node_id":    s.nodeID,
		"endpoint":   s.sanitizedEndpoint(),
		"is_leader":  s.isLeader.Is(),
		"started":    s.started.Is(),
		"num_tx_ok":  stats.Get(numNotificationsTxOK).String(),
		"num_tx_err": stats.Get(numNotificationsTxFailed).String(),
	}, nil
}

// main is the main loop of the notification Service.
func (s *Service) main() {
	defer s.wg.Done()
	for {
		select {
		case leaderNow := <-s.leaderObCh:
			s.isLeader.SetBool(leaderNow)
			if err := s.notify(leaderNow); err != nil {
				stats.Add(numNotificationsTxFailed, 1)
				s.logger.Printf("failed to notify leadership change to %s: %s",
					s.sanitizedEndpoint(), err.Error())
			} else {
				stats.Add(numNotificationsTxOK, 1)
			}

		case <-s.done:
			return
		}
	}
}

// notify sends a leadership-change notification to the endpoint.
func (s *Service) notify(leader bool) error {
	payload, err := makePayload(s.nodeID, leader)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "rqlite-notify/"+s.nodeID)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}
	return nil
}

// SetLeader allows tests to inject a leadership-change event. It is not
// recommended to call this method outside of tests.
func (s *Service) SetLeader(isLeader bool) {
	s.leaderObCh <- isLeader
}

// sanitizedEndpoint returns the endpoint with any basic-auth credentials
// removed, so it can be safely logged.
func (s *Service) sanitizedEndpoint() string {
	return httpurl.RemoveBasicAuth(s.cfg.Endpoint)
}
