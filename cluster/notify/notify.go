package notify

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	// ErrNoEndpoint is returned when no endpoint is set in the configuration.
	ErrNoEndpoint = errors.New("notify: no endpoint set")

	// ErrNoConfig is returned when no configuration is passed to the Service.
	ErrNoConfig = errors.New("notify: no configuration set")

	// ErrAlreadyStarted is returned when the Service is already started.
	ErrAlreadyStarted = errors.New("notify: service already started")
)

// Payload is the JSON payload POSTed to the notification endpoint whenever
// the leadership status of this node changes.
type Payload struct {
	// NodeID is the ID of the node whose leadership status changed.
	NodeID string `json:"node_id"`

	// Leader is true if the node is now the Leader of the cluster.
	Leader bool `json:"leader"`

	// Timestamp is the time at which the change was observed by this node.
	Timestamp time.Time `json:"timestamp"`
}

// makePayload creates the JSON payload for a leadership-change notification.
func makePayload(nodeID string, leader bool) ([]byte, error) {
	return json.Marshal(Payload{
		NodeID:    nodeID,
		Leader:    leader,
		Timestamp: time.Now().UTC(),
	})
}
