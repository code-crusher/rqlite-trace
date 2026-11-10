package cdc

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	httpurl "github.com/rqlite/rqlite/v10/http/url"
)

// Sink defines the interface that all CDC sinks must implement.
type Sink interface {
	io.WriteCloser
	fmt.Stringer
}

// SinkConfig holds the configuration for creating a sink.
type SinkConfig struct {
	Endpoint        string
	TLSConfig       *tls.Config
	TransmitTimeout time.Duration
}

// StdoutSink implements Sink by writing to os.Stdout.
type StdoutSink struct{}

// NewStdoutSink creates a new StdoutSink.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

// Write writes the data to stdout.
func (d *StdoutSink) Write(p []byte) (n int, err error) {
	n, err = fmt.Println(string(p))
	if err != nil {
		return 0, err
	}
	// fmt.Println adds a newline, but we return the original length
	return len(p), nil
}

// Close releases resources (none for stdout).
func (d *StdoutSink) Close() error {
	return nil
}

func (d *StdoutSink) String() string {
	return "stdout"
}

// HTTPSink implements Sink by POSTing batches to the configured endpoint.
type HTTPSink struct {
	endpoint   string
	httpClient *http.Client
}

// NewHTTPSink creates a new HTTPSink.
func NewHTTPSink(endpoint string, tlsConfig *tls.Config, timeout time.Duration) *HTTPSink {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		CheckRedirect: checkRedirectKeepsPost,
		Timeout:       timeout,
	}

	return &HTTPSink{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
}

// checkRedirectKeepsPost is an http.Client redirect policy which refuses any
// redirect that would not re-send the batch as a POST. A 301, 302 or 303
// converts a POST into a GET with no body, so a 2xx from the redirect target
// would acknowledge a batch it never received. A 307 or 308 preserves the
// method and body, so those are followed, up to the standard limit.
func checkRedirectKeepsPost(req *http.Request, via []*http.Request) error {
	if req.Method != http.MethodPost {
		return fmt.Errorf("refusing redirect to %s: POST would become %s and the payload would be dropped",
			httpurl.RemoveBasicAuth(req.URL.String()), req.Method)
	}
	if len(via) >= 10 {
		return fmt.Errorf("stopped after %d redirects", len(via))
	}
	return nil
}

// Write writes the data to the HTTP endpoint.
func (d *HTTPSink) Write(p []byte) (n int, err error) {
	req, err := http.NewRequest("POST", d.endpoint, bytes.NewReader(p))
	if err != nil {
		return 0, fmt.Errorf("error creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return 0, fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}
	return len(p), nil
}

// Close releases resources held by the HTTP client.
func (d *HTTPSink) Close() error {
	if d.httpClient != nil {
		d.httpClient.CloseIdleConnections()
	}
	return nil
}

func (d *HTTPSink) String() string {
	return httpurl.RemoveBasicAuth(d.endpoint)
}

// NewSink creates a new Sink based on the provided configuration.
func NewSink(cfg SinkConfig) (Sink, error) {
	if strings.EqualFold(cfg.Endpoint, "stdout") {
		return NewStdoutSink(), nil
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	switch u.Scheme {
	case "http", "https":
		return NewHTTPSink(cfg.Endpoint, cfg.TLSConfig, cfg.TransmitTimeout), nil
	default:
		return nil, fmt.Errorf("cdc: unsupported scheme %q", u.Scheme)
	}
}

// MultiSink implements Sink by writing to a set of sinks. It is used to
// deliver CDC events to more than one endpoint.
type MultiSink struct {
	sinks []Sink
}

// NewMultiSink creates a new MultiSink which writes to each of the given sinks.
func NewMultiSink(sinks ...Sink) *MultiSink {
	return &MultiSink{sinks: sinks}
}

// Write writes the data to every sink. Every sink is attempted, even if an
// earlier one fails, so that a single failing endpoint does not hold up
// delivery to the others. An error is returned if any sink fails, which
// causes the batch to be retried. A retry writes to every sink again, so
// sinks which already accepted the batch may see it more than once; this is
// consistent with the at-least-once delivery guarantee.
func (m *MultiSink) Write(p []byte) (n int, err error) {
	var errs []error
	for _, sink := range m.sinks {
		if _, err := sink.Write(p); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", sink.String(), err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close releases resources held by all sinks.
func (m *MultiSink) Close() error {
	var errs []error
	for _, sink := range m.sinks {
		if err := sink.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *MultiSink) String() string {
	ss := make([]string, len(m.sinks))
	for i, sink := range m.sinks {
		ss[i] = sink.String()
	}
	return strings.Join(ss, ", ")
}

// newSinkFromConfig creates a Sink which delivers to every endpoint configured
// in cfg. If more than one endpoint is configured, the returned Sink is a
// MultiSink.
func newSinkFromConfig(cfg *Config, tlsConfig *tls.Config) (Sink, error) {
	endpoints := cfg.allEndpoints()
	if len(endpoints) == 0 {
		return nil, errors.New("no CDC endpoint configured")
	}

	sinks := make([]Sink, 0, len(endpoints))
	for _, endpoint := range endpoints {
		sink, err := NewSink(SinkConfig{
			Endpoint:        endpoint,
			TLSConfig:       tlsConfig,
			TransmitTimeout: cfg.TransmitTimeout,
		})
		if err != nil {
			for _, s := range sinks {
				s.Close()
			}
			return nil, fmt.Errorf("failed to create sink for endpoint %s: %w", endpoint, err)
		}
		sinks = append(sinks, sink)
	}

	if len(sinks) == 1 {
		return sinks[0], nil
	}
	return NewMultiSink(sinks...), nil
}
