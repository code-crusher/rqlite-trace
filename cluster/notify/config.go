package notify

import (
	"crypto/tls"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rtls"
)

const (
	// DefaultEndpointTimeout is the default timeout for transmitting a
	// notification to the endpoint.
	DefaultEndpointTimeout = 5 * time.Second
)

// TLSConfiguration holds the TLS configuration for the notification service.
type TLSConfiguration struct {
	// CACertFile is the path to the CA certificate file for TLS verification.
	CACertFile string `json:"ca_cert_file,omitempty"`

	// CertFile is the path to the client certificate file for mutual TLS.
	CertFile string `json:"cert_file,omitempty"`

	// KeyFile is the path to the client private key file.
	KeyFile string `json:"key_file,omitempty"`

	// InsecureSkipVerify controls whether the client verifies the server's
	// certificate. If true, TLS accepts any certificate presented by the server.
	InsecureSkipVerify bool `json:"insecure_skip_verify,omitempty"`

	// ServerName is used to verify the hostname on the returned certificates.
	// If empty, the hostname used for the connection is used.
	ServerName string `json:"server_name,omitempty"`
}

// Config holds the configuration for the notification service.
type Config struct {
	// Endpoint is the HTTP(S) endpoint to which leadership-change notifications
	// are posted. It must be specified.
	Endpoint string `json:"endpoint"`

	// TLS configuration for communicating with the endpoint.
	TLS *TLSConfiguration `json:"tls,omitempty"`

	// EndpointTimeout is the timeout for transmitting a notification to the
	// endpoint. If it is zero or unspecified, the default of
	// DefaultEndpointTimeout is used.
	EndpointTimeout time.Duration `json:"endpoint_timeout"`
}

// DefaultConfig returns the default configuration for the notification service.
func DefaultConfig() *Config {
	return &Config{
		EndpointTimeout: DefaultEndpointTimeout,
	}
}

// TLSConfig creates a *tls.Config from the individual TLS configuration fields.
// This uses the same TLS utilities as other parts of rqlite.
func (c *Config) TLSConfig() (*tls.Config, error) {
	if c.TLS == nil {
		return nil, nil
	}
	if c.TLS.CACertFile == "" && c.TLS.CertFile == "" && c.TLS.KeyFile == "" &&
		!c.TLS.InsecureSkipVerify && c.TLS.ServerName == "" {
		return nil, nil
	}
	return rtls.CreateClientConfig(c.TLS.CertFile, c.TLS.KeyFile, c.TLS.CACertFile,
		c.TLS.ServerName, c.TLS.InsecureSkipVerify)
}

// NewConfig creates a new Config from a string. If the string is a valid
// http:// or https:// URL, it creates a default config with the endpoint set
// to that URL. Otherwise, it treats the string as a file path and attempts to
// read and parse a JSON configuration file.
func NewConfig(s string) (*Config, error) {
	if _, err := url.Parse(s); err == nil &&
		(strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")) {
		config := DefaultConfig()
		config.Endpoint = s
		return config, nil
	}

	// Not a URL, treat as file path.
	data, err := os.ReadFile(s)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	if config.Endpoint == "" {
		return nil, ErrNoEndpoint
	}
	if config.EndpointTimeout <= 0 {
		config.EndpointTimeout = DefaultEndpointTimeout
	}
	return &config, nil
}
