package notify

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_NewConfig_URL(t *testing.T) {
	for _, endpoint := range []string{
		"http://myapp:8080/leader_changes",
		"https://myapp:8443/leader_changes",
	} {
		cfg, err := NewConfig(endpoint)
		if err != nil {
			t.Fatalf("failed to create config from URL %s: %s", endpoint, err.Error())
		}
		if cfg.Endpoint != endpoint {
			t.Fatalf("expected endpoint %s, got %s", endpoint, cfg.Endpoint)
		}
		if cfg.EndpointTimeout != DefaultEndpointTimeout {
			t.Fatalf("expected default timeout, got %s", cfg.EndpointTimeout)
		}
	}
}

func Test_NewConfig_BadURL(t *testing.T) {
	if _, err := NewConfig("ftp://foo/bar"); err == nil {
		t.Fatalf("expected error for unsupported scheme, got none")
	}
}

func Test_NewConfig_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.json")
	if err := os.WriteFile(path, []byte(`{
		"endpoint": "http://myapp:8080/leader_changes",
		"endpoint_timeout": 10000000000
	}`), 0o644); err != nil {
		t.Fatalf("failed to write config file: %s", err.Error())
	}

	cfg, err := NewConfig(path)
	if err != nil {
		t.Fatalf("failed to create config from file: %s", err.Error())
	}
	if cfg.Endpoint != "http://myapp:8080/leader_changes" {
		t.Fatalf("unexpected endpoint %s", cfg.Endpoint)
	}
	if cfg.EndpointTimeout != 10*time.Second {
		t.Fatalf("unexpected timeout %s", cfg.EndpointTimeout)
	}
}

func Test_NewConfig_FileNoEndpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("failed to write config file: %s", err.Error())
	}
	if _, err := NewConfig(path); err != ErrNoEndpoint {
		t.Fatalf("expected %s, got %v", ErrNoEndpoint, err)
	}
}

func Test_NewConfig_FileBadTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.json")
	if err := os.WriteFile(path, []byte(`{
		"endpoint": "http://myapp:8080/leader_changes",
		"endpoint_timeout": "not-a-number"
	}`), 0o644); err != nil {
		t.Fatalf("failed to write config file: %s", err.Error())
	}
	if _, err := NewConfig(path); err == nil {
		t.Fatalf("expected error for bad duration, got none")
	}
}

func Test_TLSConfig_Nil(t *testing.T) {
	cfg := DefaultConfig()
	tlsConfig, err := cfg.TLSConfig()
	if err != nil {
		t.Fatalf("expected no error, got %s", err.Error())
	}
	if tlsConfig != nil {
		t.Fatalf("expected nil TLS config, got %v", tlsConfig)
	}
}
