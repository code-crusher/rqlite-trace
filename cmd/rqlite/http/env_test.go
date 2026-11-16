package http_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

const testHostEnvVar = "RQLITE_HOST"

func setEnv(t *testing.T, value string) {
	t.Helper()
	if value == "" {
		os.Unsetenv(testHostEnvVar)
	} else {
		os.Setenv(testHostEnvVar, value)
	}
	t.Cleanup(func() { os.Unsetenv(testHostEnvVar) })
}

func TestParseHostsEnv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		hasErr   bool
		expected []http.HostSpec
	}{
		{
			name:     "unset environment variable",
			envValue: "",
			hasErr:   true,
		},
		{
			name:     "single HTTPS host",
			envValue: "https://localhost:4002",
			expected: []http.HostSpec{{Protocol: "https", Host: "localhost", Port: 4002}},
		},
		{
			name:     "single host, no port",
			envValue: "http://localhost",
			expected: []http.HostSpec{{Protocol: "http", Host: "localhost"}},
		},
		{
			name:     "single host, no scheme",
			envValue: "//localhost",
			expected: []http.HostSpec{{Host: "localhost"}},
		},
		{
			name:     "comma-delimited set of URLs",
			envValue: "http://localhost:4001,https://foo.example.com:4002,//[2001:db8::1]:4003",
			expected: []http.HostSpec{
				{Protocol: "http", Host: "localhost", Port: 4001},
				{Protocol: "https", Host: "foo.example.com", Port: 4002},
				{Host: "2001:db8::1", Port: 4003},
			},
		},
		{
			name:     "whitespace around entries",
			envValue: " http://localhost:4001 , https://foo.example.com:4002 ",
			expected: []http.HostSpec{
				{Protocol: "http", Host: "localhost", Port: 4001},
				{Protocol: "https", Host: "foo.example.com", Port: 4002},
			},
		},
		{
			name:     "empty entries ignored",
			envValue: "http://localhost:4001,,",
			expected: []http.HostSpec{{Protocol: "http", Host: "localhost", Port: 4001}},
		},
		{
			name:     "invalid scheme",
			envValue: "ftp://localhost:4001",
			hasErr:   true,
		},
		{
			name:     "invalid URL",
			envValue: "http://[invalid:url",
			hasErr:   true,
		},
		{
			name:     "port out of range",
			envValue: "http://localhost:99999999",
			hasErr:   true,
		},
		{
			name:     "negative port",
			envValue: "http://localhost:-1",
			hasErr:   true,
		},
		{
			name:     "invalid port",
			envValue: "http://localhost:port",
			hasErr:   true,
		},
		{
			name:     "no hostname",
			envValue: "http://[invalid:host",
			hasErr:   true,
		},
		{
			name:     "invalid entry within set fails the whole parse",
			envValue: "http://localhost:4001,http://[bad:host",
			hasErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, tt.envValue)

			specs, err := http.ParseHostsEnv(testHostEnvVar)
			if tt.hasErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("expected no error, got %v", err)
				return
			}
			if len(specs) == 0 {
				t.Errorf("expected at least one host spec, got none")
				return
			}
			if !reflect.DeepEqual(specs, tt.expected) {
				t.Errorf("expected %+v, got %+v", tt.expected, specs)
			}
		})
	}
}
