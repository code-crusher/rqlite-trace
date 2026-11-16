package http

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	minPortNumber = 0
	maxPortNumber = 65535
)

// HostSpec is a single host URL parsed from an environment variable such as
// RQLITE_HOST.
type HostSpec struct {
	Protocol string
	Host     string
	Port     uint16
}

// ParseHostsEnv parses a comma-delimited set of host URLs from the environment
// variable named varName. Each URL has the form [scheme:]//host[:port], and at
// least one URL must be supplied. The first URL addresses the primary host; any
// subsequent URLs can be used as failover alternatives.
func ParseHostsEnv(varName string) ([]HostSpec, error) {
	hostEnv, found := os.LookupEnv(varName)
	if !found {
		return nil, fmt.Errorf("%s environment variable not set", varName)
	}

	var specs []HostSpec
	for _, entry := range strings.Split(hostEnv, ",") {
		if entry = strings.TrimSpace(entry); entry == "" {
			continue
		}
		spec, err := parseHostURL(entry)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", varName, err)
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("%s does not contain any host URLs", varName)
	}
	return specs, nil
}

// parseHostURL parses a single [scheme:]//host[:port] URL.
func parseHostURL(u string) (HostSpec, error) {
	uri, err := url.Parse(u)
	if err != nil {
		return HostSpec{}, err
	}

	if uri.Scheme != "http" && uri.Scheme != "https" && uri.Scheme != "" {
		return HostSpec{}, fmt.Errorf("unsupported protocol %s", uri.Scheme)
	}

	if uri.Hostname() == "" {
		return HostSpec{}, fmt.Errorf("no hostname in '%s'", u)
	}
	spec := HostSpec{Protocol: uri.Scheme, Host: uri.Hostname()}

	if uri.Port() == "" {
		return spec, nil
	}

	p, err := strconv.Atoi(uri.Port())
	if err != nil {
		return HostSpec{}, fmt.Errorf("port '%s' is not a valid number: %w", uri.Port(), err)
	}
	if p < minPortNumber || p > maxPortNumber {
		return HostSpec{}, fmt.Errorf("port number %d is outside of valid range [%d, %d]",
			p, minPortNumber, maxPortNumber)
	}
	spec.Port = uint16(p)
	return spec, nil
}
