package main

import (
	"reflect"
	"strings"
	"testing"
)

func mustForgeConfig(t *testing.T, args ...string) *Config {
	t.Helper()
	_, config, err := Forge(args)
	if err != nil {
		t.Fatalf("failed to forge config: %s", err.Error())
	}
	return config
}

func Test_HTTPAddrs_Single(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001", "/tmp/data")
	if err := c.Validate(); err != nil {
		t.Fatalf("failed to validate config: %s", err.Error())
	}
	if exp, got := []string{"localhost:4001"}, c.HTTPAddrs; !reflect.DeepEqual(exp, got) {
		t.Fatalf("incorrect HTTP bind addresses, exp %v, got %v", exp, got)
	}
	if exp, got := "localhost:4001", c.HTTPAdv; exp != got {
		t.Fatalf("incorrect advertised HTTP address, exp %s, got %s", exp, got)
	}
}

func Test_HTTPAddrs_Multiple(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001,100.64.0.1:4001",
		"-http-adv-addr", "100.64.0.1:4001", "/tmp/data")
	if err := c.Validate(); err != nil {
		t.Fatalf("failed to validate config: %s", err.Error())
	}
	exp := []string{"localhost:4001", "100.64.0.1:4001"}
	if !reflect.DeepEqual(exp, c.HTTPAddrs) {
		t.Fatalf("incorrect HTTP bind addresses, exp %v, got %v", exp, c.HTTPAddrs)
	}
	if exp, got := "100.64.0.1:4001", c.HTTPAdv; exp != got {
		t.Fatalf("incorrect advertised HTTP address, exp %s, got %s", exp, got)
	}
}

func Test_HTTPAddrs_MultipleWithWhitespace(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", " localhost:4001 , 100.64.0.1:4001 ",
		"-http-adv-addr", "100.64.0.1:4001", "/tmp/data")
	if err := c.Validate(); err != nil {
		t.Fatalf("failed to validate config: %s", err.Error())
	}
	exp := []string{"localhost:4001", "100.64.0.1:4001"}
	if !reflect.DeepEqual(exp, c.HTTPAddrs) {
		t.Fatalf("incorrect HTTP bind addresses, exp %v, got %v", exp, c.HTTPAddrs)
	}
}

func Test_HTTPAddrs_MultipleRequiresAdvAddr(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001,100.64.0.1:4001", "/tmp/data")
	err := c.Validate()
	if err == nil {
		t.Fatalf("expected error when advertised HTTP address not set")
	}
	if !strings.Contains(err.Error(), HTTPAdvAddrFlag) {
		t.Fatalf("expected error to mention -%s, got %s", HTTPAdvAddrFlag, err.Error())
	}
}

func Test_HTTPAddrs_InvalidAddress(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001,not-an-address",
		"-http-adv-addr", "localhost:4001", "/tmp/data")
	err := c.Validate()
	if err == nil {
		t.Fatalf("expected error for invalid HTTP bind address")
	}
	if !strings.Contains(err.Error(), "HTTP bind address not valid") {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

func Test_HTTPAddrs_Empty(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001,", "/tmp/data")
	err := c.Validate()
	if err == nil {
		t.Fatalf("expected error for empty HTTP bind address")
	}
	if !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

func Test_HTTPAddrs_CannotMatchRaftAddr(t *testing.T) {
	c := mustForgeConfig(t, "-http-addr", "localhost:4001,localhost:4002", "/tmp/data")
	err := c.Validate()
	if err == nil {
		t.Fatalf("expected error when HTTP bind address matches Raft address")
	}
	if !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}
