package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppBinaryPath_Default(t *testing.T) {
	os.Unsetenv("FUNCTION_APP_NAME")
	path := appBinaryPath("/home/site/wwwroot")
	expected := filepath.Join("/home/site/wwwroot", "app")
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestAppBinaryPath_CustomName(t *testing.T) {
	os.Setenv("FUNCTION_APP_NAME", "myservice")
	defer os.Unsetenv("FUNCTION_APP_NAME")

	path := appBinaryPath("/home/site/wwwroot")
	expected := filepath.Join("/home/site/wwwroot", "myservice")
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestAppBinaryPath_PicksUpSetenv(t *testing.T) {
	// Simulate FERR setting FUNCTION_APP_NAME via os.Setenv
	os.Unsetenv("FUNCTION_APP_NAME")

	// Before FERR: default name
	path := appBinaryPath("/app")
	expected := filepath.Join("/app", "app")
	if path != expected {
		t.Errorf("before setenv: expected %s, got %s", expected, path)
	}

	// Simulate FERR applying env vars
	os.Setenv("FUNCTION_APP_NAME", "custom")
	defer os.Unsetenv("FUNCTION_APP_NAME")

	// After FERR: custom name
	path = appBinaryPath("/app")
	expected = filepath.Join("/app", "custom")
	if path != expected {
		t.Errorf("after setenv: expected %s, got %s", expected, path)
	}
}

func TestSetenvOverridesExisting(t *testing.T) {
	os.Setenv("WEBSITE_PLACEHOLDER_MODE", "1")
	defer os.Unsetenv("WEBSITE_PLACEHOLDER_MODE")

	if os.Getenv("WEBSITE_PLACEHOLDER_MODE") != "1" {
		t.Fatal("expected 1")
	}

	// Simulate FERR override
	os.Setenv("WEBSITE_PLACEHOLDER_MODE", "0")

	if os.Getenv("WEBSITE_PLACEHOLDER_MODE") != "0" {
		t.Fatal("expected 0 after override")
	}

	// Verify os.Environ() doesn't have duplicates
	count := 0
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "WEBSITE_PLACEHOLDER_MODE=") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 WEBSITE_PLACEHOLDER_MODE entry, got %d", count)
	}
}
