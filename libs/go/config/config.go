package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Common struct {
	SiteID, InstanceID  string
	HTTPPort, AdminPort int
	LogLevel            string
	ShutdownTimeout     time.Duration
	PipelineMode        string
}

// Lookup reads a setting using the file override when present.
func Lookup(name, fallback string) (string, error) {
	if path, ok := os.LookupEnv(name + "_FILE"); ok {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: cannot read setting", name)
		}
		return strings.TrimRight(string(data), "\r\n"), nil
	}
	if value, ok := os.LookupEnv(name); ok {
		return value, nil
	}
	return fallback, nil
}

// Env panics on unreadable configuration. Use Lookup to handle errors.
func Env(name, def string) string {
	value, err := Lookup(name, def)
	if err != nil {
		panic(err)
	}
	return value
}

// MustEnv requires a nonempty value.
func MustEnv(name string) string {
	value := Env(name, "")
	if value == "" {
		panic(name + " is required")
	}
	return value
}

func LoadCommon() (Common, error) {
	var cfg Common
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	settings := []struct {
		name, fallback string
		dest           *string
	}{
		{"SITE_ID", "", &cfg.SiteID},
		{"INSTANCE_ID", hostname, &cfg.InstanceID},
		{"LOG_LEVEL", "info", &cfg.LogLevel},
		{"PIPELINE_MODE", "jetstream", &cfg.PipelineMode},
	}
	for _, setting := range settings {
		*setting.dest, err = Lookup(setting.name, setting.fallback)
		if err != nil {
			return Common{}, err
		}
	}
	if cfg.SiteID == "" {
		return Common{}, errors.New("SITE_ID is required")
	}
	if cfg.InstanceID == "" {
		return Common{}, errors.New("INSTANCE_ID is empty and no hostname is available")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return Common{}, errors.New("invalid LOG_LEVEL")
	}
	if cfg.PipelineMode != "jetstream" && cfg.PipelineMode != "direct-db" {
		return Common{}, errors.New("invalid PIPELINE_MODE")
	}

	for _, port := range []struct {
		name, fallback string
		dest           *int
	}{
		{"HTTP_PORT", "8080", &cfg.HTTPPort},
		{"ADMIN_PORT", "9090", &cfg.AdminPort},
	} {
		value, err := Lookup(port.name, port.fallback)
		if err != nil {
			return Common{}, err
		}
		number, err := parseScaled(value, 1, 65535)
		if err != nil || number == 0 {
			return Common{}, fmt.Errorf("%s must be between 1 and 65535", port.name)
		}
		*port.dest = int(number)
	}

	timeout, err := Lookup("SHUTDOWN_TIMEOUT_SECONDS", "25")
	if err != nil {
		return Common{}, err
	}
	cfg.ShutdownTimeout, err = ParseDuration("SHUTDOWN_TIMEOUT_SECONDS", timeout)
	if err != nil {
		return Common{}, err
	}
	return cfg, nil
}

func parseScaled(value string, scale, limit int64) (int64, error) {
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, errors.New("expected decimal digits")
		}
	}
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number > limit/scale {
		return 0, errors.New("invalid or out-of-range integer")
	}
	return number * scale, nil
}
