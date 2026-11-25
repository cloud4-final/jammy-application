package config

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
)

func ParseDuration(name, value string) (time.Duration, error) {
	unit := time.Second
	if !strings.HasSuffix(name, "_SECONDS") {
		if value == "" {
			return 0, fmt.Errorf("invalid duration for %q: duration is empty", name)
		}
		switch value[len(value)-1] {
		case 's':
		case 'm':
			unit = time.Minute
		case 'h':
			unit = time.Hour
		default:
			return 0, fmt.Errorf("invalid duration unit for %q", name)
		}
		value = value[:len(value)-1]
	}
	number, err := parseScaled(value, int64(unit), math.MaxInt64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %q: %w", name, err)
	}
	return time.Duration(number), nil
}

func ParseSize(value string) (int64, error) {
	for _, unit := range []struct {
		suffix string
		bytes  int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}} {
		if number, ok := strings.CutSuffix(value, unit.suffix); ok {
			return parseScaled(number, unit.bytes, math.MaxInt64)
		}
	}
	return parseScaled(value, 1, math.MaxInt64)
}

func ParseList(value string) ([]string, error) {
	items := strings.Split(value, ",")
	for _, item := range items {
		if item == "" || strings.IndexFunc(item, unicode.IsSpace) >= 0 {
			return nil, errors.New("expected comma-separated nonempty items without spaces")
		}
	}
	return items, nil
}

func ParseBool(value string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errors.New("expected true or false")
	}
}
