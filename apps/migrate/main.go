package main

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/cloud4-final/jammy-application/libs/go/config"
)

func migrationURL() (string, error) {
	databaseURL, err := config.Lookup("DATABASE_URL", "")
	if err != nil {
		return "", err
	}
	if databaseURL == "" {
		return "", fmt.Errorf("DATABASE_URL is required")
	}

	// PostgreSQL may resolve "$user" to jammy after the first migration.
	// Prepend the metadata options so they take precedence over any duplicates.
	const tableOptions = "x-migrations-table-quoted=true&x-migrations-table=%22public%22.%22schema_migrations%22"
	databaseURL, _, _ = strings.Cut(databaseURL, "#")
	base, query, _ := strings.Cut(databaseURL, "?")
	databaseURL = base + "?" + tableOptions
	if query != "" {
		databaseURL += "&" + query
	}
	return databaseURL, nil
}

func main() {
	databaseURL, err := migrationURL()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	const migrate = "/migrate"
	args := append([]string{migrate, "-database", databaseURL}, os.Args[1:]...)
	if err := syscall.Exec(migrate, args, os.Environ()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
