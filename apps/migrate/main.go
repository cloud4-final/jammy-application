package main

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required")
		os.Exit(1)
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

	const migrate = "/migrate"
	args := append([]string{migrate, "-database", databaseURL}, os.Args[1:]...)
	if err := syscall.Exec(migrate, args, os.Environ()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
