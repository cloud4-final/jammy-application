package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cloud4-final/jammy-5th/tools/payloadgen/gen"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: payloadgen assets|batches [flags]")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	switch args[0] {
	case "assets":
		profile := flags.String("profile", "solar", "asset profile")
		count := flags.Int("count", 1, "number of assets")
		tenants := flags.Int("tenants", 1, "number of tenants")
		if err := parse(flags, args[1:]); err != nil {
			return err
		}
		return gen.WriteAssets(stdout, *profile, *count, *tenants)
	case "batches":
		var opts gen.Options
		flags.StringVar(&opts.Profile, "profile", "solar", "event profile")
		flags.IntVar(&opts.EventsPerBatch, "events-per-batch", 100, "events per batch")
		flags.IntVar(&opts.Batches, "batches", 1, "number of batches")
		flags.IntVar(&opts.Assets, "assets", 1, "number of available assets")
		flags.Int64Var(&opts.Seed, "seed", 1, "deterministic random seed")
		out := flags.String("out", "", "empty output directory")
		mode := flags.String("time-mode", "fixed", "fixed fixture time or relative to invocation")
		if err := parse(flags, args[1:]); err != nil {
			return err
		}
		if *out == "" {
			return fmt.Errorf("batches requires --out")
		}
		switch *mode {
		case "fixed":
		case "relative":
			opts.Anchor = time.Now()
		default:
			return fmt.Errorf("invalid --time-mode %q, expected fixed or relative", *mode)
		}
		return gen.WriteBatches(*out, opts)
	case "help", "-h", "--help":
		_, err := io.WriteString(stdout, "usage: payloadgen assets|batches [flags]\n")
		return err
	default:
		return fmt.Errorf("unknown subcommand %q, expected assets or batches", args[0])
	}
}

func parse(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return nil
}
