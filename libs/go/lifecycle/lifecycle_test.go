package lifecycle

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestRunSigtermExitsCleanly(t *testing.T) { testSignal(t, "graceful", 0) }
func TestRunTimeoutForcesExit(t *testing.T)   { testSignal(t, "timeout", 1) }

func TestLifecycleChild(t *testing.T) {
	mode := os.Getenv("JAMMY_LIFECYCLE_TEST")
	if mode == "" {
		return
	}
	Run(func(ctx context.Context) error {
		fmt.Println("ready")
		<-ctx.Done()
		if mode == "timeout" {
			select {}
		}
		return nil
	}, 100*time.Millisecond)
}

func testSignal(t *testing.T, mode string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleChild$")
	cmd.Env = append(os.Environ(), "JAMMY_LIFECYCLE_TEST="+mode, "GORACE=atexit_sleep_ms=0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child did not start: %q", scanner.Text())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Wait()
		t.Fatal(err)
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if expected == 0 && err != nil ||
		expected != 0 && (!errors.As(err, &exit) || exit.ExitCode() != expected) {
		t.Fatalf("expected exit %d, got %v", expected, err)
	}
}
