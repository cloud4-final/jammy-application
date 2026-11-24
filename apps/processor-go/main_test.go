package main

import (
	"strings"
	"testing"
	"time"
)

func TestRejectInvalidParityEnv(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	for name, value := range map[string]string{"HTTP_PORT": "0", "ADMIN_PORT": "0", "MAX_ACK_PENDING": "0", "ANOMALY_DROP_RATIO": "0", "CONSUMER_NAME": ""} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := load(); err == nil {
				t.Fatalf("accepted %s=%q", name, value)
			}
		})
	}
}

func TestLoadNamesInvalidNATSStreams(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "jetstream")
	t.Setenv("NATS_STREAMS", "stream,")
	_, err := load()
	if err == nil || !strings.HasPrefix(err.Error(), "invalid NATS_STREAMS: ") {
		t.Fatalf("load error = %v, want invalid NATS_STREAMS", err)
	}
}

func TestProcessorScheduleAndFilter(t *testing.T) {
	cfg := settings{stateFlush: 19 * time.Second, consumer: "processor", maxPending: 17, ackWait: 5 * time.Second}
	c := cfg.consumerConfig("raw-test")
	if c.FilterSubject != "jammy.raw.raw-test" || c.MaxAckPending != 17 {
		t.Fatalf("consumer configuration: %+v", c)
	}
	if fetchWait != time.Second || idleDelay != 200*time.Millisecond || retryDelay != time.Second || assetRefreshInterval != time.Minute {
		t.Fatal("processor poll, retry or refresh timing differs from parity rules")
	}
	now := time.Now()
	if receivedLag(now, time.Time{}) != 0 || time.Duration(receivedLag(now, now.Add(-3*time.Second))) != 3*time.Second {
		t.Fatal("processor lag did not track the last processed row")
	}
}
