package seal

import (
	"testing"
	"time"
)

func TestSealWrittenAfterWatermark(t *testing.T) {
	hour := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	end := hour.Add(time.Hour + 10*time.Minute)
	if Eligible(hour, end, 10*time.Minute, false) || !Eligible(hour, end.Add(time.Millisecond), 10*time.Minute, false) {
		t.Fatal("JetStream seals only after the watermark exceeds hour end plus lateness")
	}
	if Eligible(hour, hour.Add(time.Hour), 10*time.Minute, true) || !Eligible(hour, hour.Add(2*time.Hour), 10*time.Minute, true) {
		t.Fatal("direct DB seals only through the last received hour allowed by the lateness window")
	}
}
