package anomaly

import "testing"

func TestAnomalyOverCapacity(t *testing.T) {
	if !OverCapacity("solar", 111, 100) || OverCapacity("solar", 110.0001, 100) || !OverCapacity("solar", 110.001, 100) || OverCapacity("solar", 109, 100) || OverCapacity("synth-small", 111, 100) {
		t.Fatal("incorrect capacity threshold")
	}
}
