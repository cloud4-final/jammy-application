package anomaly

import "math"

func OverCapacity(profile string, value, capacity float64) bool {
	if profile != "solar" {
		return false
	}
	powerMilliKW := int64(math.Round(value * 1000))
	capacityMilliKW := int64(math.Round(capacity * 1000))
	return powerMilliKW*10 > capacityMilliKW*11
}
