package gate

import (
	"fmt"
	"math"
	"regexp"
	"time"

	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	MaxPast, MaxFuture time.Duration
}

type Rejection struct {
	Event  *pb.Event
	Reason string
}

type Result struct {
	Accepted      []*pb.Event
	Rejected      []Rejection
	PrimaryValues []float64
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ValidBatch checks the metadata and event count before per-event validation.
func ValidBatch(batch *pb.EventBatch) bool {
	producer := batch.GetProducerId()
	return identifier.MatchString(batch.GetBatchId()) &&
		(producer == "" || identifier.MatchString(producer)) &&
		len(batch.GetEvents()) >= 1 && len(batch.GetEvents()) <= 10000
}

// Validate checks events in order. PrimaryValues uses the same order as Accepted.
func Validate(batch *pb.EventBatch, now time.Time, cfg Config) Result {
	minTime := now.Add(-cfg.MaxPast)
	earliest := minTime.UnixMilli()
	if minTime.Nanosecond()%int(time.Millisecond) != 0 {
		earliest++
	}
	latest := now.Add(cfg.MaxFuture).UnixMilli()
	seen := make(map[string]struct{}, len(batch.GetEvents()))
	var result Result
	for _, event := range batch.GetEvents() {
		id := event.GetEventId()
		if !identifier.MatchString(id) {
			result.Rejected = append(result.Rejected, Rejection{event, "bad_event_id"})
			continue
		}
		_, duplicate := seen[id]
		seen[id] = struct{}{}
		value, reason := validateEvent(event, earliest, latest, duplicate)
		if reason != "" {
			result.Rejected = append(result.Rejected, Rejection{event, reason})
			continue
		}
		result.Accepted = append(result.Accepted, event)
		result.PrimaryValues = append(result.PrimaryValues, value)
	}
	return result
}

func validateEvent(event *pb.Event, earliest, latest int64, duplicate bool) (float64, string) {
	timestamp := event.GetEventTimeUnixMs()
	switch {
	case !identifier.MatchString(event.GetRoutingKey()):
		return 0, "bad_routing_key"
	case event.Profile != "solar" && event.Profile != "synth-small" && event.Profile != "synth-large":
		return 0, "unknown_profile"
	case event.SchemaVersion != 1:
		return 0, "unsupported_version"
	case timestamp < earliest || timestamp > latest:
		return 0, "bad_event_time"
	}

	value, reason := primaryValue(event)
	if reason != "" {
		return 0, reason
	}
	if duplicate {
		return 0, "duplicate_in_batch"
	}
	if len(event.Attrs) > 16 {
		return 0, "bad_attrs"
	}
	for key, value := range event.Attrs {
		if len(key) > 64 || len(value) > 64 {
			return 0, "bad_attrs"
		}
	}
	return value, ""
}

func primaryValue(event *pb.Event) (float64, string) {
	switch event.Profile {
	case "solar":
		var reading pb.SolarReading
		if err := proto.Unmarshal(event.Payload, &reading); err != nil || reading.AssetId != event.RoutingKey {
			return 0, "bad_payload"
		}
		if !inRange(reading.PowerKw, 0, 100000) || !inRange(reading.IrradianceWm2, 0, 1500) ||
			!inRange(reading.TemperatureC, -40, 90) || reading.Status < pb.Status_STATUS_OK ||
			reading.Status > pb.Status_STATUS_OFFLINE {
			return 0, "out_of_range"
		}
		return reading.PowerKw, ""
	case "synth-small":
		var reading pb.SynthSmall
		if err := proto.Unmarshal(event.Payload, &reading); err != nil {
			return 0, "bad_payload"
		}
		if !finite(reading.Value) || len(reading.Pad) > 256 {
			return 0, "out_of_range"
		}
		return reading.Value, ""
	case "synth-large":
		var reading pb.SynthLarge
		if err := proto.Unmarshal(event.Payload, &reading); err != nil {
			return 0, "bad_payload"
		}
		if !finite(reading.Value) || len(reading.Blob) > 65536 {
			return 0, "out_of_range"
		}
		return reading.Value, ""
	}
	return 0, "unknown_profile"
}

func finite(value float64) bool                      { return !math.IsNaN(value) && !math.IsInf(value, 0) }
func inRange(value, minValue, maxValue float64) bool { return value >= minValue && value <= maxValue }

// ProjectPrimaryValue decodes a typed measurement without reapplying the ingest gate.
func ProjectPrimaryValue(event *pb.Event) (float64, error) {
	switch event.GetProfile() {
	case "solar":
		var reading pb.SolarReading
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.PowerKw, err
	case "synth-small":
		var reading pb.SynthSmall
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.Value, err
	case "synth-large":
		var reading pb.SynthLarge
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.Value, err
	default:
		return 0, fmt.Errorf("cannot decode payload for unknown profile")
	}
}
