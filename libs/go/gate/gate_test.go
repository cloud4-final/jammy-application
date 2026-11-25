package gate_test

import (
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/gate"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"google.golang.org/protobuf/proto"
)

func TestPrimaryValuesAlignWithAcceptedEvents(t *testing.T) {
	payload, err := proto.Marshal(&pb.SolarReading{AssetId: "a", PowerKw: 12.5, Status: pb.Status_STATUS_OK})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0)
	good := &pb.Event{EventId: "e", RoutingKey: "a", Profile: "solar", SchemaVersion: 1, EventTimeUnixMs: now.UnixMilli(), Payload: payload}
	result := gate.Validate(&pb.EventBatch{Events: []*pb.Event{{}, good}}, now, gate.Config{})
	if len(result.Accepted) != 1 || result.Accepted[0] != good ||
		len(result.PrimaryValues) != 1 || result.PrimaryValues[0] != 12.5 {
		t.Fatalf("accepted=%v primary values=%v", result.Accepted, result.PrimaryValues)
	}
}

func TestInvalidFirstEventStillClaimsBatchID(t *testing.T) {
	now := time.Unix(1700000000, 0)
	first := &pb.Event{EventId: "same", RoutingKey: "asset", Profile: "unknown", SchemaVersion: 1, EventTimeUnixMs: now.UnixMilli()}
	second := proto.CloneOf(first)
	second.Profile = "synth-small"
	result := gate.Validate(&pb.EventBatch{Events: []*pb.Event{first, second}}, now, gate.Config{})
	if len(result.Accepted) != 0 || len(result.Rejected) != 2 ||
		result.Rejected[0].Reason != "unknown_profile" || result.Rejected[1].Reason != "duplicate_in_batch" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestFractionalNowUsesInclusiveMillisecondBounds(t *testing.T) {
	now := time.Unix(1700000000, 500000)
	var events []*pb.Event
	for i, id := range []string{"early", "lower", "upper", "late"} {
		events = append(events, &pb.Event{
			EventId: id, RoutingKey: "asset", Profile: "synth-small", SchemaVersion: 1,
			EventTimeUnixMs: now.UnixMilli() + int64(i),
		})
	}
	result := gate.Validate(&pb.EventBatch{Events: events}, now,
		gate.Config{MaxPast: 0, MaxFuture: 2 * time.Millisecond})
	if len(result.Accepted) != 2 || result.Accepted[0] != events[1] || result.Accepted[1] != events[2] ||
		len(result.Rejected) != 2 || result.Rejected[0].Reason != "bad_event_time" || result.Rejected[1].Reason != "bad_event_time" {
		t.Fatalf("unexpected result %+v", result)
	}
}
