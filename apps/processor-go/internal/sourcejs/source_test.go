package sourcejs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/aggregate"
	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/state"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func launch(t *testing.T, port string, options ...string) string {
	t.Helper()
	id := docker(t, append([]string{"run", "-d", "-p", "127.0.0.1::" + port}, options...)...)
	t.Cleanup(func() { docker(t, "rm", "-f", id) })
	return docker(t, "port", id, port+"/tcp")
}

func TestJetStreamProcessor(t *testing.T) {
	ctx := t.Context()
	addr := launch(t, "5432", "-e", "POSTGRES_PASSWORD=test", "--tmpfs", "/var/lib/postgresql/data", "postgres:17.12-trixie")
	dsn := "postgres://postgres:test@" + addr + "/postgres?sslmode=disable"
	pool, err := pgxpool.New(ctx, dsn)
	must(t, err)
	t.Cleanup(pool.Close)
	deadline := time.Now().Add(90 * time.Second)
	for {
		check, cancel := context.WithTimeout(ctx, time.Second)
		err = pool.Ping(check)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PostgreSQL unavailable: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "db", "migrations", "0001_init.up.sql"))
	must(t, err)
	_, err = pool.Exec(ctx, string(schema))
	must(t, err)
	natsAddr := launch(t, "4222", "nats:2.15.3", "-js")
	var nc *nats.Conn
	for deadline := time.Now().Add(60 * time.Second); ; {
		nc, err = nats.Connect("nats://" + natsAddr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	must(t, err)
	store := state.New(pool, "processor", 24*time.Hour, .5)

	setup := func(t *testing.T, id string, count int) jetstream.Consumer {
		t.Helper()
		subject := "jammy.raw." + id
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: id, Subjects: []string{subject}})
		must(t, err)
		consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "processor", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second})
		must(t, err)
		now := time.Now()
		batch := &pb.EventBatch{BatchId: id}
		for i := range count {
			batch.Events = append(batch.Events, &pb.Event{EventId: fmt.Sprintf("%s-%d", id, i), RoutingKey: id, Profile: "synth-small", SchemaVersion: 1, EventTimeUnixMs: now.UnixMilli()})
		}
		msg := nats.NewMsg(subject)
		msg.Data, err = proto.Marshal(batch)
		must(t, err)
		msg.Header.Set("Nats-Msg-Id", id)
		msg.Header.Set("Content-Type", "application/x-protobuf")
		msg.Header.Set("Jammy-Ingest-Id", "fixture")
		msg.Header.Set("Jammy-Received-At-Ms", strconv.FormatInt(now.UnixMilli(), 10))
		_, err = js.PublishMsg(ctx, msg)
		must(t, err)
		return consumer
	}
	fetch := func(t *testing.T, consumer jetstream.Consumer) jetstream.Msg {
		t.Helper()
		message, err := consumer.Next(jetstream.FetchMaxWait(15 * time.Second))
		must(t, err)
		return message
	}
	count := func(t *testing.T, id string, want int) {
		t.Helper()
		var actual int
		must(t, pool.QueryRow(ctx, "SELECT COALESCE(sum(count),0) FROM jammy.summary_1m WHERE routing_key=$1", id).Scan(&actual))
		if actual != want {
			t.Fatalf("%s rows: got %d, want %d", id, actual, want)
		}
	}

	t.Run("TestSummaryCountsMatchPublished", func(t *testing.T) {
		c := setup(t, "count", 10000)
		_, _, err := Process(ctx, store, []jetstream.Msg{fetch(t, c)})
		must(t, err)
		count(t, "count", 10000)
		var before int
		must(t, pool.QueryRow(ctx, "SELECT count(*) FROM jammy.latest_state WHERE routing_key=$1", "count").Scan(&before))
		if before != 0 {
			t.Fatal("latest_state written during summary transaction")
		}
		must(t, store.FlushLatest(ctx))
		must(t, pool.QueryRow(ctx, "SELECT count(*) FROM jammy.latest_state WHERE routing_key=$1", "count").Scan(&before))
		if before != 1 {
			t.Fatal("coalesced state was not flushed")
		}

	})

	t.Run("TestDropDetailsAndLatestCoalescing", func(t *testing.T) {
		minute := time.Now().UTC().Truncate(time.Minute)
		events := []aggregate.Event{
			{Profile: "synth-small", RoutingKey: "drop", Time: minute.Add(-time.Minute), Value: 100},
			{Profile: "synth-small", RoutingKey: "drop", Time: minute, Value: 10},
		}
		_, err := store.Commit(ctx, []state.Batch{{ID: "drop-check", Events: events}})
		must(t, err)
		_, err = store.Commit(ctx, []state.Batch{{ID: "drop-older", Events: events[:1]}})
		must(t, err)
		var details []byte
		must(t, pool.QueryRow(ctx, "SELECT details FROM jammy.incidents WHERE routing_key='drop' AND kind='drop'").Scan(&details))
		var values map[string]float64
		must(t, json.Unmarshal(details, &values))
		if len(values) != 3 || values["last_mean"] != 10 || values["window_mean"] != 55 || values["ratio"] != .5 {
			t.Fatalf("incorrect incident details %s", details)
		}
		must(t, store.FlushLatest(ctx))
		var latest float64
		must(t, pool.QueryRow(ctx, "SELECT primary_value FROM jammy.latest_state WHERE routing_key='drop'").Scan(&latest))
		if latest != 10 {
			t.Fatalf("latest value = %g", latest)
		}
	})

	t.Run("TestSortedClaimsAndResultOrder", func(t *testing.T) {
		_, err := pool.Exec(ctx, `CREATE TABLE jammy.claim_order (sequence bigserial, batch_id text);
            CREATE FUNCTION jammy.record_claim_order() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO jammy.claim_order(batch_id) VALUES (NEW.batch_id); RETURN NEW; END $$;
            CREATE TRIGGER record_claim_order BEFORE INSERT ON jammy.processed_batches
            FOR EACH ROW EXECUTE FUNCTION jammy.record_claim_order()`)
		must(t, err)
		t.Cleanup(func() {
			_, err := pool.Exec(ctx, "DROP TRIGGER record_claim_order ON jammy.processed_batches")
			must(t, err)
		})
		result, err := store.Commit(ctx, []state.Batch{{ID: "sorted-z"}, {ID: "sorted-a"}, {ID: "sorted-z"}})
		must(t, err)
		var claims string
		must(t, pool.QueryRow(ctx, "SELECT string_agg(batch_id, ',' ORDER BY sequence) FROM jammy.claim_order").Scan(&claims))
		if claims != "sorted-a,sorted-z,sorted-z" || result[0] || result[1] || !result[2] {
			t.Fatalf("claim order %s, duplicate results %v", claims, result)
		}
	})

	t.Run("TestRefreshAssetsSuppliesSolarCapacity", func(t *testing.T) {
		_, err := pool.Exec(ctx, "INSERT INTO jammy.assets (asset_id, tenant_id, profile, capacity_kw) VALUES ('solar-test','tenant','solar',100)")
		must(t, err)
		must(t, store.RefreshAssets(ctx))
		_, err = store.Commit(ctx, []state.Batch{{ID: "solar-test", Events: []aggregate.Event{
			{RoutingKey: "solar-test", Profile: "solar", Time: time.Now(), Value: 110.001},
		}}})
		must(t, err)
		var found bool
		must(t, pool.QueryRow(ctx, "SELECT EXISTS (SELECT FROM jammy.incidents WHERE routing_key='solar-test' AND kind='over_capacity')").Scan(&found))
		if !found {
			t.Fatal("background refresh did not supply capacity")
		}
	})

	t.Run("TestAckOnlyAfterCommit", func(t *testing.T) {
		c := setup(t, "rollback", 12)
		_, err := pool.Exec(ctx, `CREATE FUNCTION jammy.interrupt_processor() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NEW; END $$;
			CREATE CONSTRAINT TRIGGER interrupt_processor AFTER INSERT ON jammy.processed_batches
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION jammy.interrupt_processor()`)
		must(t, err)
		_, _, err = Process(ctx, store, []jetstream.Msg{fetch(t, c)})
		if err == nil {
			t.Fatal("expected transaction to lose its DB connection at commit")
		}
		count(t, "rollback", 0)
		_, err = pool.Exec(ctx, "DROP TRIGGER interrupt_processor ON jammy.processed_batches")
		must(t, err)
		msg := fetch(t, c)
		meta, err := msg.Metadata()
		must(t, err)
		if meta.NumDelivered < 2 {
			t.Fatal("message was not redelivered after failed commit")
		}
		_, _, err = Process(ctx, store, []jetstream.Msg{msg})
		must(t, err)
		count(t, "rollback", 12)
	})

	t.Run("TestRedeliveryAfterAckwaitNoDoubleCount", func(t *testing.T) {
		c := setup(t, "redelivery", 15)
		first := fetch(t, c)
		time.Sleep(2 * time.Second)
		second := fetch(t, c)
		results, _, err := Process(ctx, store, []jetstream.Msg{first, second})
		must(t, err)
		if len(results) != 2 || results[0] || !results[1] {
			t.Fatalf("incorrect duplicate result %v", results)
		}
		count(t, "redelivery", 15)
	})

	t.Run("TestKill9RestartExactCounts", func(t *testing.T) {
		c := setup(t, "restart", 20)
		msg := fetch(t, c)
		if err := msg.Nak(); err != nil {
			t.Fatal(err)
		}
		child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCrashHelper$")
		child.Env = append(os.Environ(), "PROCESSOR_CRASH_FIXTURE=1", "PROCESSOR_TEST_DSN="+dsn, "PROCESSOR_TEST_NATS=nats://"+natsAddr)
		var output strings.Builder
		child.Stdout, child.Stderr = &output, &output
		must(t, child.Start())
		finished := make(chan error, 1)
		go func() { finished <- child.Wait(); close(finished) }()
		t.Cleanup(func() { _ = child.Process.Kill(); <-finished })
		for deadline := time.Now().Add(30 * time.Second); ; {
			var committed bool
			must(t, pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM jammy.processed_batches WHERE consumer='processor' AND batch_id='restart')`).Scan(&committed))
			if committed {
				break
			}
			select {
			case err := <-finished:
				t.Fatalf("crash fixture stopped: %v: %s", err, output.String())
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("subprocess did not commit")
			}
			time.Sleep(50 * time.Millisecond)
		}
		must(t, child.Process.Kill())
		if err := <-finished; err == nil {
			t.Fatal("expected SIGKILL")
		}
		results, _, err := Process(ctx, store, []jetstream.Msg{fetch(t, c)})
		must(t, err)
		if len(results) != 1 || !results[0] {
			t.Fatalf("expected durable claim after SIGKILL, got %v", results)
		}
		count(t, "restart", 20)
	})

	t.Run("TestProcessedBatchesPrunedAfter1d", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO jammy.processed_batches (consumer,batch_id,processed_at)
			VALUES ('processor','old',now()-interval '25 hours'),('ingest','old',now()-interval '25 hours'),
			('processor','recent',now()-interval '23 hours')`)
		must(t, err)
		must(t, store.Prune(ctx))
		var rows int
		must(t, pool.QueryRow(ctx, `SELECT count(*) FROM jammy.processed_batches WHERE batch_id IN ('old','recent')
			AND (consumer,batch_id) <> ('processor','old')`).Scan(&rows))
		var old bool
		must(t, pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM jammy.processed_batches WHERE consumer='processor' AND batch_id='old')`).Scan(&old))
		if rows != 2 || old {
			t.Fatal("claim retention affected wrong rows")
		}
	})
}

type crashAfterCommit struct{ *state.Store }

func (c crashAfterCommit) Commit(ctx context.Context, batches []state.Batch) ([]bool, error) {
	result, err := c.Store.Commit(ctx, batches)
	if err == nil {
		<-ctx.Done()
	}
	return result, err
}

func TestCrashHelper(t *testing.T) {
	if os.Getenv("PROCESSOR_CRASH_FIXTURE") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("PROCESSOR_TEST_DSN"))
	must(t, err)
	defer pool.Close()
	nc, err := nats.Connect(os.Getenv("PROCESSOR_TEST_NATS"))
	must(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	must(t, err)
	c, err := js.Consumer(ctx, "restart", "processor")
	must(t, err)
	msg, err := c.Next(jetstream.FetchMaxWait(15 * time.Second))
	must(t, err)
	store := state.New(pool, "processor", 24*time.Hour, .5)
	_, _, err = Process(ctx, crashAfterCommit{store}, []jetstream.Msg{msg})
	must(t, err)
}

type rawMessage struct {
	jetstream.Msg
	headers nats.Header
	data    []byte
	acked   bool
}

func (m *rawMessage) Headers() nats.Header { return m.headers }
func (m *rawMessage) Data() []byte         { return m.data }
func (m *rawMessage) Ack() error           { m.acked = true; return nil }

type recordingStore struct{ batches []state.Batch }

func (s *recordingStore) Commit(_ context.Context, batches []state.Batch) ([]bool, error) {
	s.batches = batches
	return make([]bool, len(batches)), nil
}

func TestProcessUsesIngestValidationAndLastReceiveTime(t *testing.T) {
	store := &recordingStore{}
	messages := make([]jetstream.Msg, 0, 3)
	for i, headerID := range []string{"first", "bad-header", "last"} {
		id := []string{"first", "different", "last"}[i]
		event := &pb.Event{RoutingKey: "already-at-ingest", Profile: "synth-small"}
		body, err := proto.Marshal(&pb.EventBatch{BatchId: id, Events: []*pb.Event{event}})
		must(t, err)
		headers := nats.Header{}
		headers.Set("Content-Type", "application/x-protobuf")
		headers.Set("Nats-Msg-Id", headerID)
		headers.Set("Jammy-Received-At-Ms", strconv.Itoa(100+i))
		messages = append(messages, &rawMessage{headers: headers, data: body})
	}
	results, received, err := Process(t.Context(), store, messages)
	if FailedCount(err) != 1 || len(results) != 2 || len(store.batches) != 2 || received.UnixMilli() != 102 {
		t.Fatalf("results %v, batches %v, received %s, error %v", results, store.batches, received, err)
	}
	for i, message := range messages {
		if message.(*rawMessage).acked != (i != 1) {
			t.Fatalf("wrong ACK status for message %d", i)
		}
	}
}
