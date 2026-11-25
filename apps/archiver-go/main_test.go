package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/exportdb"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/seal"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/sourcejs"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/cloud4-final/jammy-5th/libs/go/s3x"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
)

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadAllowsZeroShutdownTimeout(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("S3_BUCKET", "archive")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "0")

	cfg, err := load()
	check(t, err)
	if cfg.ShutdownTimeout != 0 {
		t.Fatalf("shutdown timeout = %s, want zero", cfg.ShutdownTimeout)
	}
}

func container(t *testing.T, port string, args ...string) string {
	t.Helper()
	run := func(args ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		cmd := exec.Command("docker", args...)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("docker %v: %v: %s%s", args, err, stderr.String(), stdout.String())
		}
		return strings.TrimSpace(stdout.String())
	}
	id := run(append([]string{"run", "-d", "-p", "127.0.0.1::" + port}, args...)...)
	t.Cleanup(func() { run("rm", "-f", id) })
	return run("port", id, port+"/tcp")
}

func retry(t *testing.T, limit time.Duration, action func() error) {
	t.Helper()
	for stop := time.Now().Add(limit); ; {
		err := action()
		if err == nil {
			return
		}
		if time.Now().After(stop) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestArchiveEndToEnd(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("S3_FORCE_PATH_STYLE", "true")
	address := container(t, "8333", "chrislusf/seaweedfs:4.54", "server", "-s3", "-dir=/data")
	t.Setenv("S3_ENDPOINT", "http://"+address)
	ctx := t.Context()
	client, err := s3x.New(ctx)
	check(t, err)
	bucket := "jammy-archive"
	retry(t, 90*time.Second, func() error {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := client.CreateBucket(c, &s3.CreateBucketInput{Bucket: &bucket})
		return err
	})
	store := pq.S3{Client: client, Bucket: bucket}
	stage := t.TempDir()
	archive := pq.Archive{Objects: store, Dir: stage, Level: 1}
	natsAddress := container(t, "4222", "nats:2.15.3", "-js")
	t.Setenv("ARCHIVE_TEST_NATS", "nats://"+natsAddress)
	t.Setenv("ARCHIVE_TEST_BUCKET", bucket)
	var nc *nats.Conn
	retry(t, 60*time.Second, func() error { nc, err = nats.Connect("nats://" + natsAddress); return err })
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	check(t, err)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "raw-test", Subjects: []string{"jammy.raw.raw-test"}})
	check(t, err)
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "archiver", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second,
	})
	check(t, err)
	publish := func(t *testing.T, owner, id string, at time.Time, count int) (uint64, jetstream.Msg) {
		t.Helper()
		batch := &pb.EventBatch{BatchId: id}
		for i := range count {
			batch.Events = append(batch.Events, &pb.Event{
				EventId: fmt.Sprintf("%s-%d", id, i), RoutingKey: "fixture", Profile: "synth-small",
				SchemaVersion: 1, EventTimeUnixMs: at.UnixMilli(),
			})
		}
		msg := nats.NewMsg("jammy.raw.raw-test")
		msg.Data, err = proto.Marshal(batch)
		check(t, err)
		msg.Header.Set("Nats-Msg-Id", id)
		msg.Header.Set("Content-Type", "application/x-protobuf")
		msg.Header.Set("Jammy-Ingest-Id", owner)
		msg.Header.Set("Jammy-Received-At-Ms", strconv.FormatInt(at.Add(time.Second).UnixMilli(), 10))
		ack, err := js.PublishMsg(ctx, msg)
		check(t, err)
		message, err := consumer.Next(jetstream.FetchMaxWait(10 * time.Second))
		check(t, err)
		return ack.Sequence, message
	}
	read := func(t *testing.T, prefix string) []pq.Row {
		t.Helper()
		keys, err := store.List(ctx, prefix)
		check(t, err)
		var rows []pq.Row
		for _, key := range keys {
			if !strings.HasSuffix(key, ".parquet") {
				continue
			}
			body, err := store.Get(ctx, key)
			check(t, err)
			data, err := io.ReadAll(body)
			check(t, err)
			check(t, body.Close())
			f, err := parquet.Read[pq.Row](bytes.NewReader(data), int64(len(data)))
			check(t, err)
			rows = append(rows, f...)
		}
		return rows
	}
	worker := &sourcejs.Archiver{Archive: archive, Lateness: 10 * time.Minute}
	hour := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	t.Run("TestS3RowsEqualUniquePublished", func(t *testing.T) {
		var messages []jetstream.Msg
		for i := range 2 {
			_, msg := publish(t, "count", fmt.Sprintf("count-%d", i), hour, 10000)
			messages = append(messages, msg)
		}
		check(t, worker.Flush(ctx, messages))
		rows := read(t, seal.Prefix("synth-small", hour))
		ids := make(map[string]struct{})
		for _, row := range rows {
			ids[row.EventID] = struct{}{}
			if row.ReceivedAt != hour.Add(time.Second).UnixMilli() {
				t.Fatal("received_at did not come from the header")
			}
		}
		if len(rows) != 20000 || len(ids) != 20000 {
			t.Fatalf("rows=%d unique=%d", len(rows), len(ids))
		}
	})
	t.Run("TestStageFileRemovedAfterUpload", func(t *testing.T) {
		files, err := os.ReadDir(stage)
		check(t, err)
		if len(files) != 0 {
			t.Fatalf("staging files remain: %v", files)
		}
	})
	t.Run("TestSealWrittenAfterWatermark", func(t *testing.T) {
		_, msg := publish(t, "count", "watermark", hour.Add(time.Hour+10*time.Minute+time.Millisecond), 1)
		check(t, worker.Flush(ctx, []jetstream.Msg{msg}))
		var doc seal.Document
		check(t, pq.ReadJSON(ctx, store, seal.Prefix("synth-small", hour)+"_SEALED-count.json", &doc))
		if doc.Rows != 20000 || len(doc.Files) != 1 {
			t.Fatalf("incorrect seal %+v", doc)
		}
	})
	t.Run("TestLateEventGoesToRawLate", func(t *testing.T) {
		_, msg := publish(t, "count", "late", hour, 1)
		worker = &sourcejs.Archiver{Archive: archive, Lateness: 10 * time.Minute}
		check(t, worker.Flush(ctx, []jetstream.Msg{msg}))
		late := read(t, "raw-late/")
		var doc seal.Document
		check(t, pq.ReadJSON(ctx, store, seal.Prefix("synth-small", hour)+"_SEALED-count.json", &doc))
		if len(late) != 1 || doc.Rows != 20000 || late[0].EventID != "late-0" {
			t.Fatal("late event altered the original seal or was not archived")
		}
	})
	t.Run("TestKill9MidFlushNoLossNoExtraKeys", func(t *testing.T) {
		crashHour := hour.Add(3 * time.Hour)
		seq, msg := publish(t, "crash", "crash", crashHour, 12)
		check(t, msg.Nak())
		key := seal.Prefix("synth-small", crashHour) + fmt.Sprintf("crash-%d-%d.parquet", seq, seq)
		child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestArchiveCrashHelper$")
		child.Env = append(os.Environ(), "ARCHIVE_CRASH_FIXTURE=1", "ARCHIVE_TEST_STAGE="+t.TempDir())
		check(t, child.Start())
		done := make(chan error, 1)
		go func() { done <- child.Wait(); close(done) }()
		t.Cleanup(func() { _ = child.Process.Kill(); <-done })
		retry(t, 30*time.Second, func() error {
			exists, err := store.Exists(ctx, key)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("process did not write Parquet before SIGKILL")
			}
			return nil
		})
		check(t, child.Process.Kill())
		if err := <-done; err == nil {
			t.Fatal("expected SIGKILL")
		}
		message, err := consumer.Next(jetstream.FetchMaxWait(10 * time.Second))
		check(t, err)
		worker = &sourcejs.Archiver{Archive: archive, Lateness: 10 * time.Minute}
		uploads := 0
		worker.Archive.Uploaded = func(context.Context, int64, time.Duration) { uploads++ }
		check(t, worker.Flush(ctx, []jetstream.Msg{message}))
		rows := read(t, seal.Prefix("synth-small", crashHour))
		keys, err := store.List(ctx, seal.Prefix("synth-small", crashHour))
		check(t, err)
		if len(rows) != 12 || len(keys) != 2 || uploads != 0 {
			t.Fatalf("crash replay rows=%d keys=%v uploads=%d", len(rows), keys, uploads)
		}
	})
	t.Run("TestDBModeExportThenDropPartition", func(t *testing.T) {
		address := container(t, "5432", "-e", "POSTGRES_PASSWORD=test", "--tmpfs", "/var/lib/postgresql/data", "postgres:17.12-trixie")
		pool, err := pgxpool.New(ctx, "postgres://postgres:test@"+address+"/postgres?sslmode=disable")
		check(t, err)
		t.Cleanup(pool.Close)
		retry(t, 90*time.Second, func() error { c, cancel := context.WithTimeout(ctx, time.Second); defer cancel(); return pool.Ping(c) })
		for _, name := range []string{"0001_init.up.sql", "0002_direct_db_xid_cursor.up.sql"} {
			data, err := os.ReadFile(filepath.Join("..", "..", "contracts", "db", "migrations", name))
			check(t, err)
			_, err = pool.Exec(ctx, string(data))
			check(t, err)
		}
		start := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
		partition := "raw_events_" + start.Format("2006010215")
		_, err = pool.Exec(ctx, fmt.Sprintf("CREATE TABLE jammy.%s PARTITION OF jammy.raw_events FOR VALUES FROM ('%s') TO ('%s')",
			partition, start.Format(time.RFC3339), start.Add(time.Hour).Format(time.RFC3339)))
		check(t, err)
		_, err = pool.Exec(ctx, `INSERT INTO jammy.raw_events (event_id,routing_key,profile,schema_version,event_time,received_at,ingest_id,primary_value,payload)
			VALUES ('row-a','a','solar',1,$1::timestamptz-interval '1 hour',$1,'pg',1,''::bytea),
				   ('row-b','b','synth-large',1,$1,$1,'pg',2,''::bytea)`, start)
		check(t, err)
		failSeal := errors.New("seal write failed")
		exporter := exportdb.Exporter{Pool: pool, Archive: archive, Lateness: 10 * time.Minute}
		exporter.Archive.Objects = failedDBSeal{Objects: store, err: failSeal}
		var uploads int
		exporter.Archive.Uploaded = func(context.Context, int64, time.Duration) { uploads++ }
		done, err := exporter.ExportNext(ctx)
		check(t, err)
		if done {
			t.Fatal("exporter dropped unprocessed rows")
		}
		_, err = pool.Exec(ctx, `INSERT INTO jammy.consumer_cursor (consumer,last_xid,last_event_id)
			SELECT 'processor',ingest_xid,event_id FROM jammy.raw_events ORDER BY ingest_xid DESC,event_id DESC LIMIT 1`)
		check(t, err)
		key := seal.Prefix("solar", start.Add(-time.Hour)) + "db-" + partition + ".parquet"
		conflict := bytes.NewReader([]byte("existing object with different bytes"))
		check(t, store.Put(ctx, key, conflict, conflict.Size(), "application/vnd.apache.parquet"))
		blocker, err := pool.Begin(ctx)
		check(t, err)
		defer func() { _ = blocker.Rollback(ctx) }()
		_, err = blocker.Exec(ctx, "LOCK TABLE ONLY jammy.raw_events IN ACCESS SHARE MODE")
		check(t, err)
		done, err = exporter.ExportNext(ctx)
		var lockErr *pgconn.PgError
		if done || !errors.As(err, &lockErr) || lockErr.Code != "55P03" || uploads != 2 {
			t.Fatalf("expected two staged uploads followed by a lock timeout, got done=%v uploads=%d err=%v", done, uploads, err)
		}
		var firstManifest exportdb.Manifest
		check(t, pq.ReadJSON(ctx, store, "exports/direct-db/"+partition+".json", &firstManifest))
		restarted := exportdb.Exporter{Pool: pool, Archive: exporter.Archive, Lateness: exporter.Lateness}
		done, err = restarted.ExportNext(ctx)
		if done || !errors.As(err, &lockErr) || lockErr.Code != "55P03" || uploads != 4 {
			t.Fatalf("restart should overwrite, verify and retry the lock, got done=%v uploads=%d err=%v", done, uploads, err)
		}
		check(t, blocker.Rollback(ctx))
		done, err = exporter.ExportNext(ctx)
		if !done || !errors.Is(err, failSeal) {
			t.Fatalf("expected the closed partition to be exported despite failed seal write, got done=%v err=%v", done, err)
		}
		if uploads != 4 {
			t.Fatalf("lock retry uploaded the partition again: %d uploads", uploads)
		}
		var rows int64
		var exportKey, hash string
		var dropped bool
		check(t, pool.QueryRow(ctx, `SELECT rows,s3_key,sha256,dropped_at IS NOT NULL FROM jammy.archive_exports
			WHERE partition_name=$1`, partition).Scan(&rows, &exportKey, &hash, &dropped))
		var manifest exportdb.Manifest
		check(t, pq.ReadJSON(ctx, store, exportKey, &manifest))
		if !dropped || rows != 2 || manifest.Rows != 2 || len(manifest.Files) != 2 || len(hash) != 64 {
			t.Fatalf("incorrect export record: dropped=%v rows=%d manifest=%+v", dropped, rows, manifest)
		}
		if manifest.ExportedAt != firstManifest.ExportedAt {
			t.Fatal("export retry replaced the original manifest")
		}
		body, err := store.Get(ctx, exportKey)
		check(t, err)
		data, err := io.ReadAll(body)
		check(t, err)
		check(t, body.Close())
		digest := sha256.Sum256(data)
		if hash != hex.EncodeToString(digest[:]) {
			t.Fatal("archive record does not point to the partition manifest's digest")
		}
		for _, ref := range manifest.Files {
			check(t, archive.Verify(ctx, ref))
		}
		var gone bool
		check(t, pool.QueryRow(ctx, "SELECT to_regclass($1) IS NULL", "jammy."+partition).Scan(&gone))
		if !gone {
			t.Fatal("exported partition still attached")
		}
		t.Run("FlushSealAfterRestartWithOpenPartition", func(t *testing.T) {
			sealKey := seal.Prefix("solar", start.Add(-time.Hour)) + "_SEALED-db.json"
			exists, err := store.Exists(ctx, sealKey)
			check(t, err)
			if exists {
				t.Fatal("seal exists after failed write")
			}
			restarted := exportdb.Exporter{Pool: pool, Archive: archive, Lateness: 10 * time.Minute}
			done, err := restarted.ExportNext(ctx)
			check(t, err)
			if done {
				t.Fatal("exported the open partition")
			}
			var doc seal.Document
			check(t, pq.ReadJSON(ctx, store, sealKey, &doc))
			wantFile := seal.Prefix("solar", start.Add(-time.Hour)) + "db-" + partition + ".parquet"
			if doc.Rows != 1 || len(doc.Files) != 1 || doc.Files[0] != wantFile {
				t.Fatalf("incorrect recovered seal: %+v", doc)
			}
		})
	})
	t.Run("TestStreamLoopsFilterAndDrain", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		check(t, err)
		adminPort := listener.Addr().(*net.TCPAddr).Port
		check(t, listener.Close())
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "jetstream")
		t.Setenv("NATS_URL", "nats://"+natsAddress)
		t.Setenv("NATS_STREAMS", "run-a,run-b")
		t.Setenv("S3_BUCKET", bucket)
		t.Setenv("STAGE_DIR", t.TempDir())
		t.Setenv("ADMIN_PORT", strconv.Itoa(adminPort))
		t.Setenv("ARCHIVE_FLUSH_SECONDS", "60")
		cfg, err := load()
		check(t, err)
		for _, stream := range cfg.streams {
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{"jammy.raw." + stream, "ignore." + stream}})
			check(t, err)
		}
		work, cancel := context.WithCancel(t.Context())
		defer cancel()
		stopped := make(chan struct{})
		var runErr error
		go func() { defer close(stopped); runErr = cfg.run(work) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				t.Error("archiver did not finish draining")
			}
		})
		endpoint := fmt.Sprintf("http://127.0.0.1:%d/readyz", adminPort)
		hc := &http.Client{Timeout: time.Second}
		retry(t, 15*time.Second, func() error {
			response, err := hc.Get(endpoint)
			if err != nil {
				return err
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("readyz status %d", response.StatusCode)
			}
			return nil
		})
		want := make(map[string]bool)
		for _, stream := range cfg.streams {
			makeMessage := func(subject, id string) *nats.Msg {
				m := nats.NewMsg(subject)
				evt := &pb.Event{EventId: id, RoutingKey: "test", Profile: "synth-small", SchemaVersion: 1, EventTimeUnixMs: hour.UnixMilli()}
				m.Data, err = proto.Marshal(&pb.EventBatch{BatchId: id, Events: []*pb.Event{evt}})
				check(t, err)
				m.Header.Set("Content-Type", "application/x-protobuf")
				m.Header.Set("Nats-Msg-Id", id)
				m.Header.Set("Jammy-Ingest-Id", stream)
				m.Header.Set("Jammy-Received-At-Ms", strconv.FormatInt(hour.UnixMilli(), 10))
				return m
			}
			_, err := js.PublishMsg(ctx, makeMessage("ignore."+stream, stream+"-ignore"))
			check(t, err)
			ack, err := js.PublishMsg(ctx, makeMessage("jammy.raw."+stream, stream+"-include"))
			check(t, err)
			key := seal.Prefix("synth-small", hour) + fmt.Sprintf("%s-%d-%d.parquet", stream, ack.Sequence, ack.Sequence)
			want[key] = true
			consumer, err := js.Consumer(ctx, stream, cfg.consumer)
			check(t, err)
			retry(t, 5*time.Second, func() error {
				info, err := consumer.Info(ctx)
				if err != nil {
					return err
				}
				if info.Config.FilterSubject != "jammy.raw."+stream || info.NumAckPending != 1 {
					return fmt.Errorf("consumer filter/pending: %q/%d", info.Config.FilterSubject, info.NumAckPending)
				}
				return nil
			})
		}
		cancel()
		select {
		case <-stopped:
			check(t, runErr)
		case <-time.After(10 * time.Second):
			t.Fatal("archiver did not finish draining")
		}
		keys, err := store.List(ctx, seal.Prefix("synth-small", hour))
		check(t, err)
		for _, key := range keys {
			if !strings.Contains(key, "/run-") || !strings.HasSuffix(key, ".parquet") {
				continue
			}
			if !want[key] {
				t.Fatalf("unexpected Parquet object %s", key)
			}
			var manifest pq.Manifest
			check(t, pq.ReadJSON(ctx, store, pq.ManifestKey(key), &manifest))
			if manifest.Rows != 1 || manifest.Version != 1 || manifest.SchemaVersion != 1 || manifest.Source != "jetstream" {
				t.Fatalf("incorrect manifest %+v", manifest)
			}
			check(t, archive.Verify(ctx, pq.Ref{Key: key, Rows: manifest.Rows, SHA256: manifest.SHA256}))
			delete(want, key)
		}
		if len(want) != 0 {
			t.Fatalf("drain did not upload %v", want)
		}
		for _, stream := range cfg.streams {
			consumer, err := js.Consumer(ctx, stream, cfg.consumer)
			check(t, err)
			retry(t, 5*time.Second, func() error {
				info, err := consumer.Info(ctx)
				if err != nil {
					return err
				}
				if info.NumAckPending != 0 {
					return fmt.Errorf("messages remain unacknowledged")
				}
				return nil
			})
		}
	})

	t.Run("TestFlushBatchesSequencesAndRecoversGap", func(t *testing.T) {
		gapConsumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
			Durable: "archive-gap", AckPolicy: jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverNewPolicy, AckWait: time.Minute,
		})
		check(t, err)
		originalConsumer := consumer
		consumer = gapConsumer
		t.Cleanup(func() { consumer = originalConsumer })
		eventHour := hour.Add(24 * time.Hour)
		first, one := publish(t, "gap", "gap-one", eventHour, 1)
		middle, two := publish(t, "gap", "gap-two", eventHour, 1)
		last, three := publish(t, "gap", "gap-three", eventHour, 1)
		worker := &sourcejs.Archiver{Archive: archive, Lateness: time.Hour}
		check(t, worker.Flush(ctx, []jetstream.Msg{three, one}))
		prefix := seal.Prefix("synth-small", eventHour)
		key := prefix + fmt.Sprintf("gap-%d-%d.parquet", first, last)
		manifestKey := pq.ManifestKey(key)
		var manifest pq.Manifest
		check(t, pq.ReadJSON(ctx, store, manifestKey, &manifest))
		if manifest.Rows != 2 || !slices.Equal(manifest.SequenceRanges, [][2]uint64{{first, first}, {last, last}}) {
			t.Fatalf("incorrect grouped manifest: %+v", manifest)
		}
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &manifestKey})
		check(t, err)
		restarted := &sourcejs.Archiver{Archive: archive, Lateness: time.Hour}
		check(t, restarted.Flush(ctx, []jetstream.Msg{three, two, one}))
		rows := read(t, prefix)
		keys, err := store.List(ctx, prefix)
		check(t, err)
		gap := prefix + fmt.Sprintf("gap-%d-%d.parquet", middle, middle)
		if len(rows) != 3 || len(keys) != 4 || !slices.Contains(keys, gap) || !slices.Contains(keys, manifestKey) {
			t.Fatalf("replay did not recover exact coverage: rows=%d keys=%v", len(rows), keys)
		}
	})

	t.Run("TestReuploadOverwritesArchiveObject", func(t *testing.T) {
		seq := uint64(1)
		key := seal.Prefix("synth-small", hour) + fmt.Sprintf("conflict-%d-%d.parquet", seq, seq)
		replacement := bytes.NewReader([]byte("replacement"))
		check(t, store.Put(ctx, key, replacement, replacement.Size(), "application/vnd.apache.parquet"))
		multipart, err := os.CreateTemp(t.TempDir(), "multipart-*.parquet")
		check(t, err)
		defer func() { _ = multipart.Close() }()
		const size = (64 << 20) + 1
		check(t, multipart.Truncate(size))
		check(t, store.Put(ctx, key, multipart, size, "application/vnd.apache.parquet"))
		stream := "raw-test"
		ref, manifest, err := archive.Write(ctx, key, pq.Rows([]pq.Row{{
			EventID: "original", RoutingKey: "asset", Profile: "synth-small", SchemaVersion: 1,
			EventTime: hour.UnixMilli(), ReceivedAt: hour.UnixMilli(), IngestID: "conflict",
		}}), pq.Manifest{Source: "jetstream", Stream: &stream, FirstSeq: &seq, LastSeq: &seq, SequenceRanges: [][2]uint64{{seq, seq}}})
		check(t, err)
		check(t, archive.Verify(ctx, ref))
		if manifest.Rows != 1 {
			t.Fatalf("manifest rows = %d", manifest.Rows)
		}

	})

}

type failedDBSeal struct {
	pq.Objects
	err error
}

func (s failedDBSeal) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, media string) error {
	if strings.HasSuffix(key, "/_SEALED-db.json") {
		return s.err
	}
	return s.Objects.Put(ctx, key, body, size, media)
}

type blockedManifest struct{ pq.Objects }

func (b blockedManifest) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, media string) error {
	if strings.HasSuffix(key, ".manifest.json") {
		<-ctx.Done()
		return ctx.Err()
	}
	return b.Objects.Put(ctx, key, body, size, media)
}

func TestArchiveCrashHelper(t *testing.T) {
	if os.Getenv("ARCHIVE_CRASH_FIXTURE") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	client, err := s3x.New(ctx)
	check(t, err)
	store := pq.S3{Client: client, Bucket: os.Getenv("ARCHIVE_TEST_BUCKET")}
	nc, err := nats.Connect(os.Getenv("ARCHIVE_TEST_NATS"))
	check(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	check(t, err)
	consumer, err := js.Consumer(ctx, "raw-test", "archiver")
	check(t, err)
	msg, err := consumer.Next(jetstream.FetchMaxWait(10 * time.Second))
	check(t, err)
	worker := sourcejs.Archiver{Archive: pq.Archive{Objects: blockedManifest{store}, Dir: os.Getenv("ARCHIVE_TEST_STAGE"), Level: 1}}
	check(t, worker.Flush(ctx, []jetstream.Msg{msg}))
}

func TestArchiveSettings(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "jetstream")
	t.Setenv("NATS_STREAMS", "raw-a,raw-b")
	t.Setenv("S3_BUCKET", "archive")
	t.Setenv("ARCHIVE_FLUSH_SECONDS", "7")
	cfg, err := load()
	check(t, err)
	if cfg.ackWait != 81*time.Second || cfg.flushBytes != 64<<20 || cfg.zstdLevel != 1 || len(cfg.streams) != 2 {
		t.Fatalf("defaults: %+v", cfg)
	}
	t.Setenv("ACK_WAIT_SECONDS", "17")
	cfg, err = load()
	check(t, err)
	if cfg.ackWait != 17*time.Second {
		t.Fatalf("ack wait=%s", cfg.ackWait)
	}
	for name, value := range map[string]string{
		"ADMIN_PORT": "0", "HTTP_PORT": "65536", "CONSUMER_NAME": "", "INSTANCE_ID": "",
		"STAGE_DIR": "", "S3_BUCKET": "", "ARCHIVE_FLUSH_SECONDS": "0", "ARCHIVE_FLUSH_BYTES": "0",
		"ARCHIVE_ZSTD_LEVEL": "0", "ACK_WAIT_SECONDS": "0",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := load(); err == nil {
				t.Fatalf("accepted %s=%q", name, value)
			}
		})
	}
}

func TestArchiveAckWaitDefaultOverflow(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("S3_BUCKET", "archive")
	t.Setenv("ARCHIVE_FLUSH_SECONDS", "4000000000")
	for _, name := range []string{"ACK_WAIT_SECONDS", "ACK_WAIT_SECONDS_FILE"} {
		t.Setenv(name, "")
		check(t, os.Unsetenv(name))
	}

	if _, err := load(); err == nil || err.Error() != "ARCHIVE_FLUSH_SECONDS is too large to derive ACK_WAIT_SECONDS" {
		t.Fatalf("unexpected overflow error: %v", err)
	}

	t.Setenv("ACK_WAIT_SECONDS", "17")
	cfg, err := load()
	check(t, err)
	if cfg.ackWait != 17*time.Second {
		t.Fatalf("ack wait=%s, want 17s", cfg.ackWait)
	}

	path := filepath.Join(t.TempDir(), "ack-wait")
	t.Setenv("ACK_WAIT_SECONDS_FILE", path)
	check(t, os.WriteFile(path, []byte("19\n"), 0600))
	cfg, err = load()
	check(t, err)
	if cfg.ackWait != 19*time.Second {
		t.Fatalf("ack wait=%s, want 19s", cfg.ackWait)
	}

	check(t, os.WriteFile(path, nil, 0600))
	if _, err := load(); err == nil || err.Error() != "invalid ACK_WAIT_SECONDS" {
		t.Fatalf("unexpected empty override error: %v", err)
	}
}
