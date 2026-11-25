package exportdb

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/seal"
	"github.com/cloud4-final/jammy-5th/libs/go/s3x"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	partitionFormat   = "raw_events_2006010215"
	advisoryLockQuery = `SELECT pg_try_advisory_xact_lock(hashtext('jammy'), hashtext('archive_exports'))`
)

type Manifest struct {
	Partition  string   `json:"partition_name"`
	Files      []pq.Ref `json:"files"`
	Rows       int64    `json:"rows"`
	ExportedAt string   `json:"exported_at"`
}

type Exporter struct {
	Pool       *pgxpool.Pool
	Archive    pq.Archive
	Lateness   time.Duration
	KeyPrefix  string
	index      *seal.Index
	lastExport string
	pending    *preparedExport
}

type stageGroup struct {
	profile string
	hour    time.Time
	version int32
}

type stagedFile struct {
	ref      pq.Ref
	manifest pq.Manifest
}

type partitionStatus struct {
	oid          uint32
	xid, eventID string
	rows         int64
}

type preparedExport struct {
	status partitionStatus
	part   Manifest
	files  []stagedFile
}

func (e *Exporter) ExportNext(ctx context.Context) (bool, error) {
	var exported, flushed bool
	var prepared *preparedExport
	err := pgx.BeginFunc(ctx, e.Pool, func(tx pgx.Tx) error {
		var lock bool
		if err := tx.QueryRow(ctx, advisoryLockQuery).Scan(&lock); err != nil || !lock {
			return err
		}
		retry := e.pending
		e.pending = nil
		if err := e.syncIndex(ctx, tx); err != nil {
			return err
		}
		name, err := oldest(ctx, tx)
		if err != nil {
			return err
		}
		if name == "" {
			// Retry pending seals while this transaction still holds the exporter lock.
			flushed = true
			return e.flush(ctx, tx, "")
		}
		hour, err := time.Parse(partitionFormat, name)
		if err != nil {
			return err
		}
		if time.Now().Before(hour.Add(time.Hour)) {
			// Retry pending seals while this transaction still holds the exporter lock.
			flushed = true
			return e.flush(ctx, tx, name)
		}
		relation := pgx.Identifier{"jammy", name}.Sanitize()
		current, err := status(ctx, tx, relation)
		if err != nil {
			return err
		}
		if current.rows > 0 {
			var processed bool
			err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT FROM jammy.consumer_cursor)
				AND NOT EXISTS(SELECT FROM jammy.consumer_cursor WHERE (last_xid,last_event_id) < ($1::text::xid8,$2))
				AND $1::text::xid8 < pg_snapshot_xmin(pg_current_snapshot())`, current.xid, current.eventID).Scan(&processed)
			if err != nil || !processed {
				return err
			}
		}
		if retry != nil && retry.part.Partition == name && retry.status == current {
			// The previous upload survived rollback. Recheck its objects in
			// case another exporter acquired the advisory lock in between.
			for _, file := range retry.files {
				if err := e.Archive.Verify(ctx, file.ref); err != nil {
					return err
				}
			}
			prepared = retry
		} else {
			prepared, err = e.prepare(ctx, tx, name, relation, current)
			if err != nil {
				return err
			}
		}
		key := e.exportKey(name)
		digest, err := pq.PutJSON(ctx, e.Archive.Objects, key, &prepared.part, func(existing Manifest) bool {
			return existing.Partition == prepared.part.Partition && existing.Rows == prepared.part.Rows &&
				slices.Equal(existing.Files, prepared.part.Files)
		})
		if errors.Is(err, pq.ErrManifestConflict) {
			// The partition is still attached and the exporter lock is held, so
			// this manifest is from an earlier, uncommitted attempt.
			digest, err = pq.PutJSON(ctx, e.Archive.Objects, key, &prepared.part, nil)
		}
		if err != nil {
			return err
		}
		// Acquire the parent lock only for the final recheck and DDL. Ingest
		// can write to current partitions while Parquet is built and verified.
		if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='2s'; LOCK TABLE ONLY jammy.raw_events IN ACCESS EXCLUSIVE MODE`); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "LOCK TABLE "+relation+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return err
		}
		after, err := status(ctx, tx, relation)
		if err != nil {
			return err
		}
		if after != current {
			return errors.New("partition changed while uploading")
		}
		_, err = tx.Exec(ctx, `INSERT INTO jammy.archive_exports (partition_name,s3_key,rows,sha256,exported_at,dropped_at)
			VALUES($1,$2,$3,$4,$5::text::timestamptz,now())
			ON CONFLICT (partition_name) DO UPDATE SET s3_key=EXCLUDED.s3_key,rows=EXCLUDED.rows,sha256=EXCLUDED.sha256,
			exported_at=EXCLUDED.exported_at,dropped_at=EXCLUDED.dropped_at`, name, key, current.rows, digest, prepared.part.ExportedAt)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "ALTER TABLE jammy.raw_events DETACH PARTITION "+relation); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "DROP TABLE "+relation); err != nil {
			return err
		}
		exported = true
		return nil
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if prepared != nil && errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			e.pending = prepared
		}
		// Reload after ambiguous commit results and on another exporter taking
		// the lock, so the next pass sees the authoritative DB state.
		e.index = nil
		return false, err
	}
	if e.index == nil || flushed {
		return false, nil
	}
	if exported {
		for _, file := range prepared.files {
			if err := e.index.Record(file.ref, file.manifest, "db"); err != nil {
				e.index = nil
				return exported, err
			}
		}
		e.lastExport = prepared.part.Partition
	}
	return exported, e.flushCommitted(ctx)
}

// syncIndex runs with the exporter lock held. Partitions export in order, so
// a changed last export indicates another worker updated the durable catalog.
func (e *Exporter) syncIndex(ctx context.Context, tx pgx.Tx) error {
	var latest string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(partition_name),'') FROM jammy.archive_exports
		WHERE dropped_at IS NOT NULL`).Scan(&latest); err != nil {
		return err
	}
	if e.index != nil && latest == e.lastExport {
		return nil
	}
	archived, err := archivedPartitions(ctx, tx)
	if err != nil {
		return err
	}
	index, err := seal.LoadWithPrefix(ctx, e.Archive.Objects, e.KeyPrefix, func(m pq.Manifest) bool {
		return m.Source == "direct-db" && m.Partition != nil && archived[*m.Partition]
	})
	if err != nil {
		return err
	}
	e.index, e.lastExport = index, latest
	return nil
}

func (e *Exporter) exportKey(partition string) string {
	return s3x.NormalizePrefix(e.KeyPrefix) + "exports/direct-db/" + partition + ".json"
}

func (e *Exporter) prepare(ctx context.Context, tx pgx.Tx, name, relation string, current partitionStatus) (*preparedExport, error) {
	groups, err := partGroups(ctx, tx, relation)
	if err != nil {
		return nil, err
	}
	export := &preparedExport{
		status: current,
		part:   Manifest{Partition: name, Files: make([]pq.Ref, 0, len(groups)), Rows: current.rows, ExportedAt: time.Now().UTC().Format(pq.Stamp)},
	}
	var archivedRows int64
	for _, group := range groups {
		key := e.index.Target(seal.PrefixWith(e.KeyPrefix, group.profile, group.hour)+"db-"+name+".parquet", "db")
		ref, manifest, err := e.Archive.Write(ctx, key, streamRows(ctx, tx, relation, group), pq.Manifest{Source: "direct-db", Partition: &name})
		if err != nil {
			return nil, err
		}
		export.part.Files = append(export.part.Files, ref)
		export.files = append(export.files, stagedFile{ref, manifest})
		archivedRows += ref.Rows
	}
	if archivedRows != current.rows {
		return nil, errors.New("partition changed during export")
	}
	return export, nil
}

func (e *Exporter) flushCommitted(ctx context.Context) error {
	return pgx.BeginFunc(ctx, e.Pool, func(tx pgx.Tx) error {
		var lock bool
		if err := tx.QueryRow(ctx, advisoryLockQuery).Scan(&lock); err != nil || !lock {
			return err
		}
		if err := e.syncIndex(ctx, tx); err != nil {
			return err
		}
		pending, err := oldest(ctx, tx)
		if err != nil {
			return err
		}
		return e.flush(ctx, tx, pending)
	})
}

func (e *Exporter) flush(ctx context.Context, tx pgx.Tx, pending string) error {
	var last string
	err := tx.QueryRow(ctx, `SELECT COALESCE(max(partition_name),'') FROM jammy.archive_exports
		WHERE dropped_at IS NOT NULL AND ($1='' OR partition_name < $1)`, pending).Scan(&last)
	if err != nil || last == "" {
		return err
	}
	hour, err := time.Parse(partitionFormat, last)
	if err != nil {
		return err
	}
	return e.index.Flush(ctx, "db", hour.Add(time.Hour), e.Lateness, true)
}

func oldest(ctx context.Context, tx pgx.Tx) (string, error) {
	var name string
	err := tx.QueryRow(ctx, `SELECT COALESCE(min(c.relname),'') FROM pg_inherits i
		JOIN pg_class c ON c.oid=i.inhrelid
		WHERE i.inhparent='jammy.raw_events'::regclass AND c.relname ~ '^raw_events_[0-9]{10}$'`).Scan(&name)
	return name, err
}

func archivedPartitions(ctx context.Context, tx pgx.Tx) (map[string]bool, error) {
	rows, err := tx.Query(ctx, `SELECT partition_name FROM jammy.archive_exports WHERE dropped_at IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		found[name] = true
	}
	return found, rows.Err()
}

func status(ctx context.Context, tx pgx.Tx, relation string) (partitionStatus, error) {
	var current partitionStatus
	query := fmt.Sprintf(`SELECT to_regclass($1)::oid, COALESCE(last.xid,'0'), COALESCE(last.event_id,''),
		(SELECT count(*) FROM %[1]s)
		FROM (SELECT 1) seed LEFT JOIN LATERAL (
			SELECT r.ingest_xid::text AS xid, r.event_id FROM %[1]s r
			ORDER BY r.ingest_xid DESC, r.event_id DESC LIMIT 1
		) last ON true`, relation)
	err := tx.QueryRow(ctx, query, relation).Scan(&current.oid, &current.xid, &current.eventID, &current.rows)
	return current, err
}

func partGroups(ctx context.Context, tx pgx.Tx, relation string) ([]stageGroup, error) {
	query := `SELECT DISTINCT profile,date_trunc('hour',event_time,'UTC'),schema_version FROM ` + relation + ` ORDER BY 1,2,3`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []stageGroup
	for rows.Next() {
		var g stageGroup
		if err := rows.Scan(&g.profile, &g.hour, &g.version); err != nil {
			return nil, err
		}
		if len(groups) > 0 {
			prior := groups[len(groups)-1]
			if prior.profile == g.profile && prior.hour.Equal(g.hour) {
				return nil, errors.New("multiple schema versions share one direct DB archive key")
			}
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

func streamRows(ctx context.Context, tx pgx.Tx, relation string, g stageGroup) iter.Seq2[pq.Row, error] {
	return func(yield func(pq.Row, error) bool) {
		query := `SELECT event_id,routing_key,profile,schema_version,event_time,received_at,ingest_id,primary_value,payload
			FROM ` + relation + ` WHERE profile=$1 AND event_time >= $2 AND event_time < $3 AND schema_version=$4
			ORDER BY ingest_xid,event_id`
		rows, err := tx.Query(ctx, query, g.profile, g.hour, g.hour.Add(time.Hour), g.version)
		if err != nil {
			yield(pq.Row{}, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var row pq.Row
			var eventTime, received time.Time
			err := rows.Scan(&row.EventID, &row.RoutingKey, &row.Profile, &row.SchemaVersion, &eventTime, &received, &row.IngestID, &row.PrimaryValue, &row.Payload)
			if err != nil {
				yield(pq.Row{}, err)
				return
			}
			row.EventTime, row.ReceivedAt = eventTime.UnixMilli(), received.UnixMilli()
			if !yield(row, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(pq.Row{}, err)
		}
	}
}
