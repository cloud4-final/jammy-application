package seal

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
)

func Eligible(hour, watermark time.Time, lateness time.Duration, direct bool) bool {
	delay := lateness
	if direct {
		delay = (lateness / time.Hour) * time.Hour
		if lateness%time.Hour != 0 {
			delay += time.Hour
		}
		return !watermark.Before(hour.Add(time.Hour).Add(delay))
	}
	return watermark.After(hour.Add(time.Hour).Add(delay))
}

func Prefix(profile string, ts time.Time) string {
	return "raw/profile=" + profile + "/dt=" + ts.UTC().Format("2006-01-02") + "/hour=" + ts.UTC().Format("15") + "/"
}

type hourKey struct {
	prefix, owner string
}
type hour struct {
	time   time.Time
	files  map[string]pq.Ref
	sealed bool
}
type Index struct {
	Store     pq.Objects
	Hours     map[hourKey]*hour
	Refs      map[string]pq.Ref
	watermark map[string]time.Time
}

type Document struct {
	Files     []string `json:"files"`
	Rows      int64    `json:"rows"`
	Watermark string   `json:"watermark"`
	SealedAt  string   `json:"sealed_at"`
}

func Load(ctx context.Context, store pq.Objects, include func(pq.Manifest) bool) (*Index, error) {
	index := &Index{Store: store, Hours: make(map[hourKey]*hour), Refs: make(map[string]pq.Ref), watermark: make(map[string]time.Time)}
	for _, prefix := range []string{"raw/", "raw-late/"} {
		keys, err := store.List(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			folder, name, ok := split(key)
			if !ok {
				continue
			}
			switch {
			case prefix == "raw/" && strings.HasPrefix(name, "_SEALED-") && strings.HasSuffix(name, ".json"):
				owner := strings.TrimSuffix(strings.TrimPrefix(name, "_SEALED-"), ".json")
				h, err := index.hour(folder, owner)
				if err != nil {
					return nil, err
				}
				h.sealed = true
			case strings.HasSuffix(name, ".manifest.json"):
				var manifest pq.Manifest
				if err := pq.ReadJSON(ctx, store, key, &manifest); err != nil {
					return nil, err
				}
				if include != nil && !include(manifest) {
					continue
				}
				owner := "db"
				if manifest.Source == "jetstream" {
					if manifest.FirstSeq == nil || manifest.LastSeq == nil {
						return nil, errors.New("invalid JetStream manifest bounds")
					}
					owner = strings.TrimSuffix(name, fmt.Sprintf("-%d-%d.manifest.json", *manifest.FirstSeq, *manifest.LastSeq))
				}
				ref := pq.Ref{Key: strings.TrimSuffix(key, ".manifest.json") + ".parquet", Rows: manifest.Rows, SHA256: manifest.SHA256}
				if err := index.Record(ref, manifest, owner); err != nil {
					return nil, err
				}
			}
		}
	}
	return index, nil
}

func split(key string) (string, string, bool) {
	index := strings.LastIndexByte(key, '/')
	if index < 0 {
		return "", "", false
	}
	return key[:index+1], key[index+1:], true
}

func (i *Index) hour(prefix, owner string) (*hour, error) {
	key := hourKey{prefix, owner}
	if h := i.Hours[key]; h != nil {
		return h, nil
	}
	components := strings.Split(strings.TrimSuffix(prefix, "/"), "/")
	if len(components) != 4 {
		return nil, errors.New("invalid archive prefix")
	}
	ts, err := time.Parse("dt=2006-01-02/hour=15", components[2]+"/"+components[3])
	if err != nil {
		return nil, err
	}
	h := &hour{time: ts, files: make(map[string]pq.Ref)}
	i.Hours[key] = h
	return h, nil
}

func (i *Index) Target(key, owner string) string {
	if _, exists := i.Refs[key]; exists {
		return key
	}
	prefix, _, _ := split(key)
	if h := i.Hours[hourKey{prefix, owner}]; h != nil && h.sealed {
		return "raw-late/" + strings.TrimPrefix(key, "raw/")
	}
	return key
}

func (i *Index) Record(ref pq.Ref, manifest pq.Manifest, owner string) error {
	maxTime, err := time.Parse(pq.Stamp, manifest.MaxTime)
	if err != nil {
		return err
	}
	if maxTime.After(i.watermark[owner]) {
		i.watermark[owner] = maxTime
	}
	i.Refs[ref.Key] = ref
	if strings.HasPrefix(ref.Key, "raw/") {
		prefix, _, _ := split(ref.Key)
		h, err := i.hour(prefix, owner)
		if err != nil {
			return err
		}
		h.files[ref.Key] = ref
	}
	return nil
}

func (i *Index) Flush(ctx context.Context, owner string, watermark time.Time, lateness time.Duration, direct bool) error {
	if !direct {
		watermark = i.watermark[owner]
	}
	for key, h := range i.Hours {
		if key.owner != owner || h.sealed || len(h.files) == 0 || !Eligible(h.time, watermark, lateness, direct) {
			continue
		}
		files := slices.Sorted(maps.Keys(h.files))
		var rows int64
		for _, file := range files {
			rows += h.files[file].Rows
		}
		doc := Document{Files: files, Rows: rows, Watermark: watermark.UTC().Format(pq.Stamp), SealedAt: time.Now().UTC().Format(pq.Stamp)}
		if _, err := pq.PutJSON(ctx, i.Store, key.prefix+"_SEALED-"+owner+".json", &doc, func(existing Document) bool {
			return existing.Rows == doc.Rows && existing.Watermark == doc.Watermark && slices.Equal(existing.Files, doc.Files)
		}); err != nil {
			return err
		}
		h.sealed = true
	}
	return nil
}
