package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	kvpb "madkv/kvstore/gen/kvpb"
	_ "modernc.org/sqlite"
)

func TestKVServerDurabilityAndReplay(t *testing.T) {
	ctx := context.Background()
	backerDir := t.TempDir()

	srv, err := newKVServer(backerDir)
	if err != nil {
		t.Fatalf("newKVServer() failed: %v", err)
	}
	defer srv.db.Close()

	if got, err := srv.Put(ctx, &kvpb.PutRequest{Key: "k1", Value: "v1"}); err != nil || got.Found {
		t.Fatalf("Put(k1,v1) = (%v, %v), want found=false and nil err", got, err)
	}
	if got, err := srv.Put(ctx, &kvpb.PutRequest{Key: "k1", Value: "v2"}); err != nil || !got.Found {
		t.Fatalf("Put(k1,v2) = (%v, %v), want found=true and nil err", got, err)
	}
	if got, err := srv.Swap(ctx, &kvpb.SwapRequest{Key: "k1", Value: "v3"}); err != nil || !got.Found || got.OldValue != "v2" {
		t.Fatalf("Swap(k1,v3) = (%v, %v), want found=true old=v2", got, err)
	}
	if got, err := srv.Swap(ctx, &kvpb.SwapRequest{Key: "k2", Value: "v9"}); err != nil || got.Found {
		t.Fatalf("Swap(k2,v9) = (%v, %v), want found=false and nil err", got, err)
	}
	if got, err := srv.Delete(ctx, &kvpb.DeleteRequest{Key: "k1"}); err != nil || !got.Found {
		t.Fatalf("Delete(k1) = (%v, %v), want found=true and nil err", got, err)
	}
	if got, err := srv.Delete(ctx, &kvpb.DeleteRequest{Key: "missing"}); err != nil || got.Found {
		t.Fatalf("Delete(missing) = (%v, %v), want found=false and nil err", got, err)
	}
	if _, err := srv.Put(ctx, &kvpb.PutRequest{Key: "a", Value: "1"}); err != nil {
		t.Fatalf("Put(a,1) err: %v", err)
	}
	if _, err := srv.Put(ctx, &kvpb.PutRequest{Key: "b", Value: "2"}); err != nil {
		t.Fatalf("Put(b,2) err: %v", err)
	}

	var logRows int
	if err := srv.db.QueryRow(`SELECT COUNT(*) FROM wal_log`).Scan(&logRows); err != nil {
		t.Fatalf("count wal_log rows: %v", err)
	}
	if logRows != 8 {
		t.Fatalf("wal_log row count = %d, want 8", logRows)
	}

	if err := srv.db.Close(); err != nil {
		t.Fatalf("close first server db: %v", err)
	}

	srv2, err := newKVServer(backerDir)
	if err != nil {
		t.Fatalf("newKVServer(replay) failed: %v", err)
	}
	defer srv2.db.Close()

	if got, err := srv2.Get(ctx, &kvpb.GetRequest{Key: "k1"}); err != nil || got.Found {
		t.Fatalf("Get(k1) after replay = (%v, %v), want found=false", got, err)
	}
	if got, err := srv2.Get(ctx, &kvpb.GetRequest{Key: "k2"}); err != nil || !got.Found || got.Value != "v9" {
		t.Fatalf("Get(k2) after replay = (%v, %v), want found=true value=v9", got, err)
	}
	if got, err := srv2.Get(ctx, &kvpb.GetRequest{Key: "a"}); err != nil || !got.Found || got.Value != "1" {
		t.Fatalf("Get(a) after replay = (%v, %v), want found=true value=1", got, err)
	}
	if got, err := srv2.Get(ctx, &kvpb.GetRequest{Key: "b"}); err != nil || !got.Found || got.Value != "2" {
		t.Fatalf("Get(b) after replay = (%v, %v), want found=true value=2", got, err)
	}
}

func TestWALPayloadIsProtobuf(t *testing.T) {
	ctx := context.Background()
	backerDir := t.TempDir()

	srv, err := newKVServer(backerDir)
	if err != nil {
		t.Fatalf("newKVServer() failed: %v", err)
	}
	defer srv.db.Close()

	if _, err := srv.Put(ctx, &kvpb.PutRequest{Key: "pkey", Value: "pval"}); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	var payload []byte
	if err := srv.db.QueryRow(`SELECT payload FROM wal_log ORDER BY seq DESC LIMIT 1`).Scan(&payload); err != nil {
		t.Fatalf("read payload row: %v", err)
	}
	if len(payload) == 0 {
		t.Fatalf("payload is empty")
	}

	var cmd kvpb.WALCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		t.Fatalf("payload is not valid protobuf WALCommand: %v", err)
	}
	if cmd.Op != kvpb.WALCommand_OP_PUT || cmd.Key != "pkey" || cmd.Value != "pval" {
		t.Fatalf("decoded cmd = %+v, want op=PUT key=pkey value=pval", cmd)
	}
}

func TestRecoveryFromPreexistingBackerDir(t *testing.T) {
	ctx := context.Background()
	backerDir := t.TempDir()

	db, err := sql.Open("sqlite", filepath.Join(backerDir, dbFileName))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if _, err := db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = FULL;
		CREATE TABLE IF NOT EXISTS wal_log (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			payload BLOB NOT NULL
		);
	`); err != nil {
		_ = db.Close()
		t.Fatalf("initialize schema: %v", err)
	}

	seed := []*kvpb.WALCommand{
		{Op: kvpb.WALCommand_OP_PUT, Key: "alpha", Value: "1"},
		{Op: kvpb.WALCommand_OP_PUT, Key: "beta", Value: "2"},
		{Op: kvpb.WALCommand_OP_DELETE, Key: "alpha"},
	}
	for _, cmd := range seed {
		payload, err := proto.Marshal(cmd)
		if err != nil {
			_ = db.Close()
			t.Fatalf("marshal seed cmd: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO wal_log(payload) VALUES(?)`, payload); err != nil {
			_ = db.Close()
			t.Fatalf("insert seed payload: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	srv, err := newKVServer(backerDir)
	if err != nil {
		t.Fatalf("newKVServer() replay from existing directory failed: %v", err)
	}
	defer srv.db.Close()

	if got, err := srv.Get(ctx, &kvpb.GetRequest{Key: "alpha"}); err != nil || got.Found {
		t.Fatalf("Get(alpha) after startup replay = (%v, %v), want found=false", got, err)
	}
	if got, err := srv.Get(ctx, &kvpb.GetRequest{Key: "beta"}); err != nil || !got.Found || got.Value != "2" {
		t.Fatalf("Get(beta) after startup replay = (%v, %v), want found=true value=2", got, err)
	}
}
