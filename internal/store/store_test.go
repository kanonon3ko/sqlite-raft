package store

import (
	"context"
	"testing"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/rewrite"
)

func TestApplyAndQuery(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	if _, err := s.ApplyEntry(ctx, 1, &logpb.LogEntry{
		Index: 1,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{Sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)"},
			},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	outcome, err := s.ApplyEntry(ctx, 2, &logpb.LogEntry{
		Index: 2,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{
					Sql: "INSERT INTO kv (k, v) VALUES (?, ?)",
					Params: []*sqlraftpb.Value{
						{Kind: &sqlraftpb.Value_S{S: "answer"}},
						{Kind: &sqlraftpb.Value_I{I: 42}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if outcome.RowsAffected != 1 {
		t.Fatalf("rows affected = %d, want 1", outcome.RowsAffected)
	}
	if outcome.LastInsertID != 1 {
		t.Fatalf("last insert id = %d, want 1（TEXT 主键表的隐式 rowid）", outcome.LastInsertID)
	}

	idx, err := s.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if idx != 2 {
		t.Fatalf("applied index = %d, want 2", idx)
	}

	columns, result, err := s.Query(ctx, "SELECT k, v FROM kv", nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(columns) != 2 {
		t.Fatalf("columns = %v, want 2", columns)
	}
	if len(result) != 1 {
		t.Fatalf("rows = %d, want 1", len(result))
	}
	row := result[0]
	if row.Values[0].GetS() != "answer" || row.Values[1].GetI() != 42 {
		t.Fatalf("unexpected row: %v", row)
	}
}

func TestApplyEntryNoop(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Noop 也要推进 appliedIndex，保证恢复后不会重复应用后续真实条目
	if _, err := s.ApplyEntry(context.Background(), 5, &logpb.LogEntry{Index: 5}); err != nil {
		t.Fatalf("apply noop: %v", err)
	}
	idx, err := s.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if idx != 5 {
		t.Fatalf("applied index = %d, want 5", idx)
	}
}

func TestAppliedIndexPersistsAcrossReopen(t *testing.T) {
	path := t.TempDir() + "/test.db"
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.ApplyEntry(context.Background(), 1, &logpb.LogEntry{
		Index: 1,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{Sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)"},
			},
		},
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	idx, err := s2.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if idx != 1 {
		t.Fatalf("applied index = %d, want 1", idx)
	}
	_, rows, err := s2.Query(context.Background(), "SELECT v FROM kv WHERE k='answer'", nil)
	if err != nil || len(rows) != 0 {
		t.Fatalf("unexpected state: err=%v rows=%v", err, rows)
	}
}

func TestTableInfoAutoIncrement(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	if _, err := s.ApplyEntry(ctx, 1, &logpb.LogEntry{
		Index: 1,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{
					Sql: `CREATE TABLE t (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						name TEXT NOT NULL,
						ts INTEGER DEFAULT 0
					)`,
				},
			},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	info, err := s.TableInfo("t")
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	if info == nil || info.AutoIncrement != "id" {
		t.Fatalf("table info = %+v, want autoincrement id", info)
	}
	if len(info.Columns) != 3 || info.Columns[0] != "id" || info.Columns[2] != "ts" {
		t.Fatalf("columns = %v", info.Columns)
	}

	next, err := s.NextAutoIncrement("t")
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if next != 1 {
		t.Fatalf("next = %d, want 1", next)
	}

	// 插入后 sqlite_sequence 更新，next 递增
	if _, err := s.ApplyEntry(ctx, 2, &logpb.LogEntry{
		Index: 2,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{
					Sql: `INSERT INTO t (id, name) VALUES (1, 'a')`,
				},
			},
		},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	next, err = s.NextAutoIncrement("t")
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if next != 2 {
		t.Fatalf("next = %d, want 2", next)
	}
}

func TestTableInfoNoAutoIncrement(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if _, err := s.ApplyEntry(context.Background(), 1, &logpb.LogEntry{
		Index: 1,
		Command: &logpb.DeterministicCommand{
			Cmd: &logpb.DeterministicCommand_Exec{
				Exec: &logpb.Exec{Sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"},
			},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	info, err := s.TableInfo("t")
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	if info.AutoIncrement != "" {
		t.Fatalf("unexpected autoincrement %q", info.AutoIncrement)
	}
}

// 确保 store 实现 rewrite.Schema 接口
var _ rewrite.Schema = (*Store)(nil)
