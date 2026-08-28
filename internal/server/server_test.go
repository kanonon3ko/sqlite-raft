package server

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/raftpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

func outcomeColumnNames(o *store.ApplyOutcome) []string {
	var out []string
	for _, c := range o.Columns {
		out = append(out, c.Name)
	}
	return out
}

func outcomeColumnTypes(o *store.ApplyOutcome) []string {
	var out []string
	for _, c := range o.Columns {
		out = append(out, c.Type)
	}
	return out
}

// TestSingleNodeExecuteQuery 覆盖 M0 的主链路：
// gRPC Execute -> Raft 提案/提交/应用 -> SQLite；Query -> 读取。
func TestSingleNodeExecuteQuery(t *testing.T) {
	st, err := store.Open("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	node, err := raft.New(raft.Config{
		ID:                0,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			return &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
				Columns:      outcomeColumnNames(outcome),
				RowTypes:     outcomeColumnTypes(outcome),
				Rows:         outcome.Rows,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(gs, node)
	sqlraftpb.RegisterSqlRaftServer(gs, New(node, st, nil))
	go gs.Serve(lis)
	node.Start()
	defer func() {
		node.Stop()
		gs.Stop()
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()
	client := sqlraftpb.NewSqlRaftClient(conn)

	// 单节点：等自己当选 Leader
	deadline := time.Now().Add(3 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx := context.Background()
	if _, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{Sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	resp, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "INSERT INTO kv (k, v) VALUES (?, ?)",
		Params: []*sqlraftpb.Value{
			{Kind: &sqlraftpb.Value_S{S: "answer"}},
			{Kind: &sqlraftpb.Value_I{I: 42}},
		},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if resp.RowsAffected != 1 {
		t.Fatalf("rows affected = %d, want 1", resp.RowsAffected)
	}
	if resp.Error != "" {
		t.Fatalf("execute error: %s", resp.Error)
	}

	q, err := client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT k, v FROM kv"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if q.Error != "" {
		t.Fatalf("query error: %s", q.Error)
	}
	if len(q.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(q.Rows))
	}
	if got := q.Rows[0].Values[0].GetS(); got != "answer" {
		t.Fatalf("k = %q, want answer", got)
	}
	if got := q.Rows[0].Values[1].GetI(); got != 42 {
		t.Fatalf("v = %d, want 42", got)
	}
}

// startSingleNode 启动单节点 sqlraft 并返回客户端句柄。
func startSingleNode(t *testing.T) (sqlraftpb.SqlRaftClient, *raft.Node, *store.Store) {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	node, err := raft.New(raft.Config{
		ID:                0,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			return &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
				Columns:      outcomeColumnNames(outcome),
				RowTypes:     outcomeColumnTypes(outcome),
				Rows:         outcome.Rows,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(gs, node)
	sqlraftpb.RegisterSqlRaftServer(gs, New(node, st, nil))
	go gs.Serve(lis)
	node.Start()
	t.Cleanup(func() {
		node.Stop()
		gs.Stop()
		st.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	deadline := time.Now().Add(3 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return sqlraftpb.NewSqlRaftClient(conn), node, st
}

// TestExecuteDeterministicRewrite 验证 NOW()/RANDOM() 在写入日志前被改写为字面量，
// 查询结果不是函数调用而是确定值。
func TestExecuteDeterministicRewrite(t *testing.T) {
	client, _, _ := startSingleNode(t)
	ctx := context.Background()

	if _, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE t (ts TEXT, r INTEGER)",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "INSERT INTO t (ts, r) VALUES (NOW(), RANDOM())",
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	q, err := client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT ts, r FROM t"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(q.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(q.Rows))
	}
	ts := q.Rows[0].Values[0].GetS()
	if len(ts) < 19 || ts[4] != '-' || ts[10] != ' ' {
		t.Fatalf("timestamp not a literal: %q", ts)
	}
	// RANDOM() 改写后必须是整数（非空）
	if q.Rows[0].Values[1].Kind == nil {
		t.Fatalf("random value missing: %v", q.Rows[0])
	}
}

// TestExecuteAutoIncrement 验证 AUTOINCREMENT 预分配：省略自增列的 INSERT
// 由 Leader 显式生成 ID，返回的 LastInsertId 递增。
func TestExecuteAutoIncrement(t *testing.T) {
	client, _, _ := startSingleNode(t)
	ctx := context.Background()

	if _, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, want := range []int64{1, 2, 3} {
		resp, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv (v) VALUES ('x')",
		})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if resp.LastInsertId != want {
			t.Fatalf("insert %d: last_insert_id = %d, want %d", i, resp.LastInsertId, want)
		}
	}

	q, err := client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT id FROM kv ORDER BY id"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(q.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(q.Rows))
	}
	for i, row := range q.Rows {
		if got := row.Values[0].GetI(); got != int64(i+1) {
			t.Fatalf("id[%d] = %d, want %d", i, got, i+1)
		}
	}
}

// TestExecuteTxAtomic 验证多语句事务作为单条日志原子执行。
func TestExecuteTxAtomic(t *testing.T) {
	client, _, _ := startSingleNode(t)
	ctx := context.Background()

	if _, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE t (a INTEGER, b INTEGER)",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Tx: true,
		Statements: []string{
			"INSERT INTO t (a, b) VALUES (1, 2)",
			"INSERT INTO t (a, b) VALUES (3, 4)",
		},
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("tx error: %s", resp.Error)
	}
	if resp.RowsAffected != 1 {
		t.Fatalf("rows affected = %d, want 1（多语句只统计最后一条）", resp.RowsAffected)
	}
	q, err := client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT COUNT(*) FROM t"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := q.Rows[0].Values[0].GetI(); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
}
