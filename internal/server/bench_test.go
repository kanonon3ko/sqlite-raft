// 性能基准：单节点与三节点集群的写吞吐/延迟。
// 运行：go test -bench=. -benchtime=3s ./internal/server/
package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"sync"
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

// benchNode 是基准用的最小节点。
type benchNode struct {
	node   *raft.Node
	gs     *grpc.Server
	client sqlraftpb.SqlRaftClient
	store  *store.Store
}

func newBenchNode(b *testing.B, id int32, peers map[int32]string, addr string) *benchNode {
	b.Helper()
	st, err := store.Open("")
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	node, err := raft.New(raft.Config{
		ID:                id,
		Peers:             peers,
		ElectionTimeout:   600 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		Logger:            log.New(io.Discard, "", 0),
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			return &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
			}, nil
		},
	})
	if err != nil {
		b.Fatalf("create node: %v", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(gs, node)
	sqlraftpb.RegisterSqlRaftServer(gs, New(node, st, nil))
	go gs.Serve(lis)
	node.Start()
	b.Cleanup(func() {
		node.Stop()
		gs.Stop()
		st.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() { conn.Close() })
	return &benchNode{node: node, gs: gs, client: sqlraftpb.NewSqlRaftClient(conn), store: st}
}

func waitBenchLeader(b *testing.B, nodes []*benchNode) int {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for i, nd := range nodes {
			if nd.node.IsLeader() {
				return i
			}
		}
		if time.Now().After(deadline) {
			b.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// BenchmarkSingleNodeWrite 单节点（无 WAL）写吞吐。
func BenchmarkSingleNodeWrite(b *testing.B) {
	nd := newBenchNode(b, 0, nil, "127.0.0.1:62000")
	waitBenchLeader(b, []*benchNode{nd})
	ctx := context.Background()
	if _, err := nd.client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)",
	}); err != nil {
		b.Fatalf("create: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := nd.client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv VALUES (?, ?)",
			Params: []*sqlraftpb.Value{
				{Kind: &sqlraftpb.Value_I{I: int64(i)}},
				{Kind: &sqlraftpb.Value_S{S: "value"}},
			},
		}); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

// BenchmarkThreeNodeWrite 三节点集群写吞吐与延迟分位数。
func BenchmarkThreeNodeWrite(b *testing.B) {
	const n = 3
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatalf("listen: %v", err)
		}
		addrs[i] = lis.Addr().String()
		lis.Close()
	}
	nodes := make([]*benchNode, n)
	for i := 0; i < n; i++ {
		peers := make(map[int32]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[int32(j)] = addrs[j]
			}
		}
		nodes[i] = newBenchNode(b, int32(i), peers, addrs[i])
	}
	leader := waitBenchLeader(b, nodes)
	ctx := context.Background()
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)",
	}); err != nil {
		b.Fatalf("create: %v", err)
	}
	// 预热
	for i := 0; i < 20; i++ {
		nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv VALUES (?, ?)",
			Params: []*sqlraftpb.Value{
				{Kind: &sqlraftpb.Value_I{I: int64(i)}},
				{Kind: &sqlraftpb.Value_S{S: "warm"}},
			},
		})
	}

	lats := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv VALUES (?, ?)",
			Params: []*sqlraftpb.Value{
				{Kind: &sqlraftpb.Value_I{I: int64(1000 + i)}},
				{Kind: &sqlraftpb.Value_S{S: "value"}},
			},
		}); err != nil {
			b.Fatalf("insert: %v", err)
		}
		lats = append(lats, time.Since(start))
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	percentile := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		idx := int(float64(len(lats)-1) * p)
		return lats[idx]
	}
	b.ReportMetric(float64(percentile(0.5).Microseconds()), "p50_us")
	b.ReportMetric(float64(percentile(0.99).Microseconds()), "p99_us")
	b.Logf("writes=%d p50=%v p99=%v", len(lats), percentile(0.5), percentile(0.99))
	_ = fmt.Sprintf
}

// BenchmarkThreeNodeConcurrentWrite 三节点 8 并发写：
// group commit 把并发 Propose 合并为批量 fsync 与批量复制。
func BenchmarkThreeNodeConcurrentWrite(b *testing.B) {
	const n = 3
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatalf("listen: %v", err)
		}
		addrs[i] = lis.Addr().String()
		lis.Close()
	}
	nodes := make([]*benchNode, n)
	for i := 0; i < n; i++ {
		peers := make(map[int32]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[int32(j)] = addrs[j]
			}
		}
		nodes[i] = newBenchNode(b, int32(i), peers, addrs[i])
	}
	leader := waitBenchLeader(b, nodes)
	ctx := context.Background()
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)",
	}); err != nil {
		b.Fatalf("create: %v", err)
	}

	const workers = 8
	// 预热：插入 workers 行，供并发 UPDATE 使用
	for i := 0; i < workers; i++ {
		if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv VALUES (?, ?)",
			Params: []*sqlraftpb.Value{
				{Kind: &sqlraftpb.Value_I{I: int64(i)}},
				{Kind: &sqlraftpb.Value_S{S: "init"}},
			},
		}); err != nil {
			b.Fatalf("warmup insert: %v", err)
		}
	}
	perWorker := b.N / workers
	if perWorker < 1 {
		perWorker = 1
	}
	var wg sync.WaitGroup
	lats := make([]time.Duration, 0, b.N)
	var latMu sync.Mutex
	b.ResetTimer()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				start := time.Now()
				if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
					Sql: "UPDATE kv SET v = ? WHERE k = ?",
					Params: []*sqlraftpb.Value{
						{Kind: &sqlraftpb.Value_S{S: "value"}},
						{Kind: &sqlraftpb.Value_I{I: int64(w)}},
					},
				}); err != nil {
					b.Errorf("insert: %v", err)
					return
				}
				latMu.Lock()
				lats = append(lats, time.Since(start))
				latMu.Unlock()
			}
		}(w * 1000000)
	}
	wg.Wait()
	b.StopTimer()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	percentile := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats)-1)*p)]
	}
	b.ReportMetric(float64(percentile(0.5).Microseconds()), "p50_us")
	b.ReportMetric(float64(percentile(0.99).Microseconds()), "p99_us")
}
