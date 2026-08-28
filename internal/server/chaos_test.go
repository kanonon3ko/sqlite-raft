// 混沌测试：在故障注入（节点崩溃/重启）下运行读写负载，
// 验证 Raft 不变量：Leader 唯一、状态机最终一致、操作历史线性化。
package server

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/raftpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/lincheck"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/raftwal"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

// chaosNode 是一个可崩溃/重启的节点（数据保留在磁盘，模拟进程重启）。
type chaosNode struct {
	id      int32
	addr    string
	dataDir string
	peers   map[int32]string
	nc      *netChaos

	mu      sync.Mutex
	st      *store.Store
	wal     *raftwal.Wal
	node    *raft.Node
	gs      *grpc.Server
	lis     net.Listener
	client  sqlraftpb.SqlRaftClient
	conn    *grpc.ClientConn
	running bool

	// 诊断：应用历史（index → 值），供不一致时对比
	appliedHistory map[uint64]string
	histMu         sync.Mutex
}

func newChaosNode(id int32, addr, dataDir string) *chaosNode {
	return &chaosNode{
		id:             id,
		addr:           addr,
		dataDir:        dataDir,
		appliedHistory: make(map[uint64]string),
	}
}

// start 启动（或重启）节点。
func (c *chaosNode) start(t *testing.T) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return nil
	}
	st, err := store.Open(filepath.Join(c.dataDir, "state.db"))
	if err != nil {
		return err
	}
	wal, err := raftwal.Open(filepath.Join(c.dataDir, "raft"))
	if err != nil {
		st.Close()
		return err
	}
	applied, err := st.AppliedIndex()
	if err != nil {
		st.Close()
		wal.Close()
		return err
	}
	node, err := raft.New(raft.Config{
		ID:                c.id,
		Peers:             c.peers, // 首次启动的初始配置；重启时以 WAL 持久化配置为准
		ElectionTimeout:   700 * time.Millisecond,
		HeartbeatInterval: 150 * time.Millisecond,
		Wal:               wal,
		AppliedIndex:      applied,
		CompactAfter:      64,
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			if entry.GetCommand().GetConfChange() != nil {
				return &raft.ApplyResult{}, nil
			}
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			if exec := entry.GetCommand().GetExec(); exec != nil {
				c.histMu.Lock()
				c.appliedHistory[index] = fmt.Sprintf("%v", exec.Params)
				c.histMu.Unlock()
			}
			return &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
				Columns:      outcomeColumnNames(outcome),
				RowTypes:     outcomeColumnTypes(outcome),
				Rows:         outcome.Rows,
			}, nil
		},
		SnapshotData: func(ctx context.Context) ([]byte, error) {
			return st.Snapshot()
		},
		ApplySnapshot: func(ctx context.Context, data []byte) error {
			return st.ApplySnapshot(data)
		},
		DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(c.nc.interceptor)},
	})
	if err != nil {
		st.Close()
		wal.Close()
		return err
	}
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		st.Close()
		wal.Close()
		return err
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(256<<20), grpc.MaxSendMsgSize(256<<20))
	raftpb.RegisterRaftServiceServer(gs, node)
	sqlraftpb.RegisterSqlRaftServer(gs, New(node, st, nil))
	sqlraftpb.RegisterAdminServer(gs, NewAdmin(node))
	go gs.Serve(lis)
	node.Start()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		node.Stop()
		gs.Stop()
		st.Close()
		wal.Close()
		return err
	}
	c.st, c.wal, c.node, c.gs, c.lis, c.conn, c.client = st, wal, node, gs, lis, conn, sqlraftpb.NewSqlRaftClient(conn)
	c.running = true
	return nil
}

// netChaos 模拟节点间的网络故障：按目标节点丢包或延迟。
// 拦截器加在每个节点的出站 gRPC 客户端上，共享同一个控制器。
type netChaos struct {
	mu    sync.Mutex
	rng   *rand.Rand
	drop  map[int32]float64
	delay map[int32]time.Duration
	drops int
}

func newNetChaos(seed int64) *netChaos {
	return &netChaos{
		rng:   rand.New(rand.NewSource(seed)),
		drop:  make(map[int32]float64),
		delay: make(map[int32]time.Duration),
	}
}

func (nc *netChaos) setDrop(node int32, p float64) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if p <= 0 {
		delete(nc.drop, node)
	} else {
		nc.drop[node] = p
	}
}

func (nc *netChaos) setDelay(node int32, d time.Duration) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if d <= 0 {
		delete(nc.delay, node)
	} else {
		nc.delay[node] = d
	}
}

func (nc *netChaos) dropped() int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.drops
}

// interceptor 是 gRPC 一元拦截器：按请求的目标节点丢包/延迟。
func (nc *netChaos) interceptor(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	to := int32(-1)
	if r, ok := req.(interface{ GetTo() int32 }); ok {
		to = r.GetTo()
	}
	nc.mu.Lock()
	drop := nc.drop[to]
	delay := nc.delay[to]
	nc.mu.Unlock()
	if drop > 0 && nc.rng.Float64() < drop {
		nc.mu.Lock()
		nc.drops++
		nc.mu.Unlock()
		return errors.New("simulated network drop")
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// stop 模拟节点崩溃（保留磁盘数据）。
func (c *chaosNode) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return
	}
	c.node.Stop()
	c.gs.Stop()
	c.conn.Close()
	c.st.Close()
	c.wal.Close()
	c.running = false
}

func (c *chaosNode) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *chaosNode) isLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running && c.node.IsLeader()
}

func (c *chaosNode) state() (commit, applied uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return 0, 0
	}
	return c.node.CommitIndex(), c.node.AppliedIndex()
}

func (c *chaosNode) snapshotIndex() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return 0
	}
	return c.node.SnapshotIndex()
}

func (c *chaosNode) historySnapshot() map[uint64]string {
	c.histMu.Lock()
	defer c.histMu.Unlock()
	out := make(map[uint64]string, len(c.appliedHistory))
	for k, v := range c.appliedHistory {
		out[k] = v
	}
	return out
}

func (c *chaosNode) logTerms() []struct {
	Index uint64
	Term  uint64
} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return nil
	}
	return c.node.DebugLogTerms()
}

func (c *chaosNode) localValue(ctx context.Context) string {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return ""
	}
	resp, err := client.Query(ctx, &sqlraftpb.QueryRequest{
		Sql: "SELECT v FROM kv WHERE k = 'counter'",
	})
	if err != nil || resp.Error != "" || len(resp.Rows) == 0 {
		return ""
	}
	return resp.Rows[0].Values[0].GetS()
}

// findLeader 返回当前 Leader 节点（无 Leader 返回 -1）。
func findLeader(nodes []*chaosNode) int {
	for i := range nodes {
		if nodes[i].isLeader() {
			return i
		}
	}
	return -1
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// TestChaosLinearizability 在故障注入下运行负载并验证不变量。
func TestChaosLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}
	seed := time.Now().UnixNano()
	t.Logf("chaos seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	const n = 3
	nc := newNetChaos(seed + 1)
	nodes := make([]*chaosNode, n)
	for i := 0; i < n; i++ {
		nodes[i] = newChaosNode(int32(i), fmt.Sprintf("127.0.0.1:%d", 61000+i),
			t.TempDir())
		nodes[i].nc = nc
		peers := make(map[int32]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[int32(j)] = fmt.Sprintf("127.0.0.1:%d", 61000+j)
			}
		}
		nodes[i].peers = peers
		if err := nodes[i].start(t); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	defer func() {
		for _, nd := range nodes {
			nd.stop()
		}
	}()

	// 等待初始 Leader
	deadline := time.Now().Add(15 * time.Second)
	for findLeader(nodes) < 0 {
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(50 * time.Millisecond)
	}

	ctx := context.Background()
	leader := findLeader(nodes)
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var (
		mu  sync.Mutex
		ops []lincheck.Op
	)
	record := func(op lincheck.Op) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// 两个写客户端：值 = clientID*100000 + 本地序号（全局唯一）
	for c := 0; c < 2; c++ {
		clientID := c + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				seq++
				val := fmt.Sprintf("%d", clientID*100000+seq)
				start := time.Now()
				ok := false
				for attempt := 0; attempt < 5 && !ok; attempt++ {
					li := findLeader(nodes)
					if li < 0 {
						time.Sleep(30 * time.Millisecond)
						continue
					}
					resp, err := nodes[li].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
						Sql:    "INSERT INTO kv (k, v) VALUES ('counter', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
						Params: []*sqlraftpb.Value{{Kind: &sqlraftpb.Value_S{S: val}}},
					})
					if err == nil && resp.Error == "" {
						ok = true
					}
					if err == nil && resp.Error == "NOT_LEADER" {
						continue
					}
				}
				if ok {
					record(lincheck.Op{
						Client: clientID, Kind: lincheck.Write, Key: "counter",
						Value: val, Start: start, End: time.Now(),
					})
				}
				time.Sleep(250 * time.Millisecond)
			}
		}()
	}

	// 一个强一致读客户端
	readerID := 99
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			start := time.Now()
			got := ""
			for attempt := 0; attempt < 5; attempt++ {
				li := findLeader(nodes)
				if li < 0 {
					time.Sleep(30 * time.Millisecond)
					continue
				}
				resp, err := nodes[li].client.Query(ctx, &sqlraftpb.QueryRequest{
					Sql: "SELECT v FROM kv WHERE k = 'counter'", Strong: true,
				})
				if err == nil && resp.Error == "" && len(resp.Rows) > 0 {
					got = resp.Rows[0].Values[0].GetS()
					break
				}
			}
			// 只记录有值的读；空读（表刚建好尚无数据）无法参与线性化判定
			if got != "" {
				record(lincheck.Op{
					Client: readerID, Kind: lincheck.Read, Key: "counter",
					Value: got, Start: start, End: time.Now(),
				})
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	// Leader 唯一性监控
	stopMon := make(chan struct{})
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		for {
			select {
			case <-stopMon:
				return
			default:
			}
			leaders := 0
			for _, nd := range nodes {
				if nd.isLeader() {
					leaders++
				}
			}
			if leaders > 1 {
				t.Errorf("raft invariant violated: %d leaders simultaneously", leaders)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 故障注入：随机崩溃/重启节点
	failDone := make(chan struct{})
	go func() {
		defer close(failDone)
		for i := 0; i < chaosFaultRounds; i++ {
			switch rng.Intn(3) {
			case 0, 1:
				// 节点崩溃/重启
				time.Sleep(time.Duration(400+rng.Intn(400)) * time.Millisecond)
				target := rng.Intn(n)
				nodes[target].stop()
				t.Logf("crashed node %d", target)
				// 停摆 0.8~1.5 秒：节点错过部分日志，强制走回退/快照路径
				time.Sleep(time.Duration(800+rng.Intn(700)) * time.Millisecond)
				if err := nodes[target].start(t); err != nil {
					t.Errorf("restart node %d: %v", target, err)
				}
				t.Logf("restarted node %d", target)
			case 2:
				// 网络故障：到目标节点的链路丢包 + 延迟
				target := rng.Intn(n)
				nc.setDrop(int32(target), 0.6+rng.Float64()*0.4)
				nc.setDelay(int32(target), time.Duration(20+rng.Intn(40))*time.Millisecond)
				t.Logf("network fault to node %d (drop+delay)", target)
				time.Sleep(time.Duration(1200+rng.Intn(800)) * time.Millisecond)
				nc.setDrop(int32(target), 0)
				nc.setDelay(int32(target), 0)
				t.Logf("network restored to node %d", target)
			}
		}
	}()

	// 运行负载
	time.Sleep(time.Duration(chaosLoadSeconds) * time.Second)
	close(stopCh)
	wg.Wait()
	close(stopMon)
	<-monDone
	<-failDone
	// 等最后一次重启的节点稳定（选举 + 追平日志）
	time.Sleep(3 * time.Second)

	// 等待集群稳定：所有存活节点就绪且本地值一致
	deadline = time.Now().Add(20 * time.Second)
	var finalValues []string
	for {
		for _, nd := range nodes {
			if !nd.isRunning() {
				if err := nd.start(t); err != nil {
					t.Fatalf("final restart: %v", err)
				}
			}
		}
		vals := make([]string, n)
		allReady := true
		for i, nd := range nodes {
			vals[i] = nd.localValue(ctx)
			if vals[i] == "" {
				allReady = false
			}
		}
		if allReady && vals[0] == vals[1] && vals[1] == vals[2] {
			finalValues = vals
			break
		}
		// 早期检测：应用历史出现分歧立即报告
		hist := make([]map[uint64]string, n)
		for i := range nodes {
			hist[i] = nodes[i].historySnapshot()
		}
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				for idx := uint64(1); idx <= minUint64(uint64(len(hist[i])), uint64(len(hist[j]))); idx++ {
					if hist[i][idx] != hist[j][idx] {
						t.Errorf("history divergence: node %d idx %d = %q, node %d idx %d = %q",
							i, idx, hist[i][idx], j, idx, hist[j][idx])
						for k, nd := range nodes {
							terms := nd.logTerms()
							start := 0
							if len(terms) > 6 {
								start = len(terms) - 6
							}
							t.Logf("node %d log tail: %+v", k, terms[start:])
						}
						t.Fatalf("history diverged")
					}
				}
			}
		}
		if time.Now().After(deadline) {
			for i, nd := range nodes {
				c, a := nd.state()
				snap := nd.snapshotIndex()
				t.Logf("node %d: value=%q commit=%d applied=%d snap=%d running=%v",
					i, vals[i], c, a, snap, nd.isRunning())
			}
			// 找出应用历史的分歧点
			hist := make([]map[uint64]string, n)
			for i := range nodes {
				hist[i] = nodes[i].historySnapshot()
			}
			for i := 0; i < n; i++ {
				for j := i + 1; j < n; j++ {
					for idx := uint64(1); idx <= uint64(len(hist[i])); idx++ {
						if hist[i][idx] != hist[j][idx] {
							t.Logf("divergence: node %d idx %d = %q, node %d idx %d = %q",
								i, idx, hist[i][idx], j, idx, hist[j][idx])
							j = n
							break
						}
					}
				}
			}
			// 打印各节点日志尾部的 (index, term)，定位日志分歧
			for i, nd := range nodes {
				terms := nd.logTerms()
				start := 0
				if len(terms) > 8 {
					start = len(terms) - 8
				}
				var tail []struct {
					Index uint64
					Term  uint64
				}
				tail = terms[start:]
				t.Logf("node %d log tail: %+v", i, tail)
			}
			t.Fatalf("nodes not converged: %v", vals)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("final counter value: %s", finalValues[0])
	t.Logf("simulated network drops: %d", nc.dropped())

	mu.Lock()
	history := make([]lincheck.Op, len(ops))
	copy(history, ops)
	mu.Unlock()
	t.Logf("history size: %d operations", len(history))
	if err := lincheck.Check(history); err != nil {
		t.Fatalf("linearizability check failed: %v", err)
	}
	t.Log("linearizability OK")
}
