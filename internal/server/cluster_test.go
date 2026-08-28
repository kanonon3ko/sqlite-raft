package server

import (
	"context"
	"net"
	"strconv"
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

type testNode struct {
	node     *raft.Node
	server   *grpc.Server
	listener net.Listener
	store    *store.Store
	client   sqlraftpb.SqlRaftClient
	admin    sqlraftpb.AdminClient
	conn     *grpc.ClientConn
}

func startTestNode(t *testing.T, id int32, peers map[int32]string, addr string) *testNode {
	return startTestNodeOpts(t, id, peers, addr, 0)
}

// startTestNodeOpts 启动一个测试节点；compactAfter 控制日志压缩阈值。
func startTestNodeOpts(t *testing.T, id int32, peers map[int32]string, addr string, compactAfter uint64) *testNode {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	node, err := raft.New(raft.Config{
		ID:                id,
		Peers:             peers,
		ElectionTimeout:   600 * time.Millisecond, // 内部还会叠加随机抖动
		HeartbeatInterval: 100 * time.Millisecond,
		CompactAfter:      compactAfter,
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
		SnapshotData: func(ctx context.Context) ([]byte, error) {
			return st.Snapshot()
		},
		ApplySnapshot: func(ctx context.Context, data []byte) error {
			return st.ApplySnapshot(data)
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(256<<20),
		grpc.MaxSendMsgSize(256<<20),
	)
	raftpb.RegisterRaftServiceServer(gs, node)
	sqlraftpb.RegisterSqlRaftServer(gs, New(node, st, nil))
	sqlraftpb.RegisterAdminServer(gs, NewAdmin(node))
	go gs.Serve(lis)
	node.Start()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &testNode{
		node:     node,
		server:   gs,
		listener: lis,
		store:    st,
		client:   sqlraftpb.NewSqlRaftClient(conn),
		admin:    sqlraftpb.NewAdminClient(conn),
		conn:     conn,
	}
}

func (tn *testNode) close() {
	tn.node.Stop()
	tn.server.Stop()
	tn.conn.Close()
	tn.store.Close()
}

// TestThreeNodeCluster 启动 3 节点集群，验证：
//  1. 能选出唯一 Leader；
//  2. 写操作在 Leader 提交；
//  3. 日志复制到全部 follower，各节点本地状态机结果一致。
func TestThreeNodeCluster(t *testing.T) {
	const n = 3

	// 先占好端口，再互相连接
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
		listeners[i].Close()
	}

	nodes := make([]*testNode, n)
	for i := 0; i < n; i++ {
		peers := make(map[int32]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[int32(j)] = addrs[j]
			}
		}
		nodes[i] = startTestNode(t, int32(i), peers, addrs[i])
	}
	defer func() {
		for _, tn := range nodes {
			if tn != nil {
				tn.close()
			}
		}
	}()

	// 等待选出 Leader（随机化选举超时下一般 1-3 轮内收敛）
	leader := waitForLeader(t, nodes, 15*time.Second)
	t.Logf("leader is node %d", leader)

	ctx := context.Background()
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	resp, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
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

	// 等待所有节点复制并应用（每个节点都能本地读到同一份数据）
	deadline := time.Now().Add(10 * time.Second)
	for {
		allReady := true
		for i := 0; i < n; i++ {
			q, err := nodes[i].client.Query(ctx, &sqlraftpb.QueryRequest{
				Sql: "SELECT v FROM kv WHERE k = ?",
				Params: []*sqlraftpb.Value{
					{Kind: &sqlraftpb.Value_S{S: "answer"}},
				},
			})
			if err != nil || q.Error != "" || len(q.Rows) != 1 || q.Rows[0].Values[0].GetI() != 42 {
				allReady = false
			}
		}
		if allReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replication to all nodes not completed within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Leader 的强一致读也能得到相同结果
	q, err := nodes[leader].client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT v FROM kv", Strong: true})
	if err != nil {
		t.Fatalf("strong query: %v", err)
	}
	if q.Error != "" || len(q.Rows) != 1 || q.Rows[0].Values[0].GetI() != 42 {
		t.Fatalf("unexpected strong query result: %+v", q)
	}
}

func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for i, tn := range nodes {
			if tn.node.IsLeader() {
				// 给一点稳定时间，确认没有立刻反复
				time.Sleep(200 * time.Millisecond)
				if tn.node.IsLeader() {
					return i
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader elected within %v", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMembershipAndSnapshot 验证：
//  1. Leader 独立压缩日志并生成快照；
//  2. 新节点加入时通过 InstallSnapshot 追赶全部数据；
//  3. 移除节点后多数派配置更新，集群仍可写入。
func TestMembershipAndSnapshot(t *testing.T) {
	const n = 3

	listeners := make([]net.Listener, n+1)
	addrs := make([]string, n+1)
	for i := 0; i <= n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
		listeners[i].Close()
	}

	nodes := make([]*testNode, n+1)
	for i := 0; i < n; i++ {
		peers := make(map[int32]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[int32(j)] = addrs[j]
			}
		}
		// 小压缩阈值：尽早触发快照，覆盖“新节点落后于压缩点”的路径
		nodes[i] = startTestNodeOpts(t, int32(i), peers, addrs[i], 2)
	}
	defer func() {
		for _, tn := range nodes {
			if tn != nil {
				tn.close()
			}
		}
	}()

	leader := waitForLeader(t, nodes[:n], 15*time.Second)
	t.Logf("leader is node %d", leader)
	ctx := context.Background()

	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
			Sql: "INSERT INTO kv VALUES (?, ?)",
			Params: []*sqlraftpb.Value{
				{Kind: &sqlraftpb.Value_I{I: int64(i)}},
				{Kind: &sqlraftpb.Value_S{S: "v" + strconv.Itoa(i)}},
			},
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// 等待 Leader 完成日志压缩（9 条日志，阈值 2 → 快照推进到 >= 8）
	deadline := time.Now().Add(15 * time.Second)
	for nodes[leader].node.SnapshotIndex() < 8 {
		if time.Now().After(deadline) {
			t.Fatalf("leader snapshot index = %d, want >= 8", nodes[leader].node.SnapshotIndex())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("leader snapshot index = %d", nodes[leader].node.SnapshotIndex())

	// 新节点加入：初始只知道 Leader 地址
	nodes[n] = startTestNodeOpts(t, int32(n), map[int32]string{int32(leader): addrs[leader]}, addrs[n], 2)
	addResp, err := nodes[leader].admin.AddPeer(ctx, &sqlraftpb.AddPeerRequest{NodeId: int32(n), Addr: addrs[n]})
	if err != nil || !addResp.Ok {
		t.Fatalf("add peer: err=%v resp=%+v", err, addResp)
	}

	// 新节点通过 InstallSnapshot 追赶历史数据
	deadline = time.Now().Add(20 * time.Second)
	for {
		q, err := nodes[n].client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT count(*) FROM kv"})
		if err == nil && q.Error == "" && len(q.Rows) == 1 && q.Rows[0].Values[0].GetI() == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new node did not catch up via snapshot: last result err=%v rows=%v", err, q)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Log("new node caught up via InstallSnapshot")

	// 新节点加入后继续复制新写入
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "INSERT INTO kv VALUES (?, ?)",
		Params: []*sqlraftpb.Value{
			{Kind: &sqlraftpb.Value_I{I: 100}},
			{Kind: &sqlraftpb.Value_S{S: "after-join"}},
		},
	}); err != nil {
		t.Fatalf("insert after join: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		q, err := nodes[n].client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT count(*) FROM kv"})
		if err == nil && q.Error == "" && len(q.Rows) == 1 && q.Rows[0].Values[0].GetI() == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new node did not replicate new write: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 移除新节点：多数派配置更新，旧集群恢复 3 节点
	rmResp, err := nodes[leader].admin.RemovePeer(ctx, &sqlraftpb.RemovePeerRequest{NodeId: int32(n)})
	if err != nil || !rmResp.Ok {
		t.Fatalf("remove peer: err=%v resp=%+v", err, rmResp)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		st, err := nodes[leader].admin.ClusterStatus(ctx, &sqlraftpb.ClusterStatusRequest{})
		if err == nil && len(st.Peers) == 2 && st.Peers[int32(n)] == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader peers not updated after remove: %+v err=%v", st, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 集群仍可写（3 节点多数派）
	if _, err := nodes[leader].client.Execute(ctx, &sqlraftpb.ExecuteRequest{
		Sql: "INSERT INTO kv VALUES (?, ?)",
		Params: []*sqlraftpb.Value{
			{Kind: &sqlraftpb.Value_I{I: 101}},
			{Kind: &sqlraftpb.Value_S{S: "after-remove"}},
		},
	}); err != nil {
		t.Fatalf("insert after remove: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		allOK := true
		for i := 0; i < n; i++ {
			q, err := nodes[i].client.Query(ctx, &sqlraftpb.QueryRequest{Sql: "SELECT count(*) FROM kv"})
			if err != nil || q.Error != "" || len(q.Rows) != 1 || q.Rows[0].Values[0].GetI() != 10 {
				allOK = false
			}
		}
		if allOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all nodes reached 10 rows after membership change")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
