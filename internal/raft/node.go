// Package raft 实现 SQLite over Raft 的共识核心。
//
// 设计要点：
//   - 状态由 sync.RWMutex 保护，网络 RPC 在锁外执行；
//   - 提交后的日志由独立 applyLoop goroutine 按序应用到本地状态机，
//     避免磁盘 I/O 阻塞共识处理；
//   - 每个 follower 一个常驻复制 goroutine，Leader 在任期间复用；
//   - 配置了 WAL 时，日志追加、状态变更在应答前同步落盘；
//   - Leader 采用保守压缩策略：只有全部节点都已复制的已应用条目才会被丢弃。
package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/raftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raftwal"
)

const (
	// rpcTimeout 是节点间 RPC 的调用超时，避免消息丢失时 goroutine 永久阻塞。
	rpcTimeout = time.Second
	// maxBatch 是单次 AppendEntries 携带的最大日志条目数，防止日志差距过大时消息体膨胀。
	maxBatch = 256
	// maxSnapshotBytes 是单次 InstallSnapshot 允许的最大消息体（gRPC 限制）。
	maxSnapshotBytes = 256 << 20
)

// Role 是节点的 Raft 角色。
type Role int32

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// ApplyResult 是 Apply 回调的返回结果，用于向客户端汇报写操作的效果。
type ApplyResult struct {
	RowsAffected int64
	LastInsertID int64
	Columns      []string // RETURNING 语句返回的列名（普通语句为空）
	RowTypes     []string // 对应列的 SQLite 声明类型
	Rows         [][]any  // RETURNING 语句返回的行（普通语句为空）
}

// ApplyFunc 在日志条目被提交后调用，把一条确定性命令应用到本地状态机。
// index 是日志索引，供状态机记录已应用位置。所有节点按相同顺序调用，
// 因此返回结果必须是确定性的。
type ApplyFunc func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*ApplyResult, error)

// Config 是 Raft 节点的配置。
type Config struct {
	ID                int32
	Peers             map[int32]string // peerID -> "host:port"
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	Apply             ApplyFunc
	Logger            *log.Logger

	// Wal 是持久化句柄；nil 表示纯内存运行（不跨重启）。
	Wal *raftwal.Wal
	// AppliedIndex 是状态机已应用的日志索引（崩溃恢复时传入，
	// 由 SQLite 元数据表读出）。
	AppliedIndex uint64
	// CompactAfter 是两次压缩之间的最小日志条数；0 表示禁用压缩。
	CompactAfter uint64

	// SnapshotData 生成当前状态机快照字节（VACUUM INTO 的产物）。
	// 在 applyMu 下调用，保证与日志应用互斥；nil 时禁用快照。
	SnapshotData func(ctx context.Context) ([]byte, error)
	// ApplySnapshot 用快照字节替换本地状态机；nil 时拒绝接收快照。
	ApplySnapshot func(ctx context.Context, data []byte) error

	// DialOptions 是节点间 gRPC 连接的额外拨号选项
	// （测试可注入网络故障拦截器）。
	DialOptions []grpc.DialOption
}

// ErrNotLeader 表示本节点不是 Leader；Leader 字段给出当前已知的 Leader 节点 ID。
type ErrNotLeader struct {
	Leader int32
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader (leader=%d)", e.Leader)
}

// proposeOutcome 是 Propose 等待的最终结果（应用结果或错误）。
type proposeOutcome struct {
	res *ApplyResult
	err error
}

// pendingPropose 记录一个等待日志提交并应用后才回复的 Propose。
type pendingPropose struct {
	index   uint64
	outcome chan *proposeOutcome
}

// pendingReadIndex 记录一次 ReadIndex 线性一致读请求：
// 需要多数派 follower 在相同任期回复 AppendEntries 后才算确认。
type pendingReadIndex struct {
	term  uint64
	index uint64
	acks  map[int32]bool // 已确认的 follower
	done  chan error
}

// peerConn 是到一个对等节点的 gRPC 连接与客户端。
type peerConn struct {
	client raftpb.RaftServiceClient
	conn   *grpc.ClientConn
}

// Node 是一个 Raft 共识节点，同时实现 raftpb.RaftServiceServer 供节点间通信。
type Node struct {
	raftpb.UnimplementedRaftServiceServer

	cfg Config

	mu            sync.RWMutex
	log           []*logpb.LogEntry // 内存日志，第一项对应 snapshotIndex+1
	currentTerm   uint64
	votedFor      int32
	commitIndex   uint64
	serverState   Role
	currentLeader int32
	votes         int
	nextIndex     map[int32]uint64
	matchIndex    map[int32]uint64
	majoritySize  int
	pending       []*pendingPropose
	readIndexes   []*pendingReadIndex

	// 快照位置：snapshotIndex 及之前的日志已压缩（由状态机持有其状态）。
	snapshotIndex uint64
	snapshotTerm  uint64

	// 持久化句柄与最近落盘状态（用于跳过无变化的 fsync）
	wal            *raftwal.Wal
	persistedTerm  uint64
	persistedVoted int32
	persistedSnap  uint64

	// 领导者上每个 follower 的常驻发送 goroutine 控制
	followerTicks map[int32]chan struct{}
	followerStops map[int32]chan struct{}

	// 动态集群配置：peers 为当前配置中的其他节点（随 ConfChange 日志变化）
	peers        map[int32]string
	snapshotData []byte // 最近一次压缩生成的快照数据（与 snapshotIndex 对应）
	// removing 是已提出但尚未提交的 REMOVE 目标：
	// 其 ConfChange 条目及之前的日志只需“其余节点”的多数派即可提交，
	// 避免故障节点导致集群永久卡死（Raft 单节点变更方法）。
	removing map[int32]bool

	// 应用队列：applyLoop 独占，保证日志严格按序应用。
	// appliedIndex 由 applyMu 保护；commitIndex 由 mu 保护。
	applyMu      sync.Mutex
	appliedIndex uint64
	applyTarget  uint64
	applyCond    *sync.Cond

	// 定时器控制：tickerLoop 独占 nextWake 的计时
	nextWake time.Time
	wakeCh   chan struct{}

	// group commit：Propose 追加的日志先进入待持久化队列，
	// 由 persistLoop 合并为一批做一次 fsync 并统一推送复制。
	pendingPersist []*logpb.LogEntry
	persistCh      chan struct{}

	clients map[int32]*peerConn

	ctx    context.Context
	cancel context.CancelFunc
}

// New 创建并启动前初始化一个 Raft 节点（调用方随后应调用 Start 并注册到 gRPC server）。
func New(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 1500 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}

	// 集群配置：优先使用 WAL 中持久化的动态配置，否则用初始配置
	peers := make(map[int32]string, len(cfg.Peers))
	if cfg.Wal != nil {
		saved, err := cfg.Wal.LoadPeers()
		if err != nil {
			return nil, fmt.Errorf("load peers: %w", err)
		}
		if len(saved) > 0 {
			peers = saved
		}
	}
	for id, addr := range cfg.Peers {
		if _, ok := peers[id]; !ok {
			peers[id] = addr
		}
	}

	n := &Node{
		cfg:            cfg,
		log:            make([]*logpb.LogEntry, 0),
		votedFor:       -1,
		persistedVoted: -1,
		currentLeader:  -1,
		peers:          peers,
		nextIndex:      make(map[int32]uint64),
		matchIndex:     make(map[int32]uint64),
		majoritySize:   (len(peers)+1)/2 + 1,
		clients:        make(map[int32]*peerConn),
		removing:       make(map[int32]bool),
		persistCh:      make(chan struct{}, 1),
		wakeCh:         make(chan struct{}, 1),
		nextWake:       time.Now().Add(cfg.ElectionTimeout),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.applyCond = sync.NewCond(&n.applyMu)

	// 从 WAL 恢复任期/投票/日志
	if cfg.Wal != nil {
		st := cfg.Wal.State()
		entries, err := cfg.Wal.LoadLog()
		if err != nil {
			return nil, fmt.Errorf("load raft log: %w", err)
		}
		n.wal = cfg.Wal
		n.currentTerm = st.CurrentTerm
		n.votedFor = st.VotedFor
		n.persistedTerm = st.CurrentTerm
		n.persistedVoted = st.VotedFor
		n.persistedSnap = st.SnapshotIndex
		n.snapshotIndex = st.SnapshotIndex
		n.snapshotTerm = st.SnapshotTerm
		n.log = entries
	}

	// 恢复应用起点：状态机已应用的索引（SQLite 元数据），之后只应用新提交的条目
	n.appliedIndex = cfg.AppliedIndex
	if n.appliedIndex > n.lastLogIndex() {
		return nil, fmt.Errorf("applied index %d exceeds log length %d (data/WAL mismatch?)",
			n.appliedIndex, n.lastLogIndex())
	}
	n.commitIndex = n.appliedIndex

	for id, addr := range n.peers {
		if err := n.dialPeer(id, addr); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// dialPeer 建立到对等节点的 gRPC 连接（配置变更时也会调用）。
func (n *Node) dialPeer(id int32, addr string) error {
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxSnapshotBytes),
			grpc.MaxCallSendMsgSize(maxSnapshotBytes),
		),
	}, n.cfg.DialOptions...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("dial peer %d (%s): %w", id, addr, err)
	}
	n.clients[id] = &peerConn{client: raftpb.NewRaftServiceClient(conn), conn: conn}
	return nil
}

// Start 启动后台 goroutine（定时器循环 + 日志应用循环）。
func (n *Node) Start() {
	go n.tickerLoop()
	go n.applyLoop()
	go n.persistLoop()
}

// Stop 停止后台 goroutine。
func (n *Node) Stop() {
	n.cancel()
}

// ---------- 对外只读接口 ----------

// IsLeader 返回本节点当前是否为 Leader。
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.serverState == RoleLeader
}

// NodeID 返回本节点 ID。
func (n *Node) NodeID() int32 {
	return n.cfg.ID
}

// Peers 返回当前配置中的其他节点（id → addr）。
func (n *Node) Peers() map[int32]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make(map[int32]string, len(n.peers))
	for id, addr := range n.peers {
		out[id] = addr
	}
	return out
}

// LeaderID 返回当前已知的 Leader 节点 ID（-1 表示未知）。
func (n *Node) LeaderID() int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentLeader
}

// CommitIndex 返回当前 commitIndex（已提交的最高日志索引）。
func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

// AppliedIndex 返回已应用的最高日志索引。
func (n *Node) AppliedIndex() uint64 {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	return n.appliedIndex
}

// SnapshotIndex 返回快照位置（已压缩的最高日志索引），用于观测与测试。
func (n *Node) SnapshotIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.snapshotIndex
}

// DebugLogTerms 返回内存日志的 (index, term) 序列（测试诊断用）。
func (n *Node) DebugLogTerms() []struct {
	Index uint64
	Term  uint64
} {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]struct {
		Index uint64
		Term  uint64
	}, 0, len(n.log))
	for i, e := range n.log {
		out = append(out, struct {
			Index uint64
			Term  uint64
		}{Index: n.snapshotIndex + uint64(i) + 1, Term: e.Term})
	}
	return out
}

// ReadIndex 实现 Raft 论文的 ReadIndex 线性一致读：
//  1. Leader 记录 readIndex = commitIndex 与当前任期；
//  2. 向所有 follower 广播一轮心跳，等待多数派在相同任期确认；
//  3. 调用方随后需等待状态机应用至 readIndex 再执行本地读。
//
// 本节点不是 Leader 时返回 ErrNotLeader。
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.serverState != RoleLeader {
		leader := n.currentLeader
		n.mu.Unlock()
		return 0, &ErrNotLeader{Leader: leader}
	}
	if len(n.clients) == 0 {
		// 单节点集群：自己即多数派，无需确认心跳
		ri := n.commitIndex
		n.mu.Unlock()
		return ri, nil
	}
	ri := &pendingReadIndex{
		term:  n.currentTerm,
		index: n.commitIndex,
		acks:  make(map[int32]bool),
		done:  make(chan error, 1),
	}
	n.readIndexes = append(n.readIndexes, ri)
	n.broadcastHeartbeatLocked() // 触发一轮确认心跳
	n.mu.Unlock()

	select {
	case err := <-ri.done:
		if err != nil {
			return 0, err
		}
		return ri.index, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// ackReadIndexLocked 在收到 follower 的同任期 AppendEntries 回复时，
// 累加 ReadIndex 确认数；达到多数派后完成对应请求。
func (n *Node) ackReadIndexLocked(peerID int32, term uint64) {
	if n.serverState != RoleLeader || term != n.currentTerm {
		return
	}
	done := 0
	for _, ri := range n.readIndexes {
		if ri.term != term {
			continue
		}
		if !ri.acks[peerID] {
			ri.acks[peerID] = true
		}
		if len(ri.acks)+1 >= n.majoritySize { // +1 是 Leader 自己
			ri.done <- nil
			done++
		}
	}
	if done > 0 {
		keep := n.readIndexes[:0]
		for _, ri := range n.readIndexes {
			select {
			case <-ri.done:
				continue // 已完成，移除
			default:
				keep = append(keep, ri)
			}
		}
		n.readIndexes = keep
	}
}

// Propose 向 Leader 提交一条确定性命令：追加日志并等待提交应用后返回结果。
// 本节点不是 Leader 时返回 ErrNotLeader。
func (n *Node) Propose(ctx context.Context, cmd *logpb.DeterministicCommand) (*ApplyResult, error) {
	// 先等待历史已提交条目全部应用，保证读取状态机时处于一致状态
	// （也是 AUTOINCREMENT 预分配、线性化读的前提）。
	if err := n.WaitCaughtUp(ctx); err != nil {
		return nil, err
	}

	n.mu.Lock()
	if n.serverState != RoleLeader {
		leader := n.currentLeader
		n.mu.Unlock()
		return nil, &ErrNotLeader{Leader: leader}
	}

	index := n.lastLogIndex() + 1
	entry := &logpb.LogEntry{
		Index:   index,
		Term:    n.currentTerm,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	n.pendingPersist = append(n.pendingPersist, entry)
	p := &pendingPropose{
		index:   index,
		outcome: make(chan *proposeOutcome, 1),
	}
	n.pending = append(n.pending, p)
	n.mu.Unlock()

	// 通知持久化 goroutine 处理批量；信号丢失时 persistLoop 也会
	// 在下一轮主动检查 pendingPersist。
	select {
	case n.persistCh <- struct{}{}:
	default:
	}

	select {
	case out := <-p.outcome:
		return out.res, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// persistLoop 是 group commit 的批处理者：把并发 Propose 追加的日志
// 合并为一批，一次 fsync 后统一推进提交并推送复制。
func (n *Node) persistLoop() {
	for {
		n.mu.Lock()
		if len(n.pendingPersist) == 0 {
			n.mu.Unlock()
			select {
			case <-n.ctx.Done():
				return
			case <-n.persistCh:
			}
			continue
		}
		batch := n.pendingPersist
		n.pendingPersist = nil
		n.mu.Unlock()

		if n.wal != nil {
			if err := n.wal.Append(batch); err != nil {
				n.failPersistBatch(batch, err)
				continue
			}
		}
		n.mu.Lock()
		committed := n.tryCommitLocked()
		// 立即推送复制：新日志不必等下一个心跳周期才发给 follower
		n.broadcastHeartbeatLocked()
		n.mu.Unlock()
		if committed > 0 {
			n.notifyApply(committed)
		}
	}
}

// failPersistBatch 在批量落盘失败时回滚内存日志并回复未决 Propose。
func (n *Node) failPersistBatch(batch []*logpb.LogEntry, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	first := batch[0].Index
	keep := int(first - n.snapshotIndex - 1)
	if keep < 0 {
		keep = 0
	}
	if keep > len(n.log) {
		keep = len(n.log)
	}
	n.log = n.log[:keep]

	var keepPending []*pendingPropose
	for _, p := range n.pending {
		if p.index >= first {
			p.outcome <- &proposeOutcome{err: fmt.Errorf("persist log: %w", err)}
		} else {
			keepPending = append(keepPending, p)
		}
	}
	n.pending = keepPending
	n.cfg.Logger.Printf("raft: persist batch failed, rolled back %d entries: %v", len(batch), err)
}

// AddPeer 提交一条配置变更日志，把新节点加入集群（需在 Leader 上调用）。
// 新节点通过 InstallSnapshot 追赶日志后开始参与复制。
func (n *Node) AddPeer(ctx context.Context, id int32, addr string) error {
	return n.proposeConfChange(ctx, &logpb.ConfChange{
		Type:   logpb.ConfChange_ADD,
		NodeId: id,
		Addr:   addr,
	})
}

// RemovePeer 提交一条配置变更日志，把节点移出集群。
func (n *Node) RemovePeer(ctx context.Context, id int32) error {
	return n.proposeConfChange(ctx, &logpb.ConfChange{
		Type:   logpb.ConfChange_REMOVE,
		NodeId: id,
	})
}

// proposeConfChange 把配置变更作为普通日志条目复制并等待应用完成。
func (n *Node) proposeConfChange(ctx context.Context, cc *logpb.ConfChange) error {
	if err := n.WaitCaughtUp(ctx); err != nil {
		return err
	}
	n.mu.Lock()
	if cc.Type == logpb.ConfChange_REMOVE {
		n.removing[cc.NodeId] = true
	}
	n.mu.Unlock()
	_, err := n.Propose(ctx, &logpb.DeterministicCommand{
		Cmd: &logpb.DeterministicCommand_ConfChange{ConfChange: cc},
	})
	return err
}

// WaitCaughtUp 等待状态机追平 commitIndex（历史已提交条目全部应用）。
// 在 applyMu 上等待，applyLoop 每次应用后会广播。
func (n *Node) WaitCaughtUp(ctx context.Context) error {
	return n.WaitApplied(ctx, n.CommitIndex())
}

// WaitApplied 等待状态机应用至指定日志索引（含）。
func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	for {
		if n.appliedIndex >= index {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		n.applyCond.Wait()
	}
}

// ---------- 日志辅助函数（调用方须持有锁） ----------

// lastLogIndex 返回日志最高索引（含快照前缀）。
func (n *Node) lastLogIndex() uint64 {
	return n.snapshotIndex + uint64(len(n.log))
}

func (n *Node) lastLogTerm() uint64 {
	return n.logTermAt(n.lastLogIndex())
}

// logTermAt 返回指定索引的任期；索引落在快照前缀内时返回 snapshotTerm。
func (n *Node) logTermAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index <= n.snapshotIndex {
		return n.snapshotTerm
	}
	if index > n.lastLogIndex() {
		return 0
	}
	return n.log[index-n.snapshotIndex-1].Term
}

// entryAt 返回指定索引的日志条目；索引不在内存范围内时返回 nil。
func (n *Node) entryAt(index uint64) *logpb.LogEntry {
	if index <= n.snapshotIndex || index > n.lastLogIndex() {
		return nil
	}
	return n.log[index-n.snapshotIndex-1]
}

// electionTimeout 返回本次选举超时：基础值 + 随机抖动，
// 避免多个节点同时超时导致票数分裂、选举活锁。
func (n *Node) electionTimeout() time.Duration {
	return n.cfg.ElectionTimeout + time.Duration(rand.Int63n(int64(n.cfg.ElectionTimeout)))
}

// ---------- 持久化 ----------

// persistStateLocked 在任期/投票对象与磁盘不一致时原子落盘。
func (n *Node) persistStateLocked() {
	if n.wal == nil {
		return
	}
	// 快照索引变化也必须落盘（InstallSnapshot / 压缩后崩溃恢复依赖它）
	if n.currentTerm == n.persistedTerm && n.votedFor == n.persistedVoted &&
		n.snapshotIndex == n.persistedSnap {
		return
	}
	st := raftwal.State{
		CurrentTerm:   n.currentTerm,
		VotedFor:      n.votedFor,
		SnapshotIndex: n.snapshotIndex,
		SnapshotTerm:  n.snapshotTerm,
	}
	if err := n.wal.PersistState(st); err != nil {
		n.cfg.Logger.Printf("raft: persist state failed: %v", err)
		return
	}
	n.persistedTerm = n.currentTerm
	n.persistedVoted = n.votedFor
	n.persistedSnap = n.snapshotIndex
}

// rewriteLogLocked 全量重写日志文件（截断冲突条目后调用）。
func (n *Node) rewriteLogLocked() {
	if n.wal == nil {
		return
	}
	if err := n.wal.Rewrite(n.log); err != nil {
		n.cfg.Logger.Printf("raft: rewrite log failed: %v", err)
	}
}

// ---------- 定时器 ----------

// tickerLoop 独占所有定时触发：按 nextWake 休眠，被 wakeCh 唤醒后重新计算。
func (n *Node) tickerLoop() {
	for {
		n.mu.Lock()
		timeout := time.Until(n.nextWake)
		n.mu.Unlock()
		if timeout < 0 {
			timeout = 0
		}

		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
		case <-n.wakeCh:
			// deadline 被改动，停止本次休眠并重新计算
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-n.ctx.Done():
			return
		}

		n.mu.Lock()
		if time.Now().Before(n.nextWake) {
			n.mu.Unlock()
			continue
		}
		n.onTimerFireLocked()
		n.mu.Unlock()
	}
}

// setNextWakeLocked 设置下一次定时触发时间并唤醒 tickerLoop（调用方须持有锁）。
func (n *Node) setNextWakeLocked(d time.Duration) {
	n.nextWake = time.Now().Add(d)
	select {
	case n.wakeCh <- struct{}{}:
	default:
	}
}

// onTimerFireLocked 定时触发：Follower/Candidate 发起选举，Leader 发送心跳。
func (n *Node) onTimerFireLocked() {
	switch n.serverState {
	case RoleFollower, RoleCandidate:
		n.startElectionLocked()
	case RoleLeader:
		n.broadcastHeartbeatLocked()
		n.setNextWakeLocked(n.cfg.HeartbeatInterval)
	}
}

// ---------- 选举 ----------

func (n *Node) startElectionLocked() {
	n.serverState = RoleCandidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.votes = 1
	n.persistStateLocked()

	term := n.currentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	for peerID, pc := range n.clients {
		go n.sendRequestVote(peerID, pc.client, term, lastLogIndex, lastLogTerm)
	}

	// 单节点集群：自己的一票即达到多数派，无需等待任何投票回复
	if n.votes >= n.majoritySize {
		n.becomeLeaderLocked()
		return
	}
	n.setNextWakeLocked(n.electionTimeout())
}

func (n *Node) sendRequestVote(peerID int32, client raftpb.RaftServiceClient,
	term, lastLogIndex, lastLogTerm uint64) {
	callCtx, cancel := context.WithTimeout(n.ctx, rpcTimeout)
	defer cancel()
	reply, err := client.RequestVote(callCtx, &raftpb.RequestVoteRequest{
		From:         n.cfg.ID,
		To:           peerID,
		Term:         term,
		CandidateId:  n.cfg.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.serverState == RoleCandidate && reply.Term == n.currentTerm && reply.Granted {
		n.votes++
		if n.votes >= n.majoritySize {
			n.becomeLeaderLocked()
		}
	}
}

// stepDownLocked 使节点退位为 Follower：更新任期、清空投票记录、
// 停止 follower goroutine、以 ErrNotLeader 回复未决 Propose 并重置选举超时。
func (n *Node) stepDownLocked(newTerm uint64) {
	if newTerm > n.currentTerm {
		n.currentTerm = newTerm
	}
	wasLeader := n.serverState == RoleLeader
	n.serverState = RoleFollower
	n.votedFor = -1
	n.currentLeader = -1

	if wasLeader {
		for _, stopCh := range n.followerStops {
			close(stopCh)
		}
		n.followerStops = nil
		n.followerTicks = nil
		for _, p := range n.pending {
			p.outcome <- &proposeOutcome{err: &ErrNotLeader{Leader: -1}}
		}
		n.pending = n.pending[:0]
		for _, ri := range n.readIndexes {
			ri.done <- &ErrNotLeader{Leader: -1}
		}
		n.readIndexes = n.readIndexes[:0]
	}
	n.persistStateLocked()
	n.setNextWakeLocked(n.electionTimeout())
}

// becomeLeaderLocked 当选后初始化 nextIndex/matchIndex，启动每个 follower 的
// 常驻发送 goroutine，并立即广播一次空 AppendEntries 宣告领导地位。
func (n *Node) becomeLeaderLocked() {
	n.serverState = RoleLeader
	n.currentLeader = n.cfg.ID
	for peerID := range n.clients {
		n.nextIndex[peerID] = n.lastLogIndex() + 1
		n.matchIndex[peerID] = 0
	}

	n.followerTicks = make(map[int32]chan struct{})
	n.followerStops = make(map[int32]chan struct{})
	for peerID, pc := range n.clients {
		tickCh := make(chan struct{}, 1)
		stopCh := make(chan struct{})
		n.followerTicks[peerID] = tickCh
		n.followerStops[peerID] = stopCh
		go n.followerLoop(peerID, pc.client, tickCh, stopCh)
	}

	// 立即宣告领导地位，不等下一个心跳周期
	n.broadcastHeartbeatLocked()
	n.setNextWakeLocked(n.cfg.HeartbeatInterval)
}

// ---------- 日志复制 ----------

func (n *Node) buildAppendEntriesRequestLocked(peerID int32) *raftpb.AppendEntriesRequest {
	nextIndex := n.nextIndex[peerID] // 下一条要发送的日志索引
	if nextIndex > n.lastLogIndex()+1 {
		nextIndex = n.lastLogIndex() + 1
	}
	prevLogIndex := uint64(0)
	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
	}

	var entries []*logpb.LogEntry
	if nextIndex > n.snapshotIndex && nextIndex <= n.lastLogIndex() {
		start := int(nextIndex - n.snapshotIndex - 1) // 内存切片下标
		end := start + maxBatch
		if end > len(n.log) {
			end = len(n.log)
		}
		entries = n.log[start:end]
	}

	return &raftpb.AppendEntriesRequest{
		From:         n.cfg.ID,
		To:           peerID,
		Term:         n.currentTerm,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  n.logTermAt(prevLogIndex),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// broadcastHeartbeatLocked 把一次心跳任务分发给所有 follower 的常驻 goroutine。
func (n *Node) broadcastHeartbeatLocked() {
	for _, tickCh := range n.followerTicks {
		select {
		case tickCh <- struct{}{}:
		default: // 上一轮 RPC 尚未完成，跳过本次心跳
		}
	}
}

// followerLoop 是每个 follower 的常驻发送 goroutine：Leader 在任期间复用。
func (n *Node) followerLoop(peerID int32, client raftpb.RaftServiceClient,
	tick <-chan struct{}, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-n.ctx.Done():
			return
		case <-tick:
			n.mu.Lock()
			if n.serverState != RoleLeader {
				n.mu.Unlock()
				continue // 已退位，丢弃本任务
			}
			// follower 落后于已压缩的日志：改用 InstallSnapshot 追赶
			if n.nextIndex[peerID] <= n.snapshotIndex && n.snapshotData != nil {
				snapIndex := n.snapshotIndex
				snapTerm := n.snapshotTerm
				n.cfg.Logger.Printf("raft: sending snapshot to peer %d (next=%d snap=%d)",
					peerID, n.nextIndex[peerID], snapIndex)
				req := &raftpb.InstallSnapshotRequest{
					From:              n.cfg.ID,
					To:                peerID,
					Term:              n.currentTerm,
					LeaderId:          n.cfg.ID,
					LastIncludedIndex: snapIndex,
					LastIncludedTerm:  snapTerm,
					Data:              n.snapshotData,
				}
				n.mu.Unlock()

				callCtx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
				reply, err := client.InstallSnapshot(callCtx, req)
				cancel()
				if err != nil {
					continue
				}
				n.mu.Lock()
				committed := n.handleSnapshotResultLocked(reply, peerID, snapIndex)
				n.mu.Unlock()
				if committed > 0 {
					n.notifyApply(committed)
				}
				continue
			}
			req := n.buildAppendEntriesRequestLocked(peerID)
			n.mu.Unlock()

			callCtx, cancel := context.WithTimeout(n.ctx, rpcTimeout)
			reply, err := client.AppendEntries(callCtx, req)
			cancel()
			if err != nil {
				continue
			}

			n.mu.Lock()
			committed := n.handleAppendResultLocked(reply, peerID)
			n.mu.Unlock()
			if committed > 0 {
				n.notifyApply(committed)
			}
		}
	}
}

// handleSnapshotResultLocked 处理 InstallSnapshot 回复；
// snapIndex 是发送该快照时的索引，不能使用处理回复时的当前 snapshotIndex
// （期间 Leader 可能已推进快照，导致错误估算 follower 的进度）。
func (n *Node) handleSnapshotResultLocked(reply *raftpb.InstallSnapshotResponse, peerID int32, snapIndex uint64) uint64 {
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return 0
	}
	if n.serverState != RoleLeader || reply.Term != n.currentTerm {
		return 0
	}
	n.ackReadIndexLocked(peerID, reply.Term)
	if reply.Success {
		// follower 已应用快照：直接推进到快照末尾
		n.matchIndex[peerID] = snapIndex
		n.nextIndex[peerID] = snapIndex + 1
		return n.tryCommitLocked()
	}
	return 0
}

// handleAppendResultLocked 处理一次 AppendEntries 回复，
// 返回推进后的 commitIndex（未推进返回 0）。
func (n *Node) handleAppendResultLocked(reply *raftpb.AppendEntriesResponse, peerID int32) uint64 {
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return 0
	}
	if n.serverState != RoleLeader || reply.Term != n.currentTerm {
		return 0 // 陈旧/跨任期的回复，忽略
	}
	// 回复证明 follower 认可当前任期：用于 ReadIndex 的多数派确认
	// （即使 Success=false 也有效，因为任期合法性已确认）。
	n.ackReadIndexLocked(peerID, reply.Term)
	if reply.Success {
		n.matchIndex[peerID] = reply.MatchIndex
		n.nextIndex[peerID] = reply.MatchIndex + 1
		return n.tryCommitLocked()
	}
	// 日志不一致：指数回退 nextIndex，快速收敛到双方一致的位置
	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID] /= 2
		if n.nextIndex[peerID] < 1 {
			n.nextIndex[peerID] = 1
		}
		n.cfg.Logger.Printf("raft: append failed peer=%d next=%d", peerID, n.nextIndex[peerID])
	}
	return 0
}

// tryCommitLocked 尝试推进 commitIndex，返回推进后的 commitIndex（未推进返回 0）。
// 按 Raft 论文，Leader 只能直接提交“当前任期”的日志条目；
// 一旦当前任期条目被多数派复制，其之前的所有条目一并提交。
func (n *Node) tryCommitLocked() uint64 {
	if n.commitIndex >= n.lastLogIndex() {
		return 0
	}

	// 待移除节点不计入多数派（其 ConfChange 及之前日志由其余节点提交）
	effective := 1 // 自己
	for peerID := range n.matchIndex {
		if !n.removing[peerID] {
			effective++
		}
	}
	majority := effective/2 + 1

	replicatedIdx := n.commitIndex
	for i := n.commitIndex + 1; i <= n.lastLogIndex(); i++ {
		count := 1 // 自己
		for peerID, mi := range n.matchIndex {
			if n.removing[peerID] {
				continue
			}
			if mi >= i {
				count++
			}
		}
		if count >= majority {
			replicatedIdx = i
		} else {
			break
		}
	}

	if replicatedIdx > n.commitIndex && n.logTermAt(replicatedIdx) == n.currentTerm {
		n.commitIndex = replicatedIdx
		return replicatedIdx
	}
	return 0
}

// notifyApply 通知 applyLoop 有新的已提交日志可应用。
// 必须在未持有 n.mu 时调用，且通过 applyMu 加锁发送信号，避免丢失唤醒。
func (n *Node) notifyApply(committed uint64) {
	n.applyMu.Lock()
	if committed > n.applyTarget {
		n.applyTarget = committed
	}
	n.applyCond.Broadcast()
	n.applyMu.Unlock()
}

// applyLoop 是唯一的日志应用者：按 commitIndex 推进，严格按序调用 cfg.Apply，
// 并把应用结果回复给等待该索引的 Propose。
func (n *Node) applyLoop() {
	for {
		n.applyMu.Lock()
		for n.appliedIndex >= n.applyTarget {
			if n.ctx.Err() != nil {
				n.applyMu.Unlock()
				return
			}
			n.applyCond.Wait()
		}

		index := n.appliedIndex + 1
		n.mu.RLock()
		entry := n.entryAt(index)
		n.mu.RUnlock()
		if entry == nil {
			// 防御：索引不在内存日志内。保守压缩下不应发生，
			// 短暂等待避免空转。
			n.cfg.Logger.Printf("raft: entry %d missing (snapshot=%d log=%d)",
				index, n.snapshotIndex, len(n.log))
			n.applyMu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var (
			res *ApplyResult
			err error
		)
		if cc := entry.GetCommand().GetConfChange(); cc != nil {
			// 集群配置变更：不进入状态机，直接更新动态配置
			err = n.applyConfChange(cc)
		} else if n.cfg.Apply != nil {
			res, err = n.cfg.Apply(n.ctx, index, entry)
		}

		n.mu.Lock()
		for i, p := range n.pending {
			if p.index == index {
				p.outcome <- &proposeOutcome{res: res, err: err}
				n.pending = append(n.pending[:i], n.pending[i+1:]...)
				break
			}
		}
		n.mu.Unlock()

		n.appliedIndex = index
		n.applyCond.Broadcast() // 唤醒等待追平的 Propose / 读路径

		if n.cfg.CompactAfter > 0 {
			n.maybeCompactLocked() // 持有 applyMu：快照生成与日志应用互斥
		}
		n.applyMu.Unlock()
	}
}

// ---------- 日志压缩（保守策略） ----------

// maybeCompactLocked 在 Leader 上触发快照压缩（须持有 applyMu）。
// 与 M1 的“全节点已复制才压缩”不同，Leader 独立压缩已应用前缀，
// 滞后的 follower 通过 InstallSnapshot 追赶。
func (n *Node) maybeCompactLocked() {
	if n.cfg.SnapshotData == nil {
		return
	}
	n.mu.Lock()
	if n.serverState != RoleLeader {
		n.mu.Unlock()
		return
	}
	index := n.appliedIndex // applyMu 保护，压缩期间不会推进
	if index < n.snapshotIndex+n.cfg.CompactAfter {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	data, err := n.cfg.SnapshotData(n.ctx)
	if err != nil {
		n.cfg.Logger.Printf("raft: snapshot generation failed: %v", err)
		return
	}
	n.mu.Lock()
	term := n.logTermAt(index)
	n.compactLocked(index, term, data)
	n.mu.Unlock()
}

// compactLocked 丢弃 snapshotIndex 及之前的日志并缓存快照数据
// （调用方须持有锁）。
func (n *Node) compactLocked(index, term uint64, data []byte) {
	drop := int(index - n.snapshotIndex)

	// 先持久化快照位置，再重写日志；
	// 若在两者之间崩溃，恢复时 LoadLog 会按新快照位置裁剪旧日志。
	if n.wal != nil {
		st := raftwal.State{
			CurrentTerm:   n.currentTerm,
			VotedFor:      n.votedFor,
			SnapshotIndex: index,
			SnapshotTerm:  term,
		}
		if err := n.wal.PersistState(st); err != nil {
			n.cfg.Logger.Printf("raft: persist snapshot state failed: %v", err)
			return
		}
		n.persistedTerm = n.currentTerm
		n.persistedVoted = n.votedFor
		n.persistedSnap = index
	}

	n.snapshotIndex = index
	n.snapshotTerm = term
	n.snapshotData = data
	n.log = n.log[drop:]
	n.rewriteLogLocked()
	n.cfg.Logger.Printf("raft: compacted log through index %d (term %d)", index, term)
}

// applyConfChange 把已提交的配置变更应用到动态配置（须持有 applyMu）。
func (n *Node) applyConfChange(cc *logpb.ConfChange) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch cc.Type {
	case logpb.ConfChange_ADD:
		if _, exists := n.clients[cc.NodeId]; !exists {
			if err := n.dialPeer(cc.NodeId, cc.Addr); err != nil {
				return err
			}
			n.peers[cc.NodeId] = cc.Addr
			if n.serverState == RoleLeader {
				n.nextIndex[cc.NodeId] = n.lastLogIndex() + 1
				n.matchIndex[cc.NodeId] = 0
				tickCh := make(chan struct{}, 1)
				stopCh := make(chan struct{})
				n.followerTicks[cc.NodeId] = tickCh
				n.followerStops[cc.NodeId] = stopCh
				go n.followerLoop(cc.NodeId, n.clients[cc.NodeId].client, tickCh, stopCh)
			}
		} else {
			n.peers[cc.NodeId] = cc.Addr // 地址更新
		}
	case logpb.ConfChange_REMOVE:
		delete(n.peers, cc.NodeId)
		delete(n.removing, cc.NodeId)
		if stopCh, ok := n.followerStops[cc.NodeId]; ok {
			close(stopCh)
		}
		delete(n.followerTicks, cc.NodeId)
		delete(n.followerStops, cc.NodeId)
		if pc, ok := n.clients[cc.NodeId]; ok {
			pc.conn.Close()
		}
		delete(n.clients, cc.NodeId)
		delete(n.nextIndex, cc.NodeId)
		delete(n.matchIndex, cc.NodeId)
	}

	if cc.NodeId == n.cfg.ID && cc.Type == logpb.ConfChange_REMOVE {
		// 本节点被移出集群：退位为 Follower，不再竞选
		n.serverState = RoleFollower
		n.currentLeader = -1
		for _, stopCh := range n.followerStops {
			close(stopCh)
		}
		n.followerStops = nil
		n.followerTicks = nil
	}

	n.majoritySize = (len(n.peers)+1)/2 + 1
	if n.wal != nil {
		if err := n.wal.SavePeers(n.peers); err != nil {
			n.cfg.Logger.Printf("raft: persist peers failed: %v", err)
		}
	}
	n.cfg.Logger.Printf("raft: applied config change %s node=%d (peers=%v)",
		cc.Type.String(), cc.NodeId, n.peers)
	return nil
}

// ---------- 节点间 RPC ----------

// RequestVote 处理投票请求。
func (n *Node) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := &raftpb.RequestVoteResponse{From: req.To, To: req.From}

	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		reply.Granted = false
	} else {
		if req.Term > n.currentTerm {
			n.stepDownLocked(req.Term)
		}
		// 日志新旧检查：候选人的日志必须至少与自己一样新
		upToDate := req.LastLogTerm > n.lastLogTerm() ||
			(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())
		if upToDate && (n.votedFor == -1 || n.votedFor == req.CandidateId) {
			reply.Granted = true
		}
	}

	if reply.Granted {
		n.votedFor = req.CandidateId
		n.currentLeader = req.CandidateId
		n.serverState = RoleFollower
		n.persistStateLocked()
		n.setNextWakeLocked(n.electionTimeout())
	}
	reply.Term = n.currentTerm
	return reply, nil
}

// AppendEntries 处理日志追加/心跳请求。
func (n *Node) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	reply := &raftpb.AppendEntriesResponse{From: req.To, To: req.From, Success: true}
	committed := uint64(0)

	n.mu.Lock()
	if req.Term < n.currentTerm {
		reply.Success = false
	} else {
		// 任期合法的 AppendEntries 说明集群中存在 Leader：
		// 无论日志一致性检查是否通过，都确认 Follower 身份并重置选举超时
		n.stepDownLocked(req.Term)
		n.currentLeader = req.LeaderId

		// 一致性检查：prevLogIndex 处的日志必须与 Leader 声称的任期一致
		if req.PrevLogIndex > 0 {
			switch {
			case req.PrevLogIndex > n.lastLogIndex():
				n.cfg.Logger.Printf("raft: append reject prev=%d > last=%d (snap=%d)",
					req.PrevLogIndex, n.lastLogIndex(), n.snapshotIndex)
				reply.Success = false
			case n.logTermAt(req.PrevLogIndex) != req.PrevLogTerm:
				n.cfg.Logger.Printf("raft: append reject prev=%d term=%d vs %d (snap=%d term=%d)",
					req.PrevLogIndex, n.logTermAt(req.PrevLogIndex), req.PrevLogTerm,
					n.snapshotIndex, n.snapshotTerm)
				reply.Success = false
				if req.PrevLogIndex > n.snapshotIndex {
					// 截断冲突日志（不含快照前缀），并全量重写日志文件
					n.log = n.log[:req.PrevLogIndex-n.snapshotIndex-1]
					n.rewriteLogLocked()
				} else {
					n.cfg.Logger.Printf("raft: term mismatch inside snapshot prefix at index %d",
						req.PrevLogIndex)
				}
			}
		}

		if reply.Success {
			startIdx := req.PrevLogIndex
			appended := 0
			rewritten := false
			for i, entry := range req.Entries {
				// entries[0] 对应日志索引 prevLogIndex+1
				logIdx := startIdx + uint64(i) + 1
				if logIdx > 0 && logIdx <= n.snapshotIndex {
					// 快照前缀内的条目：任期必须一致，否则视为协议异常
					if n.logTermAt(logIdx) != entry.Term {
						n.cfg.Logger.Printf("raft: conflict inside snapshot prefix at index %d", logIdx)
						reply.Success = false
						break
					}
					continue
				}
				if logIdx <= n.lastLogIndex() {
					// 条目已存在：任期必须一致，否则截断并追加。
					// 注意 == 情况也必须检查（崩溃重启的节点可能持有
					// 与 Leader 冲突的最后一条日志）。
					if n.logTermAt(logIdx) != entry.Term {
						n.log = n.log[:logIdx-n.snapshotIndex-1]
						n.log = append(n.log, req.Entries[i:]...)
						appended = len(req.Entries) - i
						rewritten = true
						break
					}
					continue
				}
				n.log = append(n.log, req.Entries[i:]...)
				appended = len(req.Entries) - i
				break
			}

			reply.MatchIndex = startIdx + uint64(len(req.Entries))
			if reply.Success && n.wal != nil {
				if rewritten {
					n.rewriteLogLocked() // 文件里残留被截断的旧条目，必须全量重写
				} else if appended > 0 {
					tail := req.Entries[len(req.Entries)-appended:]
					if err := n.wal.Append(tail); err != nil {
						n.cfg.Logger.Printf("raft: persist log failed: %v", err)
						// 未落盘不能应答成功：回滚内存日志，等待 Leader 重试
						n.log = n.log[:len(n.log)-appended]
						reply.Success = false
					}
				}
			}

			if reply.Success && req.LeaderCommit > n.commitIndex {
				newCommit := req.LeaderCommit
				if newCommit > n.lastLogIndex() {
					newCommit = n.lastLogIndex()
				}
				if newCommit > n.commitIndex {
					n.commitIndex = newCommit
					committed = newCommit
				}
			}
		}
	}
	reply.Term = n.currentTerm
	n.mu.Unlock()

	if committed > 0 {
		n.notifyApply(committed)
	}
	return reply, nil
}

// InstallSnapshot 接收 Leader 推送的状态机快照并替换本地状态。
func (n *Node) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	reply := &raftpb.InstallSnapshotResponse{From: req.To, To: req.From, Success: false}

	n.mu.Lock()
	if req.Term < n.currentTerm {
		reply.Term = n.currentTerm
		n.mu.Unlock()
		return reply, nil
	}
	n.stepDownLocked(req.Term)
	n.currentLeader = req.LeaderId
	n.mu.Unlock()

	// 锁外应用快照：通过 applyMu 与日志应用互斥，避免替换状态机时
	// applyLoop 正在写入旧数据库。
	n.applyMu.Lock()
	if n.cfg.ApplySnapshot != nil {
		if err := n.cfg.ApplySnapshot(n.ctx, req.Data); err != nil {
			n.cfg.Logger.Printf("raft: apply snapshot failed: %v", err)
			n.mu.Lock()
			reply.Term = n.currentTerm
			n.mu.Unlock()
			n.applyMu.Unlock()
			return reply, nil
		}
	} else {
		n.cfg.Logger.Printf("raft: rejecting snapshot (no ApplySnapshot callback)")
		n.mu.Lock()
		reply.Term = n.currentTerm
		n.mu.Unlock()
		n.applyMu.Unlock()
		return reply, nil
	}

	n.mu.Lock()
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.snapshotData = nil // 快照数据只保留在状态机里
	n.log = n.log[:0]
	n.commitIndex = req.LastIncludedIndex
	n.applyTarget = req.LastIncludedIndex
	n.appliedIndex = req.LastIncludedIndex // applyMu 保护
	n.persistStateLocked()
	n.rewriteLogLocked()
	reply.Success = true
	reply.Term = n.currentTerm
	n.mu.Unlock()
	n.applyMu.Unlock()

	n.cfg.Logger.Printf("raft: installed snapshot through index %d (term %d)",
		req.LastIncludedIndex, req.LastIncludedTerm)
	return reply, nil
}
