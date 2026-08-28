package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raftwal"
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

// testHarness 组合一个 raft 节点与本地 SQLite 状态机。
type testHarness struct {
	node  *Node
	store *store.Store
	wal   *raftwal.Wal
	dir   string
}

func newHarness(t *testing.T, dir string, compactAfter uint64) *testHarness {
	t.Helper()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wal, err := raftwal.Open(filepath.Join(dir, "raft"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	applied, err := st.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	node, err := New(Config{
		ID:                0,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Wal:               wal,
		AppliedIndex:      applied,
		CompactAfter:      compactAfter,
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*ApplyResult, error) {
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			return &ApplyResult{
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
	node.Start()
	return &testHarness{node: node, store: st, wal: wal, dir: dir}
}

func (h *testHarness) close() {
	h.node.Stop()
	h.wal.Close()
	h.store.Close()
}

func (h *testHarness) waitLeader(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !h.node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// exec 提交一条写命令并等待应用完成。
func (h *testHarness) exec(t *testing.T, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.node.Propose(ctx, &logpb.DeterministicCommand{
		Cmd: &logpb.DeterministicCommand_Exec{Exec: &logpb.Exec{Sql: sql}},
	})
	if err != nil {
		t.Fatalf("propose %q: %v", sql, err)
	}
}

func (h *testHarness) rowCount(t *testing.T) int {
	t.Helper()
	_, rows, err := h.store.Query(context.Background(), "SELECT v FROM t", nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	return len(rows)
}

// TestPersistentRecovery 验证：日志与状态落盘后，重启节点不丢失数据、
// 不重复应用已提交的日志。
func TestPersistentRecovery(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, dir, 0)
	h.waitLeader(t)

	h.exec(t, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
	h.exec(t, `INSERT INTO t (v) VALUES ('a')`)
	h.exec(t, `INSERT INTO t (v) VALUES ('b')`)
	if got := h.rowCount(t); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
	h.close()

	// 重启：从 WAL 恢复日志，从 SQLite 元数据恢复 appliedIndex
	h2 := newHarness(t, dir, 0)
	defer h2.close()
	h2.waitLeader(t)

	if got := h2.rowCount(t); got != 2 {
		t.Fatalf("rows after recovery = %d, want 2（不允许重复应用）", got)
	}
	// 恢复后仍可继续写入
	h2.exec(t, `INSERT INTO t (v) VALUES ('c')`)
	if got := h2.rowCount(t); got != 3 {
		t.Fatalf("rows after new write = %d, want 3", got)
	}
}

// TestCompaction 验证保守压缩：只丢弃已应用且全部节点已复制的日志前缀。
func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t, dir, 3)
	h.waitLeader(t)

	h.exec(t, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
	for i := 0; i < 5; i++ {
		h.exec(t, `INSERT INTO t (v) VALUES ('x')`)
	}
	if got := h.rowCount(t); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}

	// 第 3、6 条应用后触发压缩（CompactAfter=3）
	deadline := time.Now().Add(5 * time.Second)
	for h.node.SnapshotIndex() < 6 {
		if time.Now().After(deadline) {
			t.Fatalf("snapshot index = %d, want >= 6", h.node.SnapshotIndex())
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.close()

	// 压缩后的日志可恢复，且快照位置被持久化
	h2 := newHarness(t, dir, 0)
	defer h2.close()
	h2.waitLeader(t)
	if got := h2.rowCount(t); got != 5 {
		t.Fatalf("rows after recovery = %d, want 5", got)
	}
	if got := h2.node.SnapshotIndex(); got != 6 {
		t.Fatalf("snapshot index after recovery = %d, want 6", got)
	}
	// 压缩后继续写入
	h2.exec(t, `INSERT INTO t (v) VALUES ('y')`)
	if got := h2.rowCount(t); got != 6 {
		t.Fatalf("rows after new write = %d, want 6", got)
	}
}

// TestWaitCaughtUp 验证 Propose 在历史条目应用前不会读取到不一致状态。
func TestWaitCaughtUp(t *testing.T) {
	h := newHarness(t, t.TempDir(), 0)
	defer h.close()
	h.waitLeader(t)

	// 未提交时立即追平
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.node.WaitCaughtUp(ctx); err != nil {
		t.Fatalf("wait caught up: %v", err)
	}
}

// TestReadIndexSingleNode 验证单节点集群的 ReadIndex 立即返回
// 且状态机追平后可安全读取。
func TestReadIndexSingleNode(t *testing.T) {
	h := newHarness(t, t.TempDir(), 0)
	defer h.close()
	h.waitLeader(t)

	h.exec(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	h.exec(t, `INSERT INTO t (v) VALUES ('x')`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ri, err := h.node.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if ri == 0 {
		t.Fatalf("read index = 0, want >= 1")
	}
	if err := h.node.WaitApplied(ctx, ri); err != nil {
		t.Fatalf("wait applied: %v", err)
	}
	if got := h.rowCount(t); got != 1 {
		t.Fatalf("rows = %d, want 1", got)
	}
}
