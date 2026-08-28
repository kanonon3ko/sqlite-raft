package raftwal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
)

func entry(idx, term uint64) *logpb.LogEntry {
	return &logpb.LogEntry{Index: idx, Term: term}
}

func TestAppendAndRecover(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.PersistState(State{CurrentTerm: 3, VotedFor: 1}); err != nil {
		t.Fatalf("persist state: %v", err)
	}
	if err := w.Append([]*logpb.LogEntry{entry(1, 2), entry(2, 2)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	w.Close()

	// 重新打开：应恢复状态与日志
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	if st := w2.State(); st.CurrentTerm != 3 || st.VotedFor != 1 {
		t.Fatalf("state = %+v, want term=3 voted=1", st)
	}
	entries, err := w2.LoadLog()
	if err != nil {
		t.Fatalf("load log: %v", err)
	}
	if len(entries) != 2 || entries[0].Index != 1 || entries[1].Index != 2 {
		t.Fatalf("entries = %v", entries)
	}
}

func TestRewriteLog(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if err := w.Append([]*logpb.LogEntry{entry(1, 1), entry(2, 1), entry(3, 1)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// 截断为前两条
	if err := w.Rewrite([]*logpb.LogEntry{entry(1, 1), entry(2, 1)}); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	entries, err := w.LoadLog()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(entries) != 2 || entries[1].Index != 2 {
		t.Fatalf("entries after rewrite = %v", entries)
	}

	// 截断后继续追加
	if err := w.Append([]*logpb.LogEntry{entry(3, 1)}); err != nil {
		t.Fatalf("append after rewrite: %v", err)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	entries, err = w2.LoadLog()
	if err != nil {
		t.Fatalf("load after reopen: %v", err)
	}
	if len(entries) != 3 || entries[2].Index != 3 {
		t.Fatalf("entries after reopen = %v", entries)
	}
}

func TestLoadLogDropsEntriesBelowSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.Append([]*logpb.LogEntry{entry(1, 1), entry(2, 1), entry(3, 2)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// 模拟“先持久化状态、后重写日志”的崩溃窗口：状态已推进但日志尚未压缩
	if err := w.PersistState(State{CurrentTerm: 2, VotedFor: -1, SnapshotIndex: 2, SnapshotTerm: 1}); err != nil {
		t.Fatalf("persist state: %v", err)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	entries, err := w2.LoadLog()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(entries) != 1 || entries[0].Index != 3 {
		t.Fatalf("entries = %v, want only index 3", entries)
	}
}

func TestCorruptLogFile(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.Append([]*logpb.LogEntry{entry(1, 1)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	w.Close()

	// 破坏文件内容
	if err := os.WriteFile(filepath.Join(dir, logFileName), []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("expected error for corrupt log file")
	}
}
