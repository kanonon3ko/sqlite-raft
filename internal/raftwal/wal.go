// Package raftwal 提供 Raft 日志与状态的持久化（WAL）。
//
// 落盘内容：
//   - state.bin：当前任期、投票对象、快照位置（index/term），原子替换写入；
//   - log.bin：长度前缀的 protobuf LogEntry 序列，追加写入并 fsync。
//
// 本包不保存内存副本：日志内容由 raft.Node 持有，本包只负责可靠落盘与恢复。
// 所有写操作都必须由调用方在持有 Node 锁的情况下串行调用。
package raftwal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
)

const (
	stateFileName = "state.bin"
	logFileName   = "log.bin"
	peersFileName = "peers.json"
	stateMagic    = 0x52414654 // "RAFT"
	stateVersion  = 1
)

// State 是需要持久化的 Raft 状态。
type State struct {
	CurrentTerm   uint64
	VotedFor      int32
	SnapshotIndex uint64 // 快照包含的最后一条日志索引（0 表示无快照）
	SnapshotTerm  uint64
}

// Wal 是日志与状态的持久化句柄。
type Wal struct {
	dir   string
	file  *os.File // log.bin 的追加句柄
	state State
}

// Open 打开（或创建）一个 WAL 目录，并加载已有状态与日志。
func Open(dir string) (*Wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir %q: %w", dir, err)
	}
	w := &Wal{dir: dir}

	if err := w.loadState(); err != nil {
		return nil, err
	}
	if err := w.loadLog(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(w.logPath(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	w.file = f
	return w, nil
}

// Close 关闭底层文件。
func (w *Wal) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Dir 返回 WAL 目录。
func (w *Wal) Dir() string { return w.dir }

// SavePeers 持久化当前集群配置（成员变更后调用）。
func (w *Wal) SavePeers(peers map[int32]string) error {
	// protobuf map 序列化为字符串键
	m := make(map[string]string, len(peers))
	for id, addr := range peers {
		m[fmt.Sprintf("%d", id)] = addr
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal peers: %w", err)
	}
	tmp := w.peersPath() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("write peers: %w", err)
	}
	if err := os.Rename(tmp, w.peersPath()); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("replace peers: %w", err)
	}
	return syncDir(w.dir)
}

// LoadPeers 读取持久化的集群配置；文件不存在时返回空 map。
func (w *Wal) LoadPeers() (map[int32]string, error) {
	buf, err := os.ReadFile(w.peersPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read peers: %w", err)
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, fmt.Errorf("unmarshal peers: %w", err)
	}
	peers := make(map[int32]string, len(m))
	for k, v := range m {
		var id int32
		if _, err := fmt.Sscanf(k, "%d", &id); err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", k, err)
		}
		peers[id] = v
	}
	return peers, nil
}

// State 返回当前持久化的状态。
func (w *Wal) State() State { return w.state }

// LoadLog 读取日志文件中的全部条目。
// 返回条目列表；条目索引小于快照位置的会被丢弃（对应“先持久化状态、后重写日志”
// 的崩溃窗口）。
func (w *Wal) LoadLog() ([]*logpb.LogEntry, error) {
	entries, err := readLogFile(w.logPath())
	if err != nil {
		return nil, err
	}
	if w.state.SnapshotIndex == 0 {
		return entries, nil
	}
	keep := 0
	for _, e := range entries {
		if e.Index > w.state.SnapshotIndex {
			break
		}
		keep++
	}
	return entries[keep:], nil
}

// Append 追加日志条目：写入文件并 fsync。
func (w *Wal) Append(entries []*logpb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	buf := make([]byte, 0, 4096)
	for _, e := range entries {
		b, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal log entry: %w", err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, b...)
	}
	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("append log: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync log: %w", err)
	}
	return nil
}

// Rewrite 全量重写日志文件（截断冲突条目或压缩后使用），原子替换。
func (w *Wal) Rewrite(entries []*logpb.LogEntry) error {
	tmp := w.logPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp log: %w", err)
	}
	for _, e := range entries {
		b, err := proto.Marshal(e)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("marshal log entry: %w", err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("write temp log: %w", err)
		}
		if _, err := f.Write(b); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("write temp log: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync temp log: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close temp log: %w", err)
	}
	if err := os.Rename(tmp, w.logPath()); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("replace log: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}

	// 重新打开追加句柄
	if w.file != nil {
		w.file.Close()
	}
	f, err = os.OpenFile(w.logPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopen log: %w", err)
	}
	w.file = f
	return nil
}

// PersistState 原子写入状态文件。
func (w *Wal) PersistState(s State) error {
	buf := make([]byte, 4+4+8+4+8+8)
	binary.BigEndian.PutUint32(buf[0:], stateMagic)
	binary.BigEndian.PutUint32(buf[4:], stateVersion)
	binary.BigEndian.PutUint64(buf[8:], s.CurrentTerm)
	binary.BigEndian.PutUint32(buf[16:], uint32(s.VotedFor))
	binary.BigEndian.PutUint64(buf[20:], s.SnapshotIndex)
	binary.BigEndian.PutUint64(buf[28:], s.SnapshotTerm)

	tmp := w.statePath() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	f, err := os.Open(tmp)
	if err != nil {
		return fmt.Errorf("open temp state: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync temp state: %w", err)
	}
	f.Close()
	if err := os.Rename(tmp, w.statePath()); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("replace state: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	w.state = s
	return nil
}

func (w *Wal) statePath() string { return filepath.Join(w.dir, stateFileName) }
func (w *Wal) logPath() string   { return filepath.Join(w.dir, logFileName) }
func (w *Wal) peersPath() string { return filepath.Join(w.dir, peersFileName) }

func (w *Wal) loadState() error {
	buf, err := os.ReadFile(w.statePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if len(buf) < 32 {
		return fmt.Errorf("state file %s: unexpected length %d", w.statePath(), len(buf))
	}
	if binary.BigEndian.Uint32(buf[0:]) != stateMagic {
		return fmt.Errorf("state file %s: bad magic", w.statePath())
	}
	if v := binary.BigEndian.Uint32(buf[4:]); v != stateVersion {
		return fmt.Errorf("state file %s: unsupported version %d", w.statePath(), v)
	}
	w.state = State{
		CurrentTerm:   binary.BigEndian.Uint64(buf[8:]),
		VotedFor:      int32(binary.BigEndian.Uint32(buf[16:])),
		SnapshotIndex: binary.BigEndian.Uint64(buf[20:]),
		SnapshotTerm:  binary.BigEndian.Uint64(buf[28:]),
	}
	return nil
}

func (w *Wal) loadLog() error {
	if _, err := os.Stat(w.logPath()); os.IsNotExist(err) {
		return nil
	}
	_, err := w.LoadLog() // 校验文件可读
	return err
}

func readLogFile(path string) ([]*logpb.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	var entries []*logpb.LogEntry
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				return entries, nil
			}
			return nil, fmt.Errorf("read log length: %w", err)
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n > 64<<20 {
			return nil, fmt.Errorf("log entry too large: %d bytes", n)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, fmt.Errorf("read log entry: %w", err)
		}
		e := &logpb.LogEntry{}
		if err := proto.Unmarshal(buf, e); err != nil {
			return nil, fmt.Errorf("unmarshal log entry: %w", err)
		}
		entries = append(entries, e)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}
