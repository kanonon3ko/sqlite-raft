// sqlraftd 是 SQLite over Raft 的守护进程：单进程 = 一个 Raft 节点 + 一个 SQLite 状态机。
//
// 用法示例：
//
//	单节点：
//	  sqlraftd -id 0 -addr 127.0.0.1:50051
//	三节点（依次启动）：
//	  sqlraftd -id 0 -addr 127.0.0.1:50051 -peers 1=127.0.0.1:50052,2=127.0.0.1:50053
//	  sqlraftd -id 1 -addr 127.0.0.1:50052 -peers 0=127.0.0.1:50051,2=127.0.0.1:50053
//	  sqlraftd -id 2 -addr 127.0.0.1:50053 -peers 0=127.0.0.1:50051,1=127.0.0.1:50052
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/raftpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/pgwire"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/raftwal"
	"github.com/kanonon3ko/sqlite-raft/internal/server"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

// maxGRPCMessage 是节点间消息大小上限（InstallSnapshot 快照可能较大）。
const maxGRPCMessage = 256 << 20

func main() {
	var (
		id       = flag.Int("id", 0, "本节点 ID")
		addr     = flag.String("addr", "127.0.0.1:50051", "本节点监听地址")
		pgAddr   = flag.String("pg-addr", "", "PostgreSQL wire 协议监听地址（默认不启动）")
		pgUsers  = flag.String("pg-users", "", "PG 用户列表 user=password,user2=password2（默认 trust 认证）")
		peers    = flag.String("peers", "", "其他节点，格式 id=host:port，逗号分隔")
		data     = flag.String("data", "", "SQLite 数据文件路径（默认内存库）")
		raftDir  = flag.String("raft-dir", "", "Raft 日志与状态目录（默认内存运行）")
		election = flag.Duration("election-timeout", 1500*time.Millisecond, "选举超时")
		heart    = flag.Duration("heartbeat-interval", 500*time.Millisecond, "心跳间隔")
		compact  = flag.Uint("compact-after", 4096, "日志压缩阈值（条数，0 禁用）")
	)
	flag.Parse()

	peerMap, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}

	st, err := store.Open(*data)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// 打开（或创建）Raft WAL；未指定目录时纯内存运行
	var wal *raftwal.Wal
	if *raftDir != "" {
		wal, err = raftwal.Open(*raftDir)
		if err != nil {
			log.Fatalf("open raft wal: %v", err)
		}
		defer wal.Close()
	}

	// 从 SQLite 元数据恢复已应用索引，避免崩溃后重复应用
	appliedIndex, err := st.AppliedIndex()
	if err != nil {
		log.Fatalf("read applied index: %v", err)
	}

	node, err := raft.New(raft.Config{
		ID:                int32(*id),
		Peers:             peerMap,
		ElectionTimeout:   *election,
		HeartbeatInterval: *heart,
		Wal:               wal,
		AppliedIndex:      appliedIndex,
		CompactAfter:      uint64(*compact),
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			if entry.GetCommand().GetConfChange() != nil {
				return &raft.ApplyResult{}, nil // 配置变更由 raft 核心处理
			}
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			res := &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
			}
			for _, c := range outcome.Columns {
				res.Columns = append(res.Columns, c.Name)
				res.RowTypes = append(res.RowTypes, c.Type)
			}
			res.Rows = outcome.Rows
			return res, nil
		},
		SnapshotData: func(ctx context.Context) ([]byte, error) {
			return st.Snapshot()
		},
		ApplySnapshot: func(ctx context.Context, data []byte) error {
			return st.ApplySnapshot(data)
		},
	})
	if err != nil {
		log.Fatalf("create raft node: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessage),
		grpc.MaxSendMsgSize(maxGRPCMessage),
	)
	raftpb.RegisterRaftServiceServer(gs, node)
	api := server.New(node, st, nil)
	sqlraftpb.RegisterSqlRaftServer(gs, api)
	sqlraftpb.RegisterAdminServer(gs, server.NewAdmin(node))
	node.Start()

	go func() {
		log.Printf("sqlraftd node %d listening on %s (peers=%v)", *id, *addr, peerMap)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// PostgreSQL wire 兼容层：psql / JDBC / ORM 直连
	if *pgAddr != "" {
		pgLis, err := net.Listen("tcp", *pgAddr)
		if err != nil {
			log.Fatalf("listen pg %s: %v", *pgAddr, err)
		}
		pg := pgwire.New(api, st, "sqlraft", "sqlraft", parseUsers(*pgUsers), nil)
		go func() {
			log.Printf("sqlraftd pgwire listening on %s", *pgAddr)
			if err := pg.Serve(pgLis); err != nil {
				log.Fatalf("pg serve: %v", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down")
	node.Stop()
	gs.GracefulStop()
}

// parseUsers 解析 "user=pass,user2=pass2" 形式的用户列表。
func parseUsers(s string) map[string]string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	users := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			users[strings.TrimSpace(kv[0])] = kv[1]
		}
	}
	return users
}

// parsePeers 解析 "id=host:port,id=host:port" 形式的节点列表。
func parsePeers(s string) (map[int32]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	peers := make(map[int32]string)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("malformed peer %q", part)
		}
		id, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("bad peer id in %q", part)
		}
		peers[int32(id)] = strings.TrimSpace(kv[1])
	}
	return peers, nil
}
