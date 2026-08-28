// Package server 实现面向客户端的 gRPC 服务：Execute（写）与 Query（读）。
package server

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/rewrite"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

// Server 实现 sqlraftpb.SqlRaftServer。
type Server struct {
	sqlraftpb.UnimplementedSqlRaftServer
	node  *raft.Node
	store *store.Store
	log   *log.Logger

	// writeMu 串行化写路径：确保 AUTOINCREMENT 预分配与
	// 状态机读取之间没有并发间隙。
	writeMu sync.Mutex
}

// New 创建 SQL 服务。
func New(node *raft.Node, st *store.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{node: node, store: st, log: logger}
}

// execute 是写路径的核心：串行化、等待追平、确定性改写、提交 Raft。
func (s *Server) execute(ctx context.Context, sql string, params []*sqlraftpb.Value) (*raft.ApplyResult, error) {
	// 等待历史已提交条目全部应用，保证改写器读取的状态机处于一致状态
	if err := s.node.WaitCaughtUp(ctx); err != nil {
		return nil, err
	}

	// AUTOINCREMENT 预分配要求“读取序列 → 提交”之间无其他写：
	// 含 INSERT 的语句保守地串行；UPDATE/DELETE/DDL 可并发，
	// 由 raft 层的 group commit 合并批量持久化与复制。
	if needsSerialization(sql) {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		if err := s.node.WaitCaughtUp(ctx); err != nil {
			return nil, err
		}
	}

	// 确定性改写：NOW/RANDOM 等函数在 Leader 侧替换为字面量；
	// AUTOINCREMENT 的 INSERT 显式预分配 ID。
	now := time.Now()
	rewritten, err := rewrite.Rewrite(sql, params, s.store, rewrite.Options{Now: now})
	if err != nil {
		return nil, err
	}

	cmd := &logpb.DeterministicCommand{
		Cmd: &logpb.DeterministicCommand_Exec{
			Exec: &logpb.Exec{
				Sql:       rewritten.SQL,
				Params:    rewritten.Params,
				NowMicros: now.UnixMicro(), // 统一时间戳：改写 NOW()/CURRENT_TIMESTAMP 的依据
				Sequence:  rewritten.Sequence,
			},
		},
	}

	return s.node.Propose(ctx, cmd)
}

// needsSerialization 判断语句是否需要串行化执行（AUTOINCREMENT 相关）。
func needsSerialization(sql string) bool {
	up := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(up, "INSERT") ||
		strings.HasPrefix(up, "REPLACE") ||
		strings.Contains(up, " INSERT ") ||
		strings.Contains(up, " INSERT OR ")
}

// ExecuteRaw 执行一条写语句并返回原始结果（含 RETURNING 行），供 PG wire 使用。
func (s *Server) ExecuteRaw(ctx context.Context, sql string, params []*sqlraftpb.Value) (*raft.ApplyResult, error) {
	return s.execute(ctx, sql, params)
}

// ExecuteTx 把多语句事务整段作为一条 Raft 日志原子提交（PG wire 使用）。
// 任一语句失败则整段回滚，不产生任何部分生效。
func (s *Server) ExecuteTx(ctx context.Context, statements []string) error {
	_, err := s.execute(ctx, strings.Join(statements, "; "), nil)
	return err
}

// Execute 把写请求包装成确定性命令，提交到 Raft 日志，提交并应用后返回结果。
func (s *Server) Execute(ctx context.Context, req *sqlraftpb.ExecuteRequest) (*sqlraftpb.ExecuteResponse, error) {
	sql := req.Sql
	if req.Tx {
		// 多语句事务：整段作为一个日志条目复制，由 store 在单个事务内执行
		sql = strings.Join(req.Statements, "; ")
	}
	res, err := s.execute(ctx, sql, req.Params)
	if err != nil {
		var notLeader *raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			return &sqlraftpb.ExecuteResponse{Error: "NOT_LEADER", LeaderId: notLeader.Leader}, nil
		}
		return nil, status.Errorf(codes.Internal, "propose: %v", err)
	}

	s.log.Printf("executed index=%d leader=%d sql=%q", s.node.CommitIndex(), s.node.LeaderID(), sql)
	return &sqlraftpb.ExecuteResponse{
		RowsAffected: res.RowsAffected,
		LastInsertId: res.LastInsertID,
		CommitIndex:  s.node.CommitIndex(),
		LeaderId:     s.node.LeaderID(),
	}, nil
}

// Query 执行查询。strong=true 时要求由 Leader 应答（M0 的近似线性一致读，
// 后续版本用 ReadIndex 严格化）；strong=false 时返回本地副本结果。
func (s *Server) Query(ctx context.Context, req *sqlraftpb.QueryRequest) (*sqlraftpb.QueryResponse, error) {
	leader := s.node.LeaderID()
	if req.Strong {
		// ReadIndex 线性一致读：确认自己仍是 Leader，且状态机已应用
		// 到 readIndex 之后才执行本地读。
		ri, err := s.node.ReadIndex(ctx)
		if err != nil {
			var notLeader *raft.ErrNotLeader
			if errors.As(err, &notLeader) {
				return &sqlraftpb.QueryResponse{Error: "NOT_LEADER", LeaderId: notLeader.Leader}, nil
			}
			return nil, status.Errorf(codes.Internal, "read index: %v", err)
		}
		if err := s.node.WaitApplied(ctx, ri); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}

	columns, rows, err := s.store.Query(ctx, req.Sql, req.Params)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "query: %v", err)
	}
	return &sqlraftpb.QueryResponse{
		Columns:        columns,
		Rows:           rows,
		LeaderId:       leader,
		ServedByLeader: s.node.IsLeader(),
	}, nil
}
