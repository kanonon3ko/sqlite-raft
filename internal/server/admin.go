// Package server 的集群管理服务：成员变更与状态查询。
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
)

// Admin 实现 sqlraftpb.AdminServer。
type Admin struct {
	sqlraftpb.UnimplementedAdminServer
	node *raft.Node
}

// NewAdmin 创建管理服务。
func NewAdmin(node *raft.Node) *Admin {
	return &Admin{node: node}
}

// AddPeer 把新节点加入集群（在 Leader 上提交配置变更日志）。
func (a *Admin) AddPeer(ctx context.Context, req *sqlraftpb.AddPeerRequest) (*sqlraftpb.AddPeerResponse, error) {
	err := a.node.AddPeer(ctx, req.NodeId, req.Addr)
	if err != nil {
		var notLeader *raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			return &sqlraftpb.AddPeerResponse{Ok: false, Error: "NOT_LEADER", LeaderId: notLeader.Leader}, nil
		}
		return nil, status.Errorf(codes.Internal, "add peer: %v", err)
	}
	return &sqlraftpb.AddPeerResponse{Ok: true, LeaderId: a.node.LeaderID()}, nil
}

// RemovePeer 把节点移出集群。
func (a *Admin) RemovePeer(ctx context.Context, req *sqlraftpb.RemovePeerRequest) (*sqlraftpb.RemovePeerResponse, error) {
	err := a.node.RemovePeer(ctx, req.NodeId)
	if err != nil {
		var notLeader *raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			return &sqlraftpb.RemovePeerResponse{Ok: false, Error: "NOT_LEADER", LeaderId: notLeader.Leader}, nil
		}
		return nil, status.Errorf(codes.Internal, "remove peer: %v", err)
	}
	return &sqlraftpb.RemovePeerResponse{Ok: true, LeaderId: a.node.LeaderID()}, nil
}

// ClusterStatus 返回本节点视角的集群状态。
func (a *Admin) ClusterStatus(ctx context.Context, req *sqlraftpb.ClusterStatusRequest) (*sqlraftpb.ClusterStatusResponse, error) {
	state := "follower"
	if a.node.IsLeader() {
		state = "leader"
	}
	peers := a.node.Peers()
	resp := &sqlraftpb.ClusterStatusResponse{
		NodeId:   a.node.NodeID(),
		LeaderId: a.node.LeaderID(),
		State:    state,
		Peers:    peers,
	}
	return resp, nil
}
