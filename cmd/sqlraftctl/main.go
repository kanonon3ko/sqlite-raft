// sqlraftctl 是集群管理命令行工具：成员变更与状态查询。
//
// 用法：
//
//	sqlraftctl -addr 127.0.0.1:50051 status
//	sqlraftctl -addr 127.0.0.1:50051 add-peer 3=127.0.0.1:50053
//	sqlraftctl -addr 127.0.0.1:50051 remove-peer 3
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "目标节点地址")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()
	admin := sqlraftpb.NewAdminClient(conn)

	switch args[0] {
	case "status":
		resp, err := admin.ClusterStatus(ctx, &sqlraftpb.ClusterStatusRequest{})
		if err != nil {
			log.Fatalf("status: %v", err)
		}
		fmt.Printf("node_id:    %d\n", resp.NodeId)
		fmt.Printf("state:      %s\n", resp.State)
		fmt.Printf("leader:     %d\n", resp.LeaderId)
		ids := make([]int, 0, len(resp.Peers))
		for id := range resp.Peers {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		fmt.Println("peers:")
		for _, id := range ids {
			fmt.Printf("  %d -> %s\n", id, resp.Peers[int32(id)])
		}
	case "add-peer":
		if len(args) < 2 {
			usage()
		}
		kv := strings.SplitN(args[1], "=", 2)
		if len(kv) != 2 {
			usage()
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			log.Fatalf("bad node id %q", kv[0])
		}
		resp, err := admin.AddPeer(ctx, &sqlraftpb.AddPeerRequest{NodeId: int32(id), Addr: kv[1]})
		if err != nil {
			log.Fatalf("add-peer: %v", err)
		}
		if !resp.Ok {
			log.Fatalf("add-peer failed: %s (leader=%d)", resp.Error, resp.LeaderId)
		}
		fmt.Printf("peer %d (%s) added\n", id, kv[1])
	case "remove-peer":
		if len(args) < 2 {
			usage()
		}
		id, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatalf("bad node id %q", args[1])
		}
		resp, err := admin.RemovePeer(ctx, &sqlraftpb.RemovePeerRequest{NodeId: int32(id)})
		if err != nil {
			log.Fatalf("remove-peer: %v", err)
		}
		if !resp.Ok {
			log.Fatalf("remove-peer failed: %s (leader=%d)", resp.Error, resp.LeaderId)
		}
		fmt.Printf("peer %d removed\n", id)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: sqlraftctl -addr HOST:PORT <command>
  status                   查看集群状态
  add-peer ID=HOST:PORT    添加节点（需在 Leader 上执行）
  remove-peer ID           移除节点（需在 Leader 上执行）`)
	os.Exit(2)
}
