package main

import (
	"log"
	"net"
	"portbound/raft/internal/node"
	"portbound/raft/internal/transport/rpc"
	"portbound/raft/proto"

	"google.golang.org/grpc"
)

func main() {
	port := ":8080"
	peers := map[string]string{}
	id := ""
	addr := ""

	server := grpc.NewServer()

	node, err := node.New(id, addr, peers)
	if err != nil {
		log.Fatalf("failed to establish connection to peers: %s", err)
	}

	proto.RegisterRaftServer(server, rpc.New(node))

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen on %s: %v", port, err)
	}

	log.Printf("listening on %s", port)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
