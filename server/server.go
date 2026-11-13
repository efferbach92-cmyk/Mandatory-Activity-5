package main

import (
	"log"
	"net"
	"os"
	"strconv"

	proto "github.com/efferbach92-cmyk/Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
)

type NodeServer struct {
	proto.UnimplementedAuctionServiceServer
	node *Node
}

func (s *NodeServer) start_server(port string) {

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Server did not start: %v", err)
		//%v indsætter den konkrete fejl og fejlen bliver logget
	}

	proto.RegisterAuctionServiceServer(grpcServer, s)
	log.Printf("[Server] event=STARTUP port=%s", port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Server stopped: %v", err)
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run ./server <nodeID> <port>")
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	server := &NodeServer{
		node: &Node{
			id:       int32(id),
			port:     os.Args[2],
			role:     "follower",
			leaderID: 1,
		},
	}
	if id == 1 {
		server.node.role = "leader"
	}

	server.start_server(server.node.port)
}
