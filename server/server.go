package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	proto "github.com/efferbach92-cmyk/Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeServer struct {
	proto.UnimplementedAuctionServiceServer
	proto.UnimplementedNodeServiceServer
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
	proto.RegisterNodeServiceServer(grpcServer, s)
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

func (n *NodeServer) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	log.Printf("[Server] event=REPLICATION_REQUEST sequenceNumber=%d requestID=%d bidder=%s amount=%d term=%d",
		req.SequenceNumber, req.RequestID, req.Bidder, req.Amount, req.Term)

	// Modtager budet og gemmer det i loggen
	entry := BidEntry{
		sequenceNumber: req.SequenceNumber,
		requestID:      req.RequestID,
		bidder:         req.Bidder,
		amount:         req.Amount,
	}
	n.node.log = append(n.node.log, entry)

	// sender en bekræftelse tilbage til lederen
	return &proto.ReplicateResponse{
		Acknowledged: true,
	}, nil
}

func (n *NodeServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	log.Printf("[Node %d] event=BID_RECEIVED bidder=%s amount=%d",
		n.node.id, req.Bidder, req.Bid)

	if n.node.role != "leader" {
		// noden skal vidersende buddet til lederen. har bare lavet return så det kan compile
	}
	return &proto.BidResponse{
		Status: proto.BidStatus_ERROR,
	}, nil
}

func (n *NodeServer) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	leaderAddress, ok := nodeAddresses[n.node.leaderID]
	// finder lederen i nodeAddresses mappet på baggrund af leaderID. Hvis lederen ikke findes, returneres en
	if !ok {
		return nil, fmt.Errorf("leader address not found for node %d", n.node.leaderID)
	}

	conn, err := grpc.NewClient(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
}
