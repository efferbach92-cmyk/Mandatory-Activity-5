package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

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
	n.node.mu.Lock()
	defer n.node.mu.Unlock()
	log.Printf("[Server] event=REPLICATION_REQUEST sequenceNumber=%d requestID=%d bidder=%s amount=%d term=%d",
		req.SequenceNumber, req.RequestID, req.Bidder, req.Amount, req.Term)
	// vi tjekker om sekvensnummmeret er ugyldigt, eller er for højt.
	if req.SequenceNumber < 1 || req.SequenceNumber > int32(len(n.node.log))+1 {
		return &proto.ReplicateResponse{
			Acknowledged: false,
		}, nil
	}
	// tjekker om sekvensnummeret allerede findes i loggen. Hvis det gør,
	// tjekker vi om requestID matcher. Hvis det matcher, returnerer vi en bekræftelse. Hvis det ikke matcher, returnerer vi en afvisning.
	if req.SequenceNumber <= int32(len(n.node.log)) {
		existing := n.node.log[req.SequenceNumber-1]
		return &proto.ReplicateResponse{
			Acknowledged: existing.requestID == req.RequestID, // returnerer true eller false
		}, nil
	}
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
		return n.forwardBidToLeader(ctx, req)
	}
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// tjekker om der er et bud, som ikke er committed.
	// forsøger at replikerer det igen før vi behandler et nyt bud.
	if len(n.node.log) > int(n.node.commitIndex) {
		pending := n.node.log[n.node.commitIndex]
		if !n.replicateBidToFollowers(ctx, pending) {
			return &proto.BidResponse{
				Status: proto.BidStatus_ERROR,
			}, nil
		}
		n.node.commitIndex++
		if pending.requestID == req.RequestID {
			return &proto.BidResponse{
				Status: proto.BidStatus_SUCCESS,
			}, nil
		}
	}
	var highestBid int32 = 0
	if n.node.commitIndex > 0 {
		highestBid = n.node.log[n.node.commitIndex-1].amount
	}
	if highestBid >= req.Bid {
		return &proto.BidResponse{
			Status: proto.BidStatus_FAIL,
		}, nil
	}
	entry := BidEntry{
		sequenceNumber: int32(len(n.node.log) + 1),
		requestID:      req.RequestID,
		bidder:         req.Bidder,
		amount:         req.Bid,
	}
	n.node.log = append(n.node.log, entry)

	ackRecieved := n.replicateBidToFollowers(ctx, entry)
	if !ackRecieved {
		return &proto.BidResponse{
			Status: proto.BidStatus_ERROR,
		}, nil
	}
	n.node.commitIndex++
	return &proto.BidResponse{
		Status: proto.BidStatus_SUCCESS,
	}, nil
}

func (n *NodeServer) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	leaderAddress, ok := nodeAddresses[n.node.leaderID]
	// finder lederen i nodeAddresses mappet på baggrund af leaderID. Hvis lederen ikke findes, returneres en
	if !ok {
		return nil, fmt.Errorf("leader address not found for node %d", n.node.leaderID)
	}
	// opretter en gRPC-forbindelse til lederen og sender buddet videre
	conn, err := grpc.NewClient(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	leaderClient := proto.NewAuctionServiceClient(conn)
	return leaderClient.Bid(ctx, req)
}

func (n *NodeServer) replicateBidToFollowers(ctx context.Context, entry BidEntry) bool {
	ackRecieved := false
	for id, address := range nodeAddresses {
		if id == n.node.id {
			continue // skipper sig selv (lederen)
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Node %d] Failed to create client for node %d: %v", n.node.id, id, err)
			continue
		}
		defer conn.Close()
		nodeClient := proto.NewNodeServiceClient(conn)
		for _, logEntry := range n.node.log {

			request := &proto.ReplicateRequest{
				SequenceNumber: logEntry.sequenceNumber,
				RequestID:      logEntry.requestID,
				Bidder:         logEntry.bidder,
				Amount:         logEntry.amount,
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			response, err := nodeClient.Replicate(rpcCtx, request)
			cancel()
			if err != nil {
				log.Printf("[Node %d] Failed to replicate to node %d: %v", n.node.id, id, err)
				break
			}
			if !response.Acknowledged {
				break
			}
			if logEntry.sequenceNumber == entry.sequenceNumber {
				ackRecieved = true
			}
		}
	}
	return ackRecieved
}

func (n *NodeServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.Empty, error) {
	n.node.mu.Lock()
	defer n.node.mu.Unlock()
	if req.LeaderID != n.node.leaderID {
		return nil, fmt.Errorf("heartbeat from unknown leader: %d", req.LeaderID)
	}
	if req.CommitIndex > n.node.commitIndex {
		n.node.commitIndex = min(req.CommitIndex, int32(len(n.node.log)))
	}
	return &proto.Empty{}, nil
}
