package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
	"io"
	
	

	proto "github.com/efferbach92-cmyk/Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeServer struct {
	proto.UnimplementedAuctionServiceServer
	proto.UnimplementedNodeServiceServer
	node *Node


	// Stores reusable gRPC clients for commmu with the other nodes.
	nodeClients map[int32]proto.NodeServiceClient
	
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


// Creates and stores gRPC clients for commu with all other nodes. Each node gets a NodeServiceClient
func (s *NodeServer) connectToPeers() {
	s.nodeClients = make (map[int32]proto.NodeServiceClient)
	for id, addr := range nodeAddresses {
		if id == s.node.id {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			log.Fatalf("[Node %d] event =Error msg=%v", s.node.id, err)
		}
		s.nodeClients[id] = proto.NewNodeServiceClient(conn)
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
	port := os.Args[2]

	// Checks that the provided node ID and port match the nodes configured address.
	if want, ok := nodeAddresses[int32(id)]; !ok || want != "localhost:"+port {
		log.Fatalf("node %d should run on address in nodeAdresses (%q)", id, want)
	}


	// OpenFile and Multiwriter from MA3
	logFile, err := os.OpenFile(fmt.Sprintf("node=%d.log", id), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err !=nil {
		log.Fatalf("could not open log file: %v", err)
	}

	// One logfile pr. node and Lmicroseconds timestamped from MA4
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)


	server := &NodeServer{
		node: &Node{
			id:       int32(id),
			port:     port,
			role:     "follower",
			leaderID: 1,
			term:     1, // alle starter i term 1, hvor node 1 er leder
		},
	}
	if id == 1 {
		server.node.role = "leader"
		server.node.auctionEnd = time.Now().Add(2 * time.Minute) //auktionen varer 2 minutter (kan ændres)
		log.Printf("Node 1] event = Auction_Start ends=%s", server.node.auctionEnd.Format(time.RFC3339)) //
	}

	// RPC connections to all other nodes
	server.connectToPeers()



	// Background process that sends frequent heartbeats to followers.
	go server.heartbeatLoop()

	// Baggrundsproces der starter et valg, hvis lederens heartbeats stopper (se election.go).
	go server.electionLoop()

	server.start_server(port)
}


func (n *NodeServer) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	n.node.mu.Lock()
	defer n.node.mu.Unlock()
	log.Printf("[Server] event=REPLICATION_REQUEST sequenceNumber=%d requestID=%d bidder=%s amount=%d term=%d",
		req.SequenceNumber, req.RequestID, req.Bidder, req.Amount, req.Term)
	// Afviser bud fra en gammel leder, der er i en ældre term end os.
	if req.Term < n.node.term {
		return &proto.ReplicateResponse{
			Acknowledged: false,
		}, nil
	}
	// En besked fra en gyldig leder tæller også som livstegn, ligesom et heartbeat.
	if req.Term > n.node.term {
		n.node.term = req.Term
		n.node.votedFor = 0
	}
	n.node.lastHeartbeat = time.Now()
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

	if !n.isLeader() {
		return n.forwardBidToLeader(ctx, req)
	}
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	if time.Now().After(n.node.auctionEnd) {
		return &proto.BidResponse{
			Status: proto.BidStatus_AUCTION_OVER,
		}, nil
	}

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
	leaderID := n.currentLeader()
	// Hvis vi selv står som leder, men ikke er det (midt i et valg), ville vi sende buddet til os selv i ring.
	if leaderID == n.node.id {
		return nil, fmt.Errorf("no leader right now, election in progress")
	}
	leaderAddress, ok := nodeAddresses[leaderID]
	// finder lederen i nodeAddresses mappet på baggrund af leaderID. Hvis lederen ikke findes, returneres en
	if !ok {
		return nil, fmt.Errorf("leader address not found for node %d", leaderID)
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
				Term:           n.node.term, // så followers kan afvise en gammel leder
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

// Defines how often the leader sends heartbeats: every 500 milliseconds.
const heartbeatInterval = 500 * time.Millisecond

func (n *NodeServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.Empty, error) {
	n.node.mu.Lock()
	defer n.node.mu.Unlock()
	// Heartbeat fra en gammel leder i en ældre term. Den ignorerer vi.
	if req.Term < n.node.term {
		return nil, fmt.Errorf("heartbeat from old term %d, current term is %d", req.Term, n.node.term)
	}
	// Ny term betyder at der har været et valg. Vi rykker med op og har en ny stemme at give.
	if req.Term > n.node.term {
		n.node.term = req.Term
		n.node.votedFor = 0
	}
	if req.LeaderID != n.node.leaderID {
		log.Printf("[Node %d] event=NEW_LEADER leader=%d term=%d", n.node.id, req.LeaderID, req.Term)
	}
	// Afsenderen er den gyldige leder. Vi følger den og nulstiller stopuret til leader election.
	n.node.role = "follower"
	n.node.leaderID = req.LeaderID
	n.node.lastHeartbeat = time.Now()
	if req.CommitIndex > n.node.commitIndex {
		n.node.commitIndex = min(req.CommitIndex, int32(len(n.node.log)))
	}
	return &proto.Empty{}, nil
}


// Sends periodic heartbeats from the leader to all followers.
func (s *NodeServer) heartbeatLoop() {
	n := s.node

	// Creates a ticker that triggers every 500 ms. Defer makes sur that it stops when func exits.
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()



	// Checks every 500 ms if the node is the leader. If not, it skips the heartbeat.
	for range ticker.C {
		n.mu.Lock()
		if n.role != "leader" {
			n.mu.Unlock()
			continue
		}
		// Creates a heartbeat message containing these:
		req := &proto.HeartbeatRequest{
			LeaderID:    n.id,
			Term:        n.term,
			CommitIndex: n.commitIndex,
		}
		n.mu.Unlock()

		for _, c := range s.nodeClients {
			go func(c proto.NodeServiceClient) { // starts separate goroutine for each follower. This allow heartbeats to be sent concurrently
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel() // Sets 1 secound timeout for heartbeat req and make sure its resources are released afterwards.

				// Calls followers heartbeat() RPC. If fcks the error is written to log
				if _, err := c.Heartbeat(ctx, req); err != nil {
					log.Printf("[Node %d] event=HEARTBEAT_FAILED msg=%v", n.id, err)
				}
			}(c) // pas currenct follower client in the goroutine.
		}
	}
}

func (n *NodeServer) Result(ctx context.Context, req *proto.Empty) (*proto.ResultResponse, error) {
	if !n.isLeader() {
		return n.forwardResultToLeader(ctx, req)
	}
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	response := &proto.ResultResponse{}
	if n.node.commitIndex > 0 {
		lastBid := n.node.log[n.node.commitIndex-1]
		response.WinningBid = lastBid.amount
		response.Winner = lastBid.bidder
	}
	response.AuctionOver = time.Now().After(n.node.auctionEnd)
	return response, nil
}

func (n *NodeServer) forwardResultToLeader(ctx context.Context, req *proto.Empty) (*proto.ResultResponse, error) {
	leaderID := n.currentLeader()
	// Samme tjek som i forwardBidToLeader: undgå at sende til os selv i ring.
	if leaderID == n.node.id {
		return nil, fmt.Errorf("no leader right now, election in progress")
	}
	leaderAddress, ok := nodeAddresses[leaderID]
	// finder lederen i nodeAddresses mappet på baggrund af leaderID. Hvis lederen ikke findes, returneres en
	if !ok {
		return nil, fmt.Errorf("leader address not found for node %d", leaderID)
	}
	// opretter en gRPC-forbindelse til lederen og sender buddet videre
	conn, err := grpc.NewClient(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	leaderClient := proto.NewAuctionServiceClient(conn)
	return leaderClient.Result(ctx, req)
}
