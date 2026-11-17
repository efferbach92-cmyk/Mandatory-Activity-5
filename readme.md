# Mandatory-Activity-5

Mandatory Activity 5 - Distributed Systems, IT University of Copenhagen

A replicated auction in Go. Three nodes hold the same bid log, and clients can send bids and ask for the result at any node. The nodes elect a leader, replicate every bid to the other nodes and keep working if one node crashes. The nodes talk to each other and to the clients with gRPC (every node is both a server and a client).

Requirements:

* Go 1.27.0 (specified in go.mod)
* Dependencies are managed through go.mod and go.sum
* protoc is only needed if the .proto file in grpc/ is changed (the generated files are in the repository)

The generated gRPC files are already in the repository, so you do not need protoc to run the system.

## Project Structure
```text
Mandatory-Activity-5/
├── client/
│   └── client.go
├── grpc/
│   ├── node.proto
│   ├── node.pb.go
│   └── node_grpc.pb.go
├── server/
│   ├── election.go
│   ├── node.go
│   └── server.go
├── .gitignore
├── README.md
├── go.mod
└── go.sum
```


## How to Run

Start the three nodes, each in its own terminal. The port must match the address of the node in nodeAddresses

# Terminal 1
 ```bash
go run ./server 1 5050
```

# Terminal 2
 ```bash
go run ./server 2 5051
```

# Terminal 3
 ```bash
go run ./server 3 5052
```

Node 1 starts as the leader, and the auction starts when node 1 starts. The auction lasts 2 minutes (auctionEnd in main in server.go).
Then start a client in a fourth terminal. The argument is the name of the bidder


 ```bash
go run ./client Bob
```
The client sends bids for the bidder, asks for the result and prints the winner. It tries the nodes one at a time, so it still works if a node is down. Start the client with another name (for example Alice) to bid as someone else.
Press Ctrl+C in every terminal when you are done.

## How It Works

Bids
A client can send a bid to any node. A follower forwards the bid to the leader. The leader:

* Rejects the bid with AUCTION_OVER if the auction has ended.

* Treats a bid with a known requestID and bidder as a duplicate, so a retry is never counted twice.

* Rejects the bid with FAIL if it is not higher than the highest bid.

* Adds the bid to its log and replicates the log to the followers with the Replicate RPC.

* Commits the bid when a majority of the nodes has stored it. With three nodes, the leader and one follower are enough.

The possible answers are SUCCESS, FAIL, ERROR and AUCTION_OVER.

## Result

Result returns the highest committed bid, the name of the winner and whether the auction is over. A follower forwards the request to the leader. A bid that is not committed is never returned.


## Leader and terms

The nodes use terms to tell old and new leaders apart.

* All nodes start in term 1, and node 1 is the leader.

* The leader sends a heartbeat every 500 ms with its term, its commit index and the end time of the auction.
  
* A follower that gets no heartbeat for 1.5-3 seconds (random) starts an election: it increases the term, votes for itself and asks the others for their vote with RequestVote.
  
* Each node can vote only once per term. A candidate must have a log at least as long as the voting node's log.
  
* A candidate with a majority of the votes becomes leader. The new leader first commits the bids it has in its log.
  
* A message from an older term is rejected. A heartbeat from an old leader gets the gRPC code FailedPrecondition, and an old leader's Replicate request is not acknowledged.


## Failure model
The system is designed to tolerate one node crashing. With three nodes, the remaining two nodes can still form a majority and elect a new leader. Nodes use terms and heartbeats to detect leader failures and coordinate elections.

## Nodes logging
Each node writes its events to a separate log file (node=1.log, node=2.log, and node=3.log). The logs record important events such as node startup, heartbeat messages, leader changes, elections, bid replication, and errors. This makes it possible to monitor communication between nodes and investigate what happens if a node crashes or a new leader is elected.





