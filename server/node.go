package main

import "sync"

var nodeAddresses = map[int32]string{
	1: "localhost:5050",
	2: "localhost:5051",
	3: "localhost:5052",
}

type Node struct {
	id          int32
	role        string
	leaderID    int32
	log         []BidEntry
	commitIndex int32
	port        string
	mu          sync.Mutex
}

type BidEntry struct {
	sequenceNumber int32
	requestID      int32
	bidder         string
	amount         int32
}
