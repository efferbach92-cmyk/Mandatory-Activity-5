package main

import (
	"sync"
	"time"
)

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
	auctionEnd  time.Time

	// Leader election
	term          int32     // valgnummer. Tæller op hver gang nogen starter et valg.
	votedFor      int32     // hvem vi har stemt på i denne term. 0 = ingen endnu.
	lastHeartbeat time.Time // hvornår vi sidst hørte fra lederen.
}

type BidEntry struct {
	sequenceNumber int32
	requestID      int32
	bidder         string
	amount         int32
}
