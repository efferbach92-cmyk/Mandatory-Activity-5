package main

var nodeAddresses = map[int32]string{
	1: "localhost:50050",
	2: "localhost:50051",
	3: "localhost:50052",
}

type Node struct {
	id       int32
	role     string
	leaderID int32
	log      []BidEntry
	port     string
}

type BidEntry struct {
	sequenceNumber int32
	requestID      int32
	bidder         string
	amount         int32
}
