package main

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
